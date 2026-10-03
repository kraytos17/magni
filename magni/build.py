"""Build commands: debug/release binaries and all fuzz targets."""

import argparse
import os
import shutil
from pathlib import Path

from . import config
from .util import die, log, require_tool, run


def cmd_build(args: argparse.Namespace) -> None:
    """Build debug, release, ASan, or coverage fuzz targets."""
    apply_rebuild_flag(args)
    config.TARGET_DEBUG.mkdir(parents=True, exist_ok=True)
    config.TARGET_RELEASE.mkdir(parents=True, exist_ok=True)
    if args.asan:
        build_asan()
    elif args.cov:
        if args.cmplog:
            build_cov("cmplog")
        elif args.laf:
            build_cov("laf")
        else:
            build_cov()
    elif args.exec_fuzz:
        build_exec()
    elif args.release:
        run([config.ODIN, "build", str(config.SRC_DIR),
             f"-out:{config.TARGET_RELEASE}/magni_release",
             *config.COLLECTIONS, *config.RELEASE_FLAGS])
        log(f"Built {config.TARGET_RELEASE}/magni_release (release)")
    elif args.check_only:
        run([config.ODIN, "check", str(config.SRC_DIR), *config.COLLECTIONS,
             "-warnings-as-errors"])
    else:
        run([config.ODIN, "build", str(config.SRC_DIR),
             f"-out:{config.TARGET_DEBUG}/magni",
             *config.COLLECTIONS, *config.DEBUG_FLAGS])
        log(f"Built {config.TARGET_DEBUG}/magni (debug)")


def build_asan() -> None:
    run([config.ODIN, "build", "fuzz", *config.COLLECTIONS, "-o:none",
         "-sanitize:address", f"-out:{config.FUZZ_TARGET}"])
    log(f"Built {config.FUZZ_TARGET} (AddressSanitizer)")


COV_TARGETS = {
    # flavor: (output binary, extra env for afl-clang-fast)
    # AFL_LLVM_INSTRUMENT=NATIVE on all flavors: the distro's AFL LLVM
    # plugins (PCGUARD/classic) were built against older LLVM and fail to
    # load on this toolchain (undefined llvm::DebugLoc::get). NATIVE uses
    # clang's own SanitizerCoverage, which always matches the compiler.
    "cov": (config.FUZZ_TARGET_COV, {"AFL_LLVM_INSTRUMENT": "NATIVE"}),
    "cmplog": (config.FUZZ_TARGET_CMPLOG, {"AFL_LLVM_INSTRUMENT": "NATIVE",
                                           "AFL_LLVM_CMPLOG": "1"}),
    "laf": (config.FUZZ_TARGET_LAF, {"AFL_LLVM_INSTRUMENT": "NATIVE",
                                     "AFL_LLVM_LAF_ALL": "1"}),
}


def llvm_ir_sources(harness: str) -> list[str]:
    """Build a harness to LLVM IR, return the .ll file(s) to link.

    -o:speed emits a single merged module named ".ll"; the default emits one
    .ll per package. Clears TARGET_FUZZ first.
    """
    require_tool("afl-clang-fast")
    if config.TARGET_FUZZ.exists():
        shutil.rmtree(config.TARGET_FUZZ)
    config.TARGET_FUZZ.mkdir(parents=True, exist_ok=True)
    run([config.ODIN, "build", harness, "-build-mode:llvm-ir",
         *config.COLLECTIONS, "-o:speed", f"-out:{config.TARGET_FUZZ}"])
    single = config.TARGET_FUZZ / ".ll"
    ll_files = [single] if single.is_file() else sorted(config.TARGET_FUZZ.glob("*.ll"))
    if not ll_files:
        die(f"Error: no LLVM IR files in {config.TARGET_FUZZ}")
    return [str(f) for f in ll_files]


def build_cov(flavor: str = "cov") -> None:
    """Build a coverage-instrumented fuzz target for AFL++.

    flavor: "cov" (default PCGUARD), "cmplog" (RedQueen input-to-state),
    or "laf" (LAF-INTEL comparison splitting).
    """
    if flavor not in COV_TARGETS:
        die(f"Error: unknown cov flavor '{flavor}' (cov|cmplog|laf)")
    out, extra_env = COV_TARGETS[flavor]
    ll_files = llvm_ir_sources("fuzz")
    e = dict(extra_env)
    run(["afl-clang-fast", *ll_files, "-o", str(out)], env=e)
    log(f"Built {out} (AFL++ {flavor}-instrumented)")


def build_exec() -> None:
    """Build the executor/storage fuzz target (coverage + ASan combined).

    Storage bugs are memory bugs, so the exec target always carries
    AddressSanitizer — there is no sanitizer-free variant. NATIVE
    instrumentation, same rationale as COV_TARGETS.
    """
    ll_files = llvm_ir_sources("fuzz_exec")
    run(["afl-clang-fast", "-fsanitize=address",
         *ll_files, "-o", str(config.FUZZ_EXEC_TARGET)],
        env={"AFL_LLVM_INSTRUMENT": "NATIVE"})
    log(f"Built {config.FUZZ_EXEC_TARGET} (AFL++ coverage + ASan)")


def _is_executable(path) -> bool:
    return path.is_file() and os.access(path, os.X_OK)


# Set by --rebuild (or MAGNI_REBUILD=1): ensure_* rebuild unconditionally.
FORCE_REBUILD = False


def apply_rebuild_flag(args) -> None:
    """Honor --rebuild from any command that triggers an ensure_* build."""
    global FORCE_REBUILD
    if getattr(args, "rebuild", False):
        FORCE_REBUILD = True

# Harness source roots whose mtime invalidates a linked target.
# (Odin core itself is toolchain-versioned; fuzz/toolchain-version.txt pins it.)
HARNESS_WATCH = {
    "fuzz": ["src", "fuzz/main.odin"],
    "fuzz_exec": ["src", "fuzz_exec/main.odin"],
}

_mtime_cache: dict[str, float] = {}


def newest_mtime_under(roots: list[Path]) -> float:
    """Newest .odin mtime under the given files/dirs (0.0 if none)."""
    newest = 0.0
    for p in roots:
        if p.is_file():
            newest = max(newest, p.stat().st_mtime)
        elif p.is_dir():
            for f in p.rglob("*.odin"):
                if f.is_file():
                    newest = max(newest, f.stat().st_mtime)
    return newest


def newest_source_mtime(harness: str) -> float:
    """Newest mtime under the harness's watched roots (cached per process)."""
    if harness in _mtime_cache:
        return _mtime_cache[harness]
    newest = newest_mtime_under([config.ROOT / rel for rel in HARNESS_WATCH[harness]])
    _mtime_cache[harness] = newest
    return newest


def is_fresh(target, harness: str) -> bool:
    """A target is usable only if built and newer than all its sources.

    The old existence-only check silently validated stale binaries (gates
    passed against days-old code). Missing, non-executable, forced, or older
    than any watched source all count as stale.
    """
    if FORCE_REBUILD or os.environ.get("MAGNI_REBUILD") == "1":
        return False
    if not _is_executable(target):
        return False
    try:
        return target.stat().st_mtime >= newest_source_mtime(harness)
    except OSError:
        return False


def ensure_cov_target() -> None:
    if is_fresh(config.FUZZ_TARGET_COV, "fuzz"):
        return
    log("Coverage target missing or stale, building...")
    build_cov()


def ensure_cmplog_target() -> None:
    if is_fresh(config.FUZZ_TARGET_CMPLOG, "fuzz"):
        return
    log("cmplog target missing or stale, building...")
    build_cov("cmplog")


def ensure_laf_target() -> None:
    if is_fresh(config.FUZZ_TARGET_LAF, "fuzz"):
        return
    log("laf target missing or stale, building...")
    build_cov("laf")


def ensure_asan_target() -> None:
    if is_fresh(config.FUZZ_TARGET, "fuzz"):
        return
    log("ASan target missing or stale, building...")
    build_asan()


def ensure_exec_target() -> None:
    if is_fresh(config.FUZZ_EXEC_TARGET, "fuzz_exec"):
        return
    log("Exec target missing or stale, building...")
    build_exec()
