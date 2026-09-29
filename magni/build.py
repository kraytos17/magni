"""Build commands: debug/release binaries and all fuzz targets."""

import argparse
import os
import shutil

from . import config
from .util import die, log, require_tool, run


def cmd_build(args: argparse.Namespace) -> None:
    """Build debug, release, ASan, or coverage fuzz targets."""
    config.BUILD_DIR.mkdir(exist_ok=True)
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
             f"-out:{config.BUILD_DIR}/magni_release",
             *config.COLLECTIONS, *config.RELEASE_FLAGS])
        log(f"Built {config.BUILD_DIR}/magni_release (release)")
    elif args.check_only:
        run([config.ODIN, "check", str(config.SRC_DIR), *config.COLLECTIONS,
             "-warnings-as-errors"])
    else:
        run([config.ODIN, "build", str(config.SRC_DIR),
             f"-out:{config.BUILD_DIR}/magni",
             *config.COLLECTIONS, *config.DEBUG_FLAGS])
        log(f"Built {config.BUILD_DIR}/magni (debug)")


def build_asan() -> None:
    run([config.ODIN, "build", "fuzz", *config.COLLECTIONS, "-o:none",
         "-sanitize:address", f"-out:{config.FUZZ_TARGET}"])
    log(f"Built {config.FUZZ_TARGET} (AddressSanitizer)")


COV_TARGETS = {
    # flavor: (output binary, extra env for afl-clang-fast)
    "cov": (config.FUZZ_TARGET_COV, {}),
    "cmplog": (config.FUZZ_TARGET_CMPLOG, {"AFL_LLVM_CMPLOG": "1"}),
    "laf": (config.FUZZ_TARGET_LAF, {"AFL_LLVM_LAF_ALL": "1"}),
}


def llvm_ir_sources(harness: str) -> list[str]:
    """Build a harness to LLVM IR, return the .ll file(s) to link.

    -o:speed emits a single merged module named ".ll"; the default emits one
    .ll per package. Clears FUZZ_BUILD_DIR first.
    """
    require_tool("afl-clang-fast")
    if config.FUZZ_BUILD_DIR.exists():
        shutil.rmtree(config.FUZZ_BUILD_DIR)
    config.FUZZ_BUILD_DIR.mkdir(parents=True, exist_ok=True)
    run([config.ODIN, "build", harness, "-build-mode:llvm-ir",
         *config.COLLECTIONS, "-o:speed", f"-out:{config.FUZZ_BUILD_DIR}"])
    single = config.FUZZ_BUILD_DIR / ".ll"
    ll_files = [single] if single.is_file() else sorted(config.FUZZ_BUILD_DIR.glob("*.ll"))
    if not ll_files:
        die(f"Error: no LLVM IR files in {config.FUZZ_BUILD_DIR}")
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
    AddressSanitizer — there is no sanitizer-free variant.
    """
    ll_files = llvm_ir_sources("fuzz_exec")
    run(["afl-clang-fast", "-fsanitize=address",
         *ll_files, "-o", str(config.FUZZ_EXEC_TARGET)])
    log(f"Built {config.FUZZ_EXEC_TARGET} (AFL++ coverage + ASan)")


def _is_executable(path) -> bool:
    return path.is_file() and os.access(path, os.X_OK)


def ensure_cov_target() -> None:
    if not _is_executable(config.FUZZ_TARGET_COV):
        log("Coverage target missing, building...")
        build_cov()


def ensure_cmplog_target() -> None:
    if not _is_executable(config.FUZZ_TARGET_CMPLOG):
        log("cmplog target missing, building...")
        build_cov("cmplog")


def ensure_laf_target() -> None:
    if not _is_executable(config.FUZZ_TARGET_LAF):
        log("laf target missing, building...")
        build_cov("laf")


def ensure_asan_target() -> None:
    if not _is_executable(config.FUZZ_TARGET):
        log("ASan target missing, building...")
        build_asan()


def ensure_exec_target() -> None:
    if not _is_executable(config.FUZZ_EXEC_TARGET):
        log("Exec target missing, building...")
        build_exec()
