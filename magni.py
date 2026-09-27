#!/usr/bin/env python3
"""MagniDB build orchestrator. Single entry point for all build/test/fuzz flows.

Usage:
    python3 magni.py <command> [options]

Run `python3 magni.py help` for the full command list.
"""

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC_DIR = ROOT / "src"
TEST_DIR = ROOT / "tests"
BUILD_DIR = ROOT / "build"
FUZZ_DIR = ROOT / "fuzz"
FUZZ_SCRIPTS = FUZZ_DIR / "scripts"
CORPUS_DIR = FUZZ_DIR / "corpus"
GEN_CORPUS = CORPUS_DIR / "gen_corpus.py"
FUZZ_TARGET = FUZZ_DIR / "fuzz_target"
FUZZ_TARGET_COV = FUZZ_DIR / "fuzz_target_cov"
FUZZ_BUILD_DIR = FUZZ_DIR / "build"
SQL_DICT = FUZZ_DIR / "sql.dict"

ODIN = os.environ.get("ODIN", "odin")
COLLECTIONS = ["-collection:src=src"]
DEBUG_FLAGS = ["-debug", "-o:none", "-warnings-as-errors", "-use-separate-modules"]
RELEASE_FLAGS = [
    "-o:aggressive", "-lto:thin", "-no-bounds-check", "-no-type-assert",
    "-disable-assert", "-microarch:native", "-source-code-locations:none",
]
TEST_FLAGS = DEBUG_FLAGS + ["-define:ODIN_TEST_THREADS=1"]
AFL_MAP_SIZE = "10000000"


def log(msg: str) -> None:
    print(msg, flush=True)


def run(cmd: list[str], check: bool = True, env: dict | None = None, cwd: Path | None = None) -> subprocess.CompletedProcess:
    """Run a command with logging. Raises on failure if check=True."""
    log(f"  $ {' '.join(str(c) for c in cmd)}")
    e = os.environ.copy()
    if env:
        e.update(env)
    result = subprocess.run(cmd, cwd=str(cwd or ROOT), env=e)
    if check and result.returncode != 0:
        sys.exit(result.returncode)
    return result


def require_tool(name: str) -> None:
    """Exit with a clear error if a tool is missing from PATH."""
    if shutil.which(name) is None:
        log(f"Error: '{name}' not found on PATH", )
        sys.exit(1)


def md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def cmd_build(args: argparse.Namespace) -> None:
    """Build debug, release, ASan, or coverage fuzz targets."""
    BUILD_DIR.mkdir(exist_ok=True)
    if args.asan:
        run([ODIN, "build", "fuzz", *COLLECTIONS, "-o:none",
             "-sanitize:address", f"-out:{FUZZ_TARGET}"])
        log(f"Built {FUZZ_TARGET} (AddressSanitizer)")
    elif args.cov:
        cmd_build_cov()
    elif args.release:
        run([ODIN, "build", str(SRC_DIR), f"-out:{BUILD_DIR}/magni_release",
             *COLLECTIONS, *RELEASE_FLAGS])
        log(f"Built {BUILD_DIR}/magni_release (release)")
    elif args.check_only:
        run([ODIN, "check", str(SRC_DIR), *COLLECTIONS, "-warnings-as-errors"])
    else:
        run([ODIN, "build", str(SRC_DIR), f"-out:{BUILD_DIR}/magni",
             *COLLECTIONS, *DEBUG_FLAGS])
        log(f"Built {BUILD_DIR}/magni (debug)")


def cmd_build_cov() -> None:
    """Build the coverage-instrumented fuzz target for AFL++."""
    require_tool("afl-clang-fast")
    if FUZZ_BUILD_DIR.exists():
        shutil.rmtree(FUZZ_BUILD_DIR)
    FUZZ_BUILD_DIR.mkdir(parents=True, exist_ok=True)
    run([ODIN, "build", "fuzz", "-build-mode:llvm-ir", *COLLECTIONS,
         "-o:speed", f"-out:{FUZZ_BUILD_DIR}"])
    # -o:speed emits a single merged module named ".ll"; the default emits one .ll per package.
    single = FUZZ_BUILD_DIR / ".ll"
    ll_files = [single] if single.is_file() else sorted(FUZZ_BUILD_DIR.glob("*.ll"))
    if not ll_files:
        log(f"Error: no LLVM IR files in {FUZZ_BUILD_DIR}")
        sys.exit(1)
    run(["afl-clang-fast", *[str(f) for f in ll_files], "-o", str(FUZZ_TARGET_COV)])
    log(f"Built {FUZZ_TARGET_COV} (AFL++ coverage-instrumented)")


def cmd_test(args: argparse.Namespace) -> None:
    """Run all Odin tests."""
    cmd = [ODIN, "test", str(TEST_DIR), *COLLECTIONS, *TEST_FLAGS]
    if args.verbose:
        cmd.append("-define:ODIN_TEST_FANCY=false")
    if args.name:
        cmd.append(f"-define:ODIN_TEST_NAMES=tests.{args.name}")
    run(cmd)


def cmd_test_cli(args: argparse.Namespace) -> None:
    """Run CLI smoke or full integration tests."""
    script = TEST_DIR / ("cli_test.sh" if args.full else "cli_smoke.sh")
    run(["bash", str(script)])


def cmd_vet(args: argparse.Namespace) -> None:
    """Run odin vet checks."""
    if args.all:
        run([ODIN, "build", str(SRC_DIR), *COLLECTIONS, "-vet", "-vet-shadowing",
             "-warnings-as-errors", "-strict-style", "-out:/dev/null"])
        run([ODIN, "test", str(TEST_DIR), *COLLECTIONS, "-vet", "-vet-shadowing",
             "-warnings-as-errors", "-strict-style", "-define:ODIN_TEST_THREADS=1"])
    else:
        flag_map = {"shadowing": "-vet-shadowing", "unused": "-vet-unused",
                    "style": "-vet-style", "cast": "-vet-cast", "semicolon": "-vet-semicolon"}
        flags = [flag_map[f] for f in (args.flags or []) if f in flag_map]
        if not flags:
            flags = ["-vet", "-vet-shadowing"]
        extra = ["-strict-style"] if not args.flags else []
        run([ODIN, "check", str(SRC_DIR), *COLLECTIONS, *flags, *extra, "-warnings-as-errors"])


def ensure_cov_target() -> None:
    if not FUZZ_TARGET_COV.is_file() or not os.access(FUZZ_TARGET_COV, os.X_OK):
        log("Coverage target missing, building...")
        cmd_build_cov()


def ensure_asan_target() -> None:
    if not FUZZ_TARGET.is_file() or not os.access(FUZZ_TARGET, os.X_OK):
        log("ASan target missing, building...")
        ns = argparse.Namespace(asan=True, cov=False, release=False, check_only=False)
        cmd_build(ns)


def fuzz_out_dir() -> Path:
    return Path(os.environ.get("MAGNI_FUZZ_OUT", "fuzz/afl-output"))


def cmd_fuzz(args: argparse.Namespace) -> None:
    """Fuzz subcommand dispatcher."""
    if args.fuzz_cmd == "run":
        ensure_cov_target()
        CORPUS_DIR.mkdir(exist_ok=True)
        env = {"AFL_SKIP_CPUFREQ": "1",
               "AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES": "1",
               "AFL_MAP_SIZE": AFL_MAP_SIZE}
        cmd = ["afl-fuzz", "-i", str(CORPUS_DIR), "-o", str(fuzz_out_dir()),
               "-x", str(SQL_DICT), "-t", "1000", "-m", "none"]
        if args.extra:
            cmd.extend(args.extra)
        cmd += ["--", str(FUZZ_TARGET_COV), "@@"]
        run(cmd, env=env)
    elif args.fuzz_cmd == "campaign":
        ensure_cov_target()
        cmd_fuzz_campaign(args.workers, args.seconds, args.extra or [])
    elif args.fuzz_cmd == "status":
        cmd_fuzz_status()
    elif args.fuzz_cmd == "stop":
        run(["pkill", "afl-fuzz"], check=False)
        log("stopped afl-fuzz (if running)")
    elif args.fuzz_cmd == "showmap":
        ensure_cov_target()
        cmd_fuzz_showmap()
    elif args.fuzz_cmd == "cmin":
        ensure_cov_target()
        cmd_fuzz_cmin()


def cmd_fuzz_campaign(workers: int, seconds: int, extra: list[str]) -> None:
    """Launch a headless parallel AFL++ campaign (1 master + N-1 secondaries)."""
    import signal
    out = fuzz_out_dir()
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)
    env = {"AFL_SKIP_CPUFREQ": "1",
           "AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES": "1",
           "AFL_MAP_SIZE": AFL_MAP_SIZE,
           "AFL_NO_UI": "1"}
    procs: list[subprocess.Popen] = []
    try:
        log(f"Starting {workers} AFL++ workers (output: {out})...")
        procs.append(_spawn_worker("master", out, seconds, extra, env, master=True))
        for i in range(1, workers):
            procs.append(_spawn_worker(f"s{i}", out, seconds, extra, env))
        for p in procs:
            p.wait()
    except KeyboardInterrupt:
        log("Interrupted, killing workers...")
    finally:
        for p in procs:
            if p.poll() is None:
                p.terminate()
    log(f"campaign: {workers} workers, {seconds}s, out={out}")


def _spawn_worker(name: str, out: Path, seconds: int, extra: list[str],
                  env: dict, master: bool = False) -> subprocess.Popen:
    mode_flag = "-M" if master else "-S"
    cmd = ["afl-fuzz", "-i", str(CORPUS_DIR), "-o", str(out),
           "-x", str(SQL_DICT), "-t", "1000", "-m", "none",
           mode_flag, name, "-V", str(seconds)]
    if extra:
        cmd.extend(extra)
    cmd += ["--", str(FUZZ_TARGET_COV), "@@"]
    e = os.environ.copy()
    e.update(env)
    log(f"  $ {' '.join(cmd)}")
    return subprocess.Popen(cmd, cwd=str(ROOT), env=e)


def cmd_fuzz_status() -> None:
    """Print per-worker progress of the running AFL++ campaign."""
    require_tool("rg")
    found = False
    for stats in sorted(fuzz_out_dir().glob("*/fuzzer_stats")):
        found = True
        log(f"== {stats.parent.name} ==")
        subprocess.run(["rg", "execs_done|execs_per_sec|corpus_count|saved_crashes|"
                        "saved_hangs|stability|cycles_done", str(stats)], cwd=str(ROOT))
    if not found:
        log(f"No fuzzer_stats found in {fuzz_out_dir()}/")


def _stage_corpus_tmp() -> Path:
    """Stage corpus files (excluding .py/pycache) into a temp dir. Returns path."""
    tmp = Path(tempfile.mkdtemp(prefix="magni_corpus_"))
    for f in CORPUS_DIR.iterdir():
        if f.is_file() and f.suffix != ".py" and f.name != "__pycache__":
            shutil.copy2(f, tmp / f.name)
    return tmp


def cmd_fuzz_showmap() -> None:
    """Show coverage tuples for current corpus."""
    tmp = _stage_corpus_tmp()
    try:
        map_file = Path("/tmp/magni.map")
        if map_file.exists():
            map_file.unlink()
        cmd = ["afl-showmap", "-C", "-i", str(tmp), "-o", str(map_file),
               "--", str(FUZZ_TARGET_COV), "@@"]
        e = os.environ.copy()
        e["AFL_MAP_SIZE"] = AFL_MAP_SIZE
        result = subprocess.run(cmd, cwd=str(ROOT), env=e,
                                capture_output=True, text=True)
        for line in result.stdout.splitlines() + result.stderr.splitlines():
            if "Captured" in line or "coverage" in line:
                # Strip ANSI codes for readability
                import re as _re
                log(_re.sub(r"\x1b\[[0-9;]*m", "", line))
        try:
            n = sum(1 for _ in map_file.read_text().splitlines()) if map_file.exists() else 0
        except OSError:
            n = 0
        log(f"map: {n} tuples listed in {map_file}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def cmd_fuzz_cmin() -> None:
    """Minimize corpus (-i fuzz/corpus -o /tmp/min)."""
    tmp = _stage_corpus_tmp()
    try:
        out = Path("/tmp/min")
        if out.exists():
            shutil.rmtree(out)
        out.mkdir(parents=True, exist_ok=True)
        cmd = ["afl-cmin", "-i", str(tmp), "-o", str(out),
               "-m", "none", "-t", "1000",
               "--", str(FUZZ_TARGET_COV), "@@"]
        e = os.environ.copy()
        e["AFL_MAP_SIZE"] = AFL_MAP_SIZE
        result = subprocess.run(cmd, cwd=str(ROOT), env=e,
                                capture_output=True, text=True)
        tail = (result.stdout + result.stderr).strip().splitlines()[-5:]
        for line in tail:
            log(line)
        n = sum(1 for _ in out.iterdir()) if out.exists() else 0
        log(f"minimized: {n} files in {out}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def cmd_corpus(args: argparse.Namespace) -> None:
    """Corpus subcommand dispatcher."""
    if args.corpus_cmd == "generate":
        run([sys.executable, str(GEN_CORPUS)])
        pycache = CORPUS_DIR / "__pycache__"
        if pycache.exists():
            shutil.rmtree(pycache)
        n = sum(1 for f in CORPUS_DIR.iterdir()
                if f.is_file() and f.name != "gen_corpus.py" and f.name != "__pycache__")
        import subprocess as _sp
        size = _sp.run(["du", "-sh", str(CORPUS_DIR)], capture_output=True, text=True)
        sz = size.stdout.split()[0] if size.stdout else "?"
        log(f"corpus: {n} seeds in {CORPUS_DIR}/ ({sz})")
    elif args.corpus_cmd == "test":
        ensure_asan_target()
        n = 0
        for f in sorted(CORPUS_DIR.iterdir()):
            if not f.is_file() or f.suffix == ".py" or f.name == "__pycache__":
                continue
            env = {"ASAN_OPTIONS": "detect_leaks=0"}
            result = run([str(FUZZ_TARGET), str(f)], check=False, env=env)
            if result.returncode != 0:
                log(f"FAILED on seed: {f}")
                sys.exit(1)
            n += 1
        log(f"All {n} fuzz seeds passed under ASan.")
    elif args.corpus_cmd == "promote":
        cmd_corpus_promote(args.minimize, args.dry_run, test_after=False)


def parse_seeds() -> dict[str, bytes]:
    """Load SEEDS from gen_corpus.py via exec. Returns {md5: (name, content)}."""
    ns = {"__name__": "gen_corpus_parsing", "__file__": str(GEN_CORPUS)}
    try:
        exec(compile(GEN_CORPUS.read_text(), str(GEN_CORPUS), "exec"), ns)
    except SystemExit:
        pass  # gen_corpus.py calls sys.exit(main()) — ignore it
    except Exception:
        pass
    seeds = {}
    for entry in ns.get("SEEDS", []):
        if not isinstance(entry, (tuple, list)) or len(entry) != 2:
            continue
        name, content = entry
        if isinstance(content, str):
            content = content.encode("utf-8")
        if isinstance(content, bytes):
            seeds[md5(content)] = (name, content)
    return seeds


def parse_afl_metadata(filename: str) -> str:
    """Extract useful AFL metadata from queue filename for a comment."""
    parts, meta = filename.split(","), []
    for p in parts:
        if p.startswith("orig:"):
            meta.append(p[5:])
        elif p.startswith("+cov"):
            meta.append("+cov")
        elif p.startswith("sync:"):
            meta.append(f"sync:{p.split(':')[1]}")
    return ",".join(meta) if meta else filename


def cmd_corpus_promote(minimize: bool, dry_run: bool, test_after: bool = False) -> None:
    """Merge AFL++ worker queues, dedup, diff, and append new seeds to gen_corpus.py."""
    out = fuzz_out_dir()
    if not out.exists():
        log(f"Error: {out} not found")
        sys.exit(1)

    log("[1/5] Merging worker queues...")
    seen: dict[str, tuple[bytes, str]] = {}
    for queue_dir in sorted(out.glob("*/queue")):
        for f in sorted(queue_dir.iterdir()):
            if f.is_file() and not f.name.endswith(".py"):
                content = f.read_bytes()
                h = md5(content)
                if h not in seen:
                    seen[h] = (content, parse_afl_metadata(f.name))
    log(f"  {len(seen)} unique items across workers")
    if not seen:
        log("No queue items found. Nothing to promote.")
        return

    if minimize:
        ensure_cov_target()
        log("[2/5] Minimizing with afl-cmin...")
        with tempfile.TemporaryDirectory(prefix="magni_merge_") as tmpdir:
            merge_path = Path(tmpdir) / "merged"
            merge_path.mkdir()
            for h, (content, _) in seen.items():
                (merge_path / h).write_bytes(content)
            min_path = Path(tmpdir) / "minimized"
            min_path.mkdir()
            e = os.environ.copy()
            e["AFL_MAP_SIZE"] = AFL_MAP_SIZE
            result = subprocess.run(
                ["afl-cmin", "-i", str(merge_path), "-o", str(min_path),
                 "-m", "none", "-t", "1000",
                 "--", str(FUZZ_TARGET_COV), "@@"],
                cwd=str(ROOT), env=e, capture_output=True, text=True)
            if result.returncode != 0:
                log(f"afl-cmin failed:\n{result.stderr}")
                sys.exit(1)
            minimized = sorted(min_path.iterdir())
            log(f"  {len(minimized)} seeds after minimization")
            seen = {}
            for f in minimized:
                content = f.read_bytes()
                seen[md5(content)] = (content, parse_afl_metadata(f.name))
    else:
        log("[2/5] Skipping minimization (use --minimize to enable)")

    log("[3/5] Loading existing seeds...")
    existing = parse_seeds()
    for f in CORPUS_DIR.iterdir():
        if f.is_file() and f.name != "gen_corpus.py" and not f.name.startswith("__"):
            h = md5(f.read_bytes())
            if h not in existing:
                existing[h] = (f.name, f.read_bytes())
    log(f"  {len(existing)} existing seeds (SEEDS + on-disk)")

    log("[4/5] Diffing against existing corpus...")
    new_seeds = [(content, meta) for h, (content, meta) in seen.items()
                 if h not in existing]
    if not new_seeds:
        log("  No new seeds found. Corpus is up to date.")
        return
    log(f"  {len(new_seeds)} new seeds to promote")

    if dry_run:
        log(f"\n[DRY RUN] Would add {len(new_seeds)} seeds to {GEN_CORPUS}")
        return

    log(f"[5/5] Appending {len(new_seeds)} seeds to {GEN_CORPUS}...")
    existing_names = {name for name, _ in existing.values()}
    max_index = 0
    for name in existing_names:
        if name.startswith("promoted_"):
            try:
                max_index = max(max_index, int(name.split("_")[1]))
            except (IndexError, ValueError):
                pass
    new_entries = []
    for content, metadata in new_seeds:
        max_index += 1
        name = f"promoted_{max_index:04d}"
        while name in existing_names:
            max_index += 1
            name = f"promoted_{max_index:04d}"
        existing_names.add(name)
        comment = f"  # {metadata}" if metadata else ""
        new_entries.append(f'    ("{name}", {content!r}),{comment}')

    text = GEN_CORPUS.read_text()
    lines = text.split("\n")
    insert_pos = len(lines) - 1
    for i in range(len(lines) - 1, -1, -1):
        stripped = lines[i].strip()
        if stripped.startswith("(") and stripped.endswith(("),", "),")):
            insert_pos = i + 1
            break
    for entry in reversed(new_entries):
        lines.insert(insert_pos, entry)
    GEN_CORPUS.write_text("\n".join(lines))
    log(f"  Done. Total seeds: {len(existing) + len(new_seeds)}")

    if test_after:
        cmd_corpus(argparse.Namespace(corpus_cmd="generate"))
        cmd_corpus(argparse.Namespace(corpus_cmd="test"))


def cmd_clean(args: argparse.Namespace) -> None:
    """Remove build artifacts."""
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
        log(f"cleaned {BUILD_DIR}/")
    if args.all or args.fuzz_only:
        out = fuzz_out_dir()
        if out.exists():
            shutil.rmtree(out)
        if FUZZ_BUILD_DIR.exists():
            shutil.rmtree(FUZZ_BUILD_DIR)
        pycache = CORPUS_DIR / "__pycache__"
        if pycache.exists():
            shutil.rmtree(pycache)
        log("cleaned fuzz artifacts")


def cmd_help(_args: argparse.Namespace) -> None:
    print("""MagniDB — magni.py help

  BUILD
    magni.py build              build debug binary → build/magni
    magni.py build --release    build release binary (LTO) → build/magni_release
    magni.py build --asan       ASan fuzz target → fuzz/fuzz_target
    magni.py build --cov        coverage target → fuzz/fuzz_target_cov
    magni.py build --check-only parse + type check (no vet)

  TEST
    magni.py test               run all Odin tests
    magni.py test --verbose     verbose (no fancy)
    magni.py test --name NAME   one test: --name test_integration_vacuum
    magni.py test-cli           basic CLI smoke (needs build/magni)
    magni.py test-cli --full    comprehensive CLI integration

  VET
    magni.py vet                fast vet (odin check, full flags)
    magni.py vet --all          LLVM vet via build+test (strict, shadowing)
    magni.py vet --shadowing|--unused|--style|--cast|--semicolon

  FUZZ
    magni.py fuzz run [-- --extra args]   interactive AFL++ campaign
    magni.py fuzz campaign -w N -s SEC     headless parallel campaign
    magni.py fuzz status                   per-worker fuzzer_stats
    magni.py fuzz stop                     pkill afl-fuzz
    magni.py fuzz showmap                  coverage tuples for current corpus
    magni.py fuzz cmin                     minimize corpus → /tmp/min

  CORPUS
    magni.py corpus generate               regenerate 2479 seeds → fuzz/corpus/
    magni.py corpus test                   ASan gate (every corpus seed)
    magni.py corpus promote [--minimize] [--dry-run]

  CLEAN
    magni.py clean              remove build/ only
    magni.py clean --all        remove build/ + fuzz artifacts

  Env overrides: ODIN=odin, MAGNI_FUZZ_OUT=<dir>
""")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="magni.py", description="MagniDB build orchestrator")
    sub = p.add_subparsers(dest="command", required=True)

    # build
    pb = sub.add_parser("build", help="build debug/release/asan/cov targets")
    pb.add_argument("--release", action="store_true")
    pb.add_argument("--asan", action="store_true")
    pb.add_argument("--cov", action="store_true")
    pb.add_argument("--check-only", action="store_true")
    pb.set_defaults(func=cmd_build)

    # test
    pt = sub.add_parser("test", help="run Odin tests")
    pt.add_argument("--verbose", action="store_true")
    pt.add_argument("--name", default=None, help="single test name")
    pt.set_defaults(func=cmd_test)

    # test-cli
    ptc = sub.add_parser("test-cli", help="run CLI smoke/full tests")
    ptc.add_argument("--full", action="store_true", help="comprehensive CLI integration")
    ptc.set_defaults(func=cmd_test_cli)

    # vet
    pv = sub.add_parser("vet", help="run odin vet checks")
    pv.add_argument("--all", action="store_true")
    pv.add_argument("--shadowing", dest="flags", action="append_const", const="shadowing")
    pv.add_argument("--unused", dest="flags", action="append_const", const="unused")
    pv.add_argument("--style", dest="flags", action="append_const", const="style")
    pv.add_argument("--cast", dest="flags", action="append_const", const="cast")
    pv.add_argument("--semicolon", dest="flags", action="append_const", const="semicolon")
    pv.set_defaults(func=cmd_vet)

    # fuzz
    pf = sub.add_parser("fuzz", help="AFL++ campaign management")
    fsub = pf.add_subparsers(dest="fuzz_cmd", required=True)
    fr = fsub.add_parser("run", help="interactive AFL++ campaign")
    fr.add_argument("extra", nargs=argparse.REMAINDER)
    fc = fsub.add_parser("campaign", help="headless parallel campaign")
    fc.add_argument("-w", "--workers", type=int, default=4)
    fc.add_argument("-s", "--seconds", type=int, default=3600)
    fc.add_argument("extra", nargs=argparse.REMAINDER)
    fsub.add_parser("status", help="per-worker fuzzer_stats")
    fsub.add_parser("stop", help="pkill afl-fuzz")
    fsub.add_parser("showmap", help="coverage tuples for current corpus")
    fsub.add_parser("cmin", help="minimize corpus → /tmp/min")
    pf.set_defaults(func=cmd_fuzz)

    # corpus
    pc = sub.add_parser("corpus", help="seed corpus management")
    csub = pc.add_subparsers(dest="corpus_cmd", required=True)
    csub.add_parser("generate", help="regenerate corpus from gen_corpus.py")
    csub.add_parser("test", help="ASan gate on every seed")
    pp = csub.add_parser("promote", help="promote grown queue into gen_corpus.py")
    pp.add_argument("--minimize", action="store_true")
    pp.add_argument("--dry-run", action="store_true")
    pc.set_defaults(func=cmd_corpus)

    # clean
    pcl = sub.add_parser("clean", help="remove build artifacts")
    pcl.add_argument("--all", action="store_true")
    pcl.add_argument("--fuzz-only", action="store_true")
    pcl.set_defaults(func=cmd_clean)

    # help
    ph = sub.add_parser("help", help="show this help")
    ph.set_defaults(func=cmd_help)

    return p


def main() -> None:
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
