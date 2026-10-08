"""CLI: argument parsing and help text.

Help lists (roles, commands) are rendered from roles.py/config.py so they can
never drift from the implementation — previously the role list lived in three
places (help heredoc, --roles help, Makefile comment).
"""

import argparse

from .build import apply_rebuild_flag, cmd_build
from .clean import cmd_clean
from .corpus import corpus_generate, corpus_test
from .fuzz import cmd_fuzz
from .minimize import corpus_minimize
from .promote import corpus_promote
from .promote_exec import corpus_promote_exec
from .roles import role_names
from .testcmd import cmd_test, cmd_test_cli, cmd_test_py, cmd_vet


def _corpus_test(args: argparse.Namespace) -> None:
    apply_rebuild_flag(args)
    corpus_test(exec_scripts=args.exec_test)


def _corpus_promote(args: argparse.Namespace) -> None:
    if args.exec_promote:
        corpus_promote_exec(args.minimize, args.dry_run, test_after=args.test)
    else:
        corpus_promote(args.minimize, args.dry_run, test_after=args.test)


def cmd_help(_args: argparse.Namespace) -> None:
    print(f"""MagniDB — magni.py help

  BUILD
    magni.py build              build debug binary → target/debug/magni
    magni.py build --release    build release binary (LTO) → target/release/magni
    magni.py build --asan       ASan fuzz target → target/fuzz/fuzz_target
    magni.py build --cov        coverage target → target/fuzz/fuzz_target_cov
    magni.py build --cov --cmplog  RedQueen binary → target/fuzz/fuzz_target_cmplog
    magni.py build --cov --laf     LAF-INTEL binary → target/fuzz/fuzz_target_laf
    magni.py build --exec         executor/storage target (coverage + ASan) → target/fuzz-exec/fuzz_exec_target
    magni.py build --check-only parse + type check (no vet)

  TEST
    magni.py test               run all Odin tests
    magni.py test --verbose     verbose (no fancy)
    magni.py test --name NAME   one test: --name test_integration_vacuum
    magni.py test-py            run magni.py's own unit tests (no Odin/AFL needed)
    magni.py test-cli           basic CLI smoke (needs target/debug/magni)
    magni.py test-cli --full    comprehensive CLI integration

  VET
    magni.py vet                fast vet (odin check, full flags)
    magni.py vet --all          LLVM vet via build+test (strict, shadowing)
    magni.py vet --shadowing|--unused|--style|--cast|--semicolon

  FUZZ
    magni.py fuzz run [-- --extra args]   interactive AFL++ campaign
    magni.py fuzz campaign -w N -s SEC [--roles R,...]
      default roles: master + rotation (cmplog,asan,fast,explore,coe,laf,mopt)
      roles: {role_names()}
        (exec runs full scripts against scratch DBs, own seed pool+timeout)
        (grammar/exec_grammar add the SQL-aware Python mutator on top of havoc)
    magni.py fuzz status                   per-worker fuzzer_stats
    magni.py fuzz stop                     pkill afl-fuzz
    magni.py fuzz showmap                  coverage tuples for current corpus
    magni.py fuzz cmin                     minimize corpus → /tmp/min

  CORPUS
    magni.py corpus generate               regenerate seeds → fuzz/corpus/
    magni.py corpus generate --exec         regenerate exec scripts → fuzz/corpus_exec/
    magni.py corpus test                   ASan gate (every corpus seed)
    magni.py corpus test --exec             ASan gate (every exec script)
    magni.py corpus promote [--minimize] [--dry-run] [--test] [--exec]
    magni.py corpus minimize               cmin + purge, rewrite promoted_seeds.py

  CLEAN
    magni.py clean              remove target/debug + target/release only
    magni.py clean --all        remove target/ + fuzz artifacts

  Env overrides: ODIN=odin, MAGNI_FUZZ_OUT=<dir>, MAGNI_REBUILD=1 (force rebuild)
""")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="magni.py", description="MagniDB build orchestrator")
    sub = p.add_subparsers(dest="command", required=True)

    # build
    pb = sub.add_parser("build", help="build debug/release/asan/cov targets")
    pb.add_argument("--release", action="store_true")
    pb.add_argument("--asan", action="store_true")
    pb.add_argument("--cov", action="store_true")
    pb.add_argument(
        "--cmplog", action="store_true", help="with --cov: RedQueen input-to-state binary"
    )
    pb.add_argument(
        "--laf", action="store_true", help="with --cov: LAF-INTEL comparison-splitting binary"
    )
    pb.add_argument(
        "--exec",
        dest="exec_fuzz",
        action="store_true",
        help="executor/storage target (coverage + ASan combined)",
    )
    pb.add_argument("--check-only", action="store_true")
    pb.add_argument(
        "--rebuild", action="store_true", help="force rebuild even if the target looks fresh"
    )
    pb.set_defaults(func=cmd_build)

    # test
    pt = sub.add_parser("test", help="run Odin tests")
    pt.add_argument("--verbose", action="store_true")
    pt.add_argument("--name", default=None, help="single test name")
    pt.set_defaults(func=cmd_test)

    # test-py
    ptp = sub.add_parser("test-py", help="run magni.py's own unit tests")
    ptp.set_defaults(func=cmd_test_py)

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
    fr.add_argument("--rebuild", action="store_true", help="force target rebuild even if fresh")
    fr.add_argument("extra", nargs=argparse.REMAINDER)
    fc = fsub.add_parser("campaign", help="headless parallel campaign")
    fc.add_argument("-w", "--workers", type=int, default=4)
    fc.add_argument("-s", "--seconds", type=int, default=3600)
    fc.add_argument("--rebuild", action="store_true", help="force target rebuild even if fresh")
    fc.add_argument(
        "--roles",
        default=None,
        help=f"comma-separated worker roles (default: master + rotation). Roles: {role_names()}",
    )
    fc.add_argument("extra", nargs=argparse.REMAINDER)
    fsub.add_parser("status", help="per-worker fuzzer_stats")
    fsub.add_parser("stop", help="pkill afl-fuzz")
    fsub.add_parser("showmap", help="coverage tuples for current corpus")
    fsub.add_parser("cmin", help="minimize corpus → /tmp/min")
    pf.set_defaults(func=cmd_fuzz)

    # corpus
    pc = sub.add_parser("corpus", help="seed corpus management")
    csub = pc.add_subparsers(dest="corpus_cmd", required=True)
    cg = csub.add_parser("generate", help="regenerate corpus from gen_corpus.py")
    cg.add_argument(
        "--exec",
        dest="exec_gen",
        action="store_true",
        help="regenerate exec scripts from gen_exec_corpus.py instead",
    )
    cg.set_defaults(func=lambda args: corpus_generate(exec_scripts=args.exec_gen))
    ct = csub.add_parser("test", help="ASan gate on every seed")
    ct.add_argument(
        "--exec",
        dest="exec_test",
        action="store_true",
        help="gate exec seeds with the exec target instead",
    )
    ct.add_argument("--rebuild", action="store_true", help="force target rebuild even if fresh")
    ct.set_defaults(func=_corpus_test)
    pp = csub.add_parser("promote", help="promote grown queue into gen_corpus.py")
    pp.add_argument("--minimize", action="store_true")
    pp.add_argument("--dry-run", action="store_true")
    pp.add_argument(
        "--test", action="store_true", help="regenerate corpus + ASan gate after promoting"
    )
    pp.add_argument(
        "--exec",
        dest="exec_promote",
        action="store_true",
        help="promote exec queue into corpus_exec/promoted_seeds.py instead",
    )
    pp.set_defaults(func=_corpus_promote)
    cm = csub.add_parser("minimize", help="afl-cmin + monster purge, rewrite promoted_seeds.py")
    cm.set_defaults(func=lambda _args: corpus_minimize())

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
