"""Test and vet commands."""

import argparse

from . import config
from .util import run


def cmd_test(args: argparse.Namespace) -> None:
    """Run all Odin tests."""
    cmd = [config.ODIN, "test", str(config.TEST_DIR), *config.COLLECTIONS,
           *config.TEST_FLAGS]
    if args.verbose:
        cmd.append("-define:ODIN_TEST_FANCY=false")
    if args.name:
        cmd.append(f"-define:ODIN_TEST_NAMES=tests.{args.name}")
    run(cmd)


def cmd_test_cli(args: argparse.Namespace) -> None:
    """Run CLI smoke or full integration tests."""
    script = config.TEST_DIR / ("cli_test.sh" if args.full else "cli_smoke.sh")
    run(["bash", str(script)])


def cmd_test_py(_args: argparse.Namespace) -> None:
    """Run magni.py's own unit tests (stdlib unittest, no Odin/AFL needed)."""
    import sys
    import unittest
    loader = unittest.TestLoader()
    suite = loader.discover(str(config.ROOT / "tests_magni"))
    runner = unittest.TextTestRunner(verbosity=1)
    result = runner.run(suite)
    if not result.wasSuccessful():
        sys.exit(1)


VET_FLAG_MAP = {"shadowing": "-vet-shadowing", "unused": "-vet-unused",
                "style": "-vet-style", "cast": "-vet-cast",
                "semicolon": "-vet-semicolon"}


def cmd_vet(args: argparse.Namespace) -> None:
    """Run odin vet checks."""
    if args.all:
        run([config.ODIN, "build", str(config.SRC_DIR), *config.COLLECTIONS,
             "-vet", "-vet-shadowing",
             "-warnings-as-errors", "-strict-style", "-out:/dev/null"])
        run([config.ODIN, "test", str(config.TEST_DIR), *config.COLLECTIONS,
             "-vet", "-vet-shadowing",
             "-warnings-as-errors", "-strict-style",
             "-define:ODIN_TEST_THREADS=1"])
    else:
        flags = [VET_FLAG_MAP[f] for f in (args.flags or []) if f in VET_FLAG_MAP]
        if not flags:
            flags = ["-vet", "-vet-shadowing"]
        extra = ["-strict-style"] if not args.flags else []
        run([config.ODIN, "check", str(config.SRC_DIR), *config.COLLECTIONS,
             *flags, *extra, "-warnings-as-errors"])
