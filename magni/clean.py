"""Clean command: remove build artifacts."""

import argparse
import shutil

from . import config
from .fuzz import fuzz_out_dir
from .util import log


def cmd_clean(args: argparse.Namespace) -> None:
    """Remove build artifacts."""
    if config.BUILD_DIR.exists():
        shutil.rmtree(config.BUILD_DIR)
        log(f"cleaned {config.BUILD_DIR}/")
    if args.all or args.fuzz_only:
        out = fuzz_out_dir()
        if out.exists():
            shutil.rmtree(out)
        if config.FUZZ_BUILD_DIR.exists():
            shutil.rmtree(config.FUZZ_BUILD_DIR)
        pycache = config.CORPUS_DIR / "__pycache__"
        if pycache.exists():
            shutil.rmtree(pycache)
        log("cleaned fuzz artifacts")
