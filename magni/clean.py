"""Clean command: remove build artifacts."""

import argparse
import shutil

from . import config
from .fuzz import fuzz_out_dir
from .util import log


def cmd_clean(args: argparse.Namespace) -> None:
    """Remove build artifacts."""
    for d in (config.TARGET_DEBUG, config.TARGET_RELEASE):
        if d.exists():
            shutil.rmtree(d)
            log(f"cleaned {d}/")
    if args.all or args.fuzz_only:
        out = fuzz_out_dir()
        if out.exists():
            shutil.rmtree(out)
        for d in (config.TARGET_FUZZ, config.TARGET_FUZZ_EXEC):
            if d.exists():
                shutil.rmtree(d)
        pycache = config.CORPUS_DIR / "__pycache__"
        if pycache.exists():
            shutil.rmtree(pycache)
        log("cleaned fuzz artifacts")
