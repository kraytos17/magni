"""Shared afl-cmin wrapper plus the `fuzz cmin` command."""

import os
import shutil
import subprocess
from pathlib import Path

from . import config
from .corpus import stage_corpus_tmp
from .util import die, log


def afl_cmin(src: Path, dst: Path, timeout_ms: str = "1000") -> list[str]:
    """Minimize src corpus into dst with afl-cmin. Returns kept filenames."""
    dst.mkdir(parents=True, exist_ok=True)
    e = {**os.environ, "AFL_MAP_SIZE": config.AFL_MAP_SIZE}
    result = subprocess.run(
        ["afl-cmin", "-i", str(src), "-o", str(dst),
         "-m", "none", "-t", timeout_ms,
         "--", str(config.FUZZ_TARGET_COV), "@@"],
        cwd=str(config.ROOT), env=e, capture_output=True, text=True)
    if result.returncode != 0:
        die(f"afl-cmin failed:\n{result.stderr}")
    return sorted(f.name for f in dst.iterdir() if f.is_file())


def cmd_fuzz_cmin() -> None:
    """Minimize corpus (-i fuzz/corpus -o /tmp/min)."""
    tmp = stage_corpus_tmp(config.CORPUS_DIR)
    try:
        out = config.FUZZ_CMIN_OUT_DIR
        if out.exists():
            shutil.rmtree(out)
        out.mkdir(parents=True, exist_ok=True)
        e = {**os.environ, "AFL_MAP_SIZE": config.AFL_MAP_SIZE}
        result = subprocess.run(
            ["afl-cmin", "-i", str(tmp), "-o", str(out),
             "-m", "none", "-t", "1000",
             "--", str(config.FUZZ_TARGET_COV), "@@"],
            cwd=str(config.ROOT), env=e, capture_output=True, text=True)
        tail = (result.stdout + result.stderr).strip().splitlines()[-5:]
        for line in tail:
            log(line)
        n = sum(1 for _ in out.iterdir()) if out.exists() else 0
        log(f"minimized: {n} files in {out}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
