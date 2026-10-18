"""Shared afl-cmin wrapper plus the `fuzz cmin` command."""

import os
import shutil
import subprocess
from pathlib import Path

from . import config
from .corpus import stage_corpus_tmp
from .util import die, log


def afl_cmin(
    src: Path, dst: Path, timeout_ms: str = "1000", target: Path | None = None
) -> list[str]:
    """Minimize src corpus into dst with afl-cmin. Returns kept filenames."""
    if target is None:
        target = config.FUZZ_TARGET_COV
    dst.mkdir(parents=True, exist_ok=True)
    e = {**os.environ, "AFL_MAP_SIZE": config.AFL_MAP_SIZE}
    result = subprocess.run(  # noqa: PLW1510 - returncode checked manually below for die() message
        [
            "afl-cmin",
            "-i",
            str(src),
            "-o",
            str(dst),
            "-m",
            "none",
            "-t",
            timeout_ms,
            "--",
            str(target),
            "@@",
        ],
        cwd=str(config.ROOT),
        env=e,
        capture_output=True,
        text=True,
    )  # noqa: PLW1510 - returncode checked manually below for die() message
    if result.returncode != 0:
        die(f"afl-cmin failed:\n{result.stderr}")
    return sorted(f.name for f in dst.iterdir() if f.is_file())


def cmd_fuzz_cmin(exec_pool: bool = False) -> None:
    """Minimize corpus (-i fuzz/corpus -o target/fuzz/min; exec pool with --exec)."""
    if exec_pool:
        from .build import ensure_exec_target

        ensure_exec_target()
        corpus_dir = config.EXEC_CORPUS_DIR
        out = config.FUZZ_CMIN_EXEC_OUT_DIR
        timeout_ms = config.EXEC_TIMEOUT_MS
        target = config.FUZZ_EXEC_TARGET
    else:
        from .build import ensure_cov_target

        ensure_cov_target()
        corpus_dir = config.CORPUS_DIR
        out = config.FUZZ_CMIN_OUT_DIR
        timeout_ms = "1000"
        target = config.FUZZ_TARGET_COV
    tmp = stage_corpus_tmp(corpus_dir)
    try:
        if out.exists():
            shutil.rmtree(out)
        out.mkdir(parents=True, exist_ok=True)
        kept = afl_cmin(tmp, out, timeout_ms, target)
        log(f"minimized: {len(kept)} files in {out}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
