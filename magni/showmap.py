"""afl-showmap coverage helpers with a forkserver-flakiness guard."""

import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from . import config
from .corpus import stage_corpus_tmp
from .util import die, log


def run_showmap(input_dir: Path, map_file: Path) -> int | None:
    """Run afl-showmap, return captured tuple count (None on failure).

    Guards against forkserver flakiness: a collapsed run (few dozen startup
    edges instead of real coverage) is retried once, then reported as failure
    rather than silently trusted.
    """
    for attempt in (1, 2):
        if map_file.exists():
            map_file.unlink()
        cmd = ["afl-showmap", "-C", "-i", str(input_dir), "-o", str(map_file),
               "--", str(config.FUZZ_TARGET_COV), "@@"]
        e = {"AFL_MAP_SIZE": config.AFL_MAP_SIZE}
        result = subprocess.run(cmd, cwd=str(config.ROOT),
                                env={**os.environ, **e},
                                capture_output=True, text=True)
        captured = None
        for line in result.stdout.splitlines() + result.stderr.splitlines():
            clean = re.sub(r"\x1b\[[0-9;]*m", "", line)
            if "Captured" in clean or "coverage" in clean:
                log(clean)
            m = re.search(r"Captured (\d+) tuples", clean)
            if m:
                captured = int(m.group(1))
        # Healthy corpora yield hundreds+ tuples; a collapse to dozens means
        # the forkserver handshake degraded — retry, don't trust it.
        if captured is not None and captured >= config.SHOWMAP_HEALTHY_MIN_TUPLES:
            return captured
        log(f"showmap run {attempt}: suspicious tuple count ({captured}), "
            f"{'retrying' if attempt == 1 else 'aborting'}")
    return None


def showmap_tuples(input_dir: Path) -> int | None:
    """Tuple count for an input dir via run_showmap into a temp map file."""
    with tempfile.NamedTemporaryFile(suffix=".map", delete=False) as tf:
        map_file = Path(tf.name)
    try:
        return run_showmap(input_dir, map_file)
    finally:
        map_file.unlink(missing_ok=True)


def cmd_fuzz_showmap() -> None:
    """Show coverage tuples for current corpus."""
    tmp = stage_corpus_tmp(config.CORPUS_DIR)
    try:
        n = run_showmap(tmp, config.FUZZ_CMIN_MAP_FILE)
        if n is None:
            die("showmap failed or degraded twice; see above")
        try:
            listed = sum(1 for _ in config.FUZZ_CMIN_MAP_FILE.read_text().splitlines()) \
                if config.FUZZ_CMIN_MAP_FILE.exists() else 0
        except OSError:
            listed = 0
        log(f"map: {listed} tuples listed in {config.FUZZ_CMIN_MAP_FILE}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
