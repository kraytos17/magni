"""All paths, tool flags, timeouts, thresholds, and AFL environment.

Single place to change when the toolchain, corpus layout, or fuzz tuning
changes. Nothing here executes anything at import time.
"""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT / "src"
TEST_DIR = ROOT / "tests"
BUILD_DIR = ROOT / "build"
FUZZ_DIR = ROOT / "fuzz"
FUZZ_SCRIPTS = FUZZ_DIR / "scripts"
CORPUS_DIR = FUZZ_DIR / "corpus"
FUZZ_BUILD_DIR = FUZZ_DIR / "build"
GENERATORS_DIR = FUZZ_DIR / "generators"
GEN_CORPUS = GENERATORS_DIR / "gen_corpus.py"
GEN_EXEC_CORPUS = GENERATORS_DIR / "gen_exec_corpus.py"
FUZZ_TARGET = FUZZ_BUILD_DIR / "fuzz_target"
FUZZ_TARGET_COV = FUZZ_BUILD_DIR / "fuzz_target_cov"
FUZZ_TARGET_CMPLOG = FUZZ_BUILD_DIR / "fuzz_target_cmplog"
FUZZ_TARGET_LAF = FUZZ_BUILD_DIR / "fuzz_target_laf"
FUZZ_EXEC_DIR = ROOT / "fuzz_exec"
FUZZ_EXEC_TARGET = FUZZ_BUILD_DIR / "fuzz_exec_target"
EXEC_CORPUS_DIR = FUZZ_DIR / "corpus_exec"
EXEC_TIMEOUT_MS = "5000"
SQL_DICT = FUZZ_DIR / "sql.dict"

# AFL++ Python custom mutator: SQL-aware mutations complementing
# byte-havoc. Loaded via AFL_PYTHON_MODULE with PYTHONPATH=fuzz/.
GRAMMAR_MUTATOR = FUZZ_DIR / "grammar_mutator.py"
ODIN = os.environ.get("ODIN", "odin")
COLLECTIONS = ["-collection:src=src"]
DEBUG_FLAGS = ["-debug", "-o:none", "-warnings-as-errors", "-use-separate-modules"]
RELEASE_FLAGS = [
    "-o:aggressive", "-lto:thin", "-no-bounds-check", "-no-type-assert",
    "-disable-assert", "-microarch:native", "-source-code-locations:none",
]

TEST_FLAGS = DEBUG_FLAGS + ["-define:ODIN_TEST_THREADS=1"]
AFL_MAP_SIZE = "10000000"

# Base environment for every afl-fuzz invocation.
AFL_BASE_ENV = {
    "AFL_SKIP_CPUFREQ": "1",
    "AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES": "1",
    "AFL_MAP_SIZE": AFL_MAP_SIZE,
}

ASAN_FUZZ_OPTIONS = (
    "abort_on_error=1:symbolize=0:"
    "detect_stack_use_after_return=1:max_malloc_fill_size=1073741824"
)

# Default per-role afl-fuzz timeout (exec roles override to EXEC_TIMEOUT_MS).
DEFAULT_TIMEOUT_MS = "1000"

# showmap health guard: a run yielding fewer tuples than this is treated as
# forkserver flakiness (retry once, then abort). Observed 2026-09-29: 55-tuple
# runs interleaved with healthy 1441-tuple runs on identical inputs.
SHOWMAP_HEALTHY_MIN_TUPLES = 200

# Seeds larger than this are dropped as monsters (calibration/ASan-gate cost
# dwarfs their marginal coverage). Verified 0 tuple loss on first use.
MONSTER_SIZE_CAP = 10 * 1024

# Deep-nesting guard seeds are deliberate (MAX_PARSE_NESTING paths) and always
# regenerated — never purge them in minimize.
DEEP_GUARD_SEEDS = frozenset({
    "deep_subquery_under_guard", "deep_subquery_over_guard",
    "deep_parens", "deep_parens_511", "deep_parens_513",
    "deep_parens_not", "deep_check_nested",
})

# A corpus-dir entry is a fuzzer input unless it is a generator script or
# cache dir. (Seed *counts* in corpus generate intentionally differ — see
# corpus.py — so this predicate is only for staging/gating.)
def is_staged_seed(path: Path) -> bool:
    return path.is_file() and path.suffix != ".py" and path.name != "__pycache__"


FUZZ_CMIN_MAP_FILE = Path("/tmp/magni.map")
FUZZ_CMIN_OUT_DIR = Path("/tmp/min")
