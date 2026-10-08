"""All paths, tool flags, timeouts, thresholds, and AFL environment.

Single place to change when the toolchain, corpus layout, or fuzz tuning
changes. Nothing here executes anything at import time.
"""

import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT / "src"
TEST_DIR = ROOT / "tests"
TARGET_DIR = ROOT / "target"
TARGET_DEBUG = TARGET_DIR / "debug"
TARGET_RELEASE = TARGET_DIR / "release"
TARGET_FUZZ = TARGET_DIR / "fuzz"
TARGET_FUZZ_EXEC = TARGET_DIR / "fuzz-exec"
FUZZ_DIR = ROOT / "fuzz"
CORPUS_DIR = FUZZ_DIR / "corpus"
GENERATORS_DIR = FUZZ_DIR / "generators"
GEN_CORPUS = GENERATORS_DIR / "gen_corpus.py"
GEN_EXEC_CORPUS = GENERATORS_DIR / "gen_exec_corpus.py"
FUZZ_TARGET = TARGET_FUZZ / "fuzz_target"
FUZZ_TARGET_COV = TARGET_FUZZ / "fuzz_target_cov"
FUZZ_TARGET_CMPLOG = TARGET_FUZZ / "fuzz_target_cmplog"
FUZZ_TARGET_LAF = TARGET_FUZZ / "fuzz_target_laf"
FUZZ_EXEC_DIR = ROOT / "fuzz_exec"
FUZZ_EXEC_TARGET = TARGET_FUZZ_EXEC / "fuzz_exec_target"
EXEC_CORPUS_DIR = FUZZ_DIR / "corpus_exec"
EXEC_PROMOTED = EXEC_CORPUS_DIR / "promoted_seeds.py"
EXEC_TIMEOUT_MS = "5000"
SQL_DICT = FUZZ_DIR / "sql.dict"

# AFL++ Python custom mutator: SQL-aware mutations complementing
# byte-havoc. Loaded via AFL_PYTHON_MODULE with PYTHONPATH=fuzz/.
GRAMMAR_MUTATOR = FUZZ_DIR / "grammar_mutator.py"
ODIN = os.environ.get("ODIN", "odin")
COLLECTIONS = ["-collection:src=src"]
DEBUG_FLAGS = ["-debug", "-o:none", "-warnings-as-errors", "-use-separate-modules"]
RELEASE_FLAGS = [
    "-o:aggressive",
    "-lto:thin",
    "-no-bounds-check",
    "-no-type-assert",
    "-disable-assert",
    "-microarch:native",
    "-source-code-locations:none",
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
    "abort_on_error=1:symbolize=0:detect_stack_use_after_return=1:max_malloc_fill_size=1073741824"
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
DEEP_GUARD_SEEDS = frozenset(
    {
        "deep_subquery_under_guard",
        "deep_subquery_over_guard",
        "deep_parens",
        "deep_parens_511",
        "deep_parens_513",
        "deep_parens_not",
        "deep_check_nested",
    }
)


# A corpus-dir entry is a fuzzer input unless it is a generator script or
# cache dir. (Seed *counts* in corpus generate intentionally differ — see
# corpus.py — so this predicate is only for staging/gating.)
def is_staged_seed(path: Path) -> bool:
    return path.is_file() and path.suffix != ".py" and path.name != "__pycache__"


@dataclass(frozen=True)
class CorpusPool:
    """One promote pool: parser seeds or exec scripts.

    Data only (no behavior — promote.py dispatches ensure/minimize/append
    from these fields, so config never imports build code). The two pools
    differ in seed tables, append target, cmin target/timeout, and labels;
    the 5-step promote flow is shared.
    """

    label: str  # "seed" (parser) or "script" (exec), for log lines
    seed_tables: tuple[str, ...]  # gen-script tables forming the diff basis
    gen_script: Path  # generator holding the seed tables
    corpus_dir: Path  # on-disk seed dir scanned for existing entries
    promoted_path: Path  # file new entries are appended to
    cmin_target: Path  # binary afl-cmin minimizes merged queues against
    cmin_timeout_ms: str
    is_exec: bool  # selects exec generate/test variants in the shared flow


PARSER_POOL = CorpusPool(
    label="seed",
    seed_tables=("SEEDS",),
    gen_script=GEN_CORPUS,
    corpus_dir=CORPUS_DIR,
    promoted_path=GEN_CORPUS,
    cmin_target=FUZZ_TARGET_COV,
    cmin_timeout_ms="1000",
    is_exec=False,
)

EXEC_POOL = CorpusPool(
    label="script",
    seed_tables=("EXEC_SEEDS", "EXEC_PROMOTED"),
    gen_script=GEN_EXEC_CORPUS,
    corpus_dir=EXEC_CORPUS_DIR,
    promoted_path=EXEC_PROMOTED,
    cmin_target=FUZZ_EXEC_TARGET,
    cmin_timeout_ms=EXEC_TIMEOUT_MS,
    is_exec=True,
)


FUZZ_CMIN_MAP_FILE = Path("/tmp/magni.map")
FUZZ_CMIN_OUT_DIR = Path("/tmp/min")
