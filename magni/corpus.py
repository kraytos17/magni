"""Seed corpus management: staging, generation, ASan gates, seed parsing."""

import hashlib
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from . import config
from .build import ensure_asan_target, ensure_exec_target
from .util import die, log, run


def stage_corpus_into(src: Path, dst: Path) -> int:
    """Copy seed files (excluding generator sources) into dst. Returns count."""
    dst.mkdir(parents=True, exist_ok=True)
    n = 0
    for f in src.iterdir():
        if config.is_staged_seed(f):
            shutil.copy2(f, dst / f.name)
            n += 1
    return n


def stage_corpus_tmp(src: Path) -> Path:
    """Stage seeds into a fresh temp dir. Caller removes it. Returns path."""
    tmp = Path(tempfile.mkdtemp(prefix="magni_corpus_"))
    stage_corpus_into(src, tmp)
    return tmp


def stage_clean_corpus(src: Path) -> Path:
    """Stage fuzz inputs excluding generator sources.

    afl-fuzz reads every file in -i as a seed, but the corpus dirs also hold
    the generator scripts — feeding Python source as SQL wastes cycles and
    produces junk-derived "crashes". The staging dir holds only real seeds.
    """
    dst = config.TARGET_FUZZ / f"inputs_{src.name}"
    if dst.exists():
        shutil.rmtree(dst)
    n = stage_corpus_into(src, dst)
    if n == 0:
        die(f"Error: no seeds staged from {src} (empty corpus?)")
    return dst


def md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def parse_seed_tables(script: Path, *tables: str) -> dict[str, tuple[str, bytes]]:
    """Load seed tables from a generator script via exec. Returns {md5: (name, content)}.

    Shared by the parser corpus (SEEDS) and the exec pool (EXEC_SEEDS +
    EXEC_PROMOTED). The gen script calls sys.exit(main()) — ignored, like any
    corrupt-script failure (falls back to an empty set; the on-disk scan in
    load_existing still applies).
    """
    ns = {"__name__": "seed_parsing", "__file__": str(script)}
    try:
        exec(compile(script.read_text(), str(script), "exec"), ns)  # noqa: S102 - gen script is first-party, exec is the loader by design
    except SystemExit:
        pass  # gen script calls sys.exit(main()) — ignore it
    except Exception:  # noqa: BLE001, S110 - corrupt gen script falls back to empty seed set by design
        pass
    seeds = {}
    for table in tables:
        for entry in ns.get(table, []):
            if not isinstance(entry, (tuple, list)) or len(entry) != 2:
                continue
            name, content = entry
            if isinstance(content, str):
                content = content.encode("utf-8")
            if isinstance(content, bytes):
                seeds[md5(content)] = (name, content)
    return seeds


def parse_seeds() -> dict[str, tuple[str, bytes]]:
    """Load SEEDS from gen_corpus.py via exec. Returns {md5: (name, content)}."""
    return parse_seed_tables(config.GEN_CORPUS, "SEEDS")


def purge_pycache(corpus_dir: Path) -> None:
    """Remove a corpus dir's __pycache__ (regenerators leave it behind)."""
    pycache = corpus_dir / "__pycache__"
    if pycache.exists():
        shutil.rmtree(pycache)


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


def corpus_generate(exec_scripts: bool = False) -> None:
    """Regenerate seeds (parser corpus) or exec scripts (with exec_scripts)."""
    if exec_scripts:
        gen_exec = config.GEN_EXEC_CORPUS
        run([sys.executable, str(gen_exec)])
        purge_pycache(config.EXEC_CORPUS_DIR)
        # NOTE: counts every non-.py file, including any future promoted file.
        n = sum(
            1
            for f in config.EXEC_CORPUS_DIR.iterdir()
            if f.is_file() and f.suffix != ".py" and f.name != "__pycache__"
        )
        log(f"exec corpus: {n} seeds in {config.EXEC_CORPUS_DIR}/")
        return
    run([sys.executable, str(config.GEN_CORPUS)])
    purge_pycache(config.CORPUS_DIR)
    # NOTE: intentionally counts promoted_seeds.py as well (historical quirk).
    n = sum(
        1
        for f in config.CORPUS_DIR.iterdir()
        if f.is_file() and f.name != "gen_corpus.py" and f.name != "__pycache__"
    )
    size = subprocess.run(["du", "-sh", str(config.CORPUS_DIR)], capture_output=True, text=True)  # noqa: PLW1510 - du failure yields "?" size, not fatal
    sz = size.stdout.split()[0] if size.stdout else "?"
    log(f"corpus: {n} seeds in {config.CORPUS_DIR}/ ({sz})")


def gate_seeds(target: Path, seeds: list[Path], env: dict, fail_label: str, ok_label: str) -> int:
    """Run every seed through target under ASan; die on first failure."""
    n = 0
    for f in seeds:
        result = run([str(target), str(f)], check=False, env=env)
        if result.returncode != 0:
            die(f"FAILED on {fail_label}: {f}")
        n += 1
    log(f"All {n} {ok_label} seeds passed under ASan.")
    return n


def corpus_test(exec_scripts: bool = False) -> None:
    """ASan gate over every seed (parser corpus or exec scripts)."""
    if exec_scripts:
        ensure_exec_target()
        seeds = sorted(f for f in config.EXEC_CORPUS_DIR.iterdir() if config.is_staged_seed(f))
        gate_seeds(
            config.FUZZ_EXEC_TARGET,
            seeds,
            {"ASAN_OPTIONS": "abort_on_error=1:symbolize=0"},
            "exec seed",
            "exec",
        )
        return
    ensure_asan_target()
    seeds = sorted(f for f in config.CORPUS_DIR.iterdir() if config.is_staged_seed(f))
    gate_seeds(config.FUZZ_TARGET, seeds, {"ASAN_OPTIONS": "detect_leaks=0"}, "seed", "fuzz")
