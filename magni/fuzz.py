"""AFL++ campaign management: run/campaign/status/stop."""

import argparse
import os
import shutil
import subprocess
from pathlib import Path

from . import config
from .build import (
    apply_rebuild_flag,
    ensure_asan_target,
    ensure_cmplog_target,
    ensure_cov_target,
    ensure_exec_target,
    ensure_laf_target,
)
from .cmin import cmd_fuzz_cmin
from .corpus import stage_clean_corpus
from .roles import ROLES, check_roles, default_roles
from .showmap import cmd_fuzz_showmap
from .util import clean_extra, log, require_tool, run


def fuzz_out_dir() -> Path:
    return Path(os.environ.get("MAGNI_FUZZ_OUT", "fuzz/afl-output"))


def cmd_fuzz(args: argparse.Namespace) -> None:
    """Fuzz subcommand dispatcher."""
    apply_rebuild_flag(args)
    if args.fuzz_cmd == "run":
        ensure_cov_target()
        config.CORPUS_DIR.mkdir(exist_ok=True)
        staged = stage_clean_corpus(config.CORPUS_DIR)
        env = dict(config.AFL_BASE_ENV)
        cmd = ["afl-fuzz", "-i", str(staged), "-o", str(fuzz_out_dir()),
               "-x", str(config.SQL_DICT), "-t", "1000", "-m", "none"]
        cmd.extend(clean_extra(args.extra))
        cmd += ["--", str(config.FUZZ_TARGET_COV), "@@"]
        run(cmd, env=env)
    elif args.fuzz_cmd == "campaign":
        ensure_cov_target()
        roles = ([r.strip() for r in args.roles.split(",") if r.strip()]
                 if args.roles else None)
        cmd_fuzz_campaign(args.workers, args.seconds, clean_extra(args.extra),
                          roles=roles)
    elif args.fuzz_cmd == "status":
        cmd_fuzz_status()
    elif args.fuzz_cmd == "stop":
        run(["pkill", "afl-fuzz"], check=False)
        log("stopped afl-fuzz (if running)")
    elif args.fuzz_cmd == "showmap":
        ensure_cov_target()
        cmd_fuzz_showmap()
    elif args.fuzz_cmd == "cmin":
        ensure_cov_target()
        cmd_fuzz_cmin()


def cmd_fuzz_campaign(workers: int, seconds: int, extra: list[str],
                      roles: list[str] | None = None) -> None:
    """Launch a headless parallel AFL++ campaign with per-worker roles."""
    if roles:
        check_roles(roles)
        # First role becomes the -M master. Parser and exec campaigns must
        # use separate output dirs (different binaries/maps) — set
        # MAGNI_FUZZ_OUT=fuzz/afl-exec-output for pure exec runs.
    else:
        roles = default_roles(workers)
    out = fuzz_out_dir()
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)
    # Build every binary the roles need (cached by ensure_* checks).
    kinds = {ROLES[r].binary for r in roles}
    if "cov" in kinds:
        ensure_cov_target()
    if "cmplog" in kinds:
        ensure_cmplog_target()
    if "laf" in kinds:
        ensure_laf_target()
    if "asan" in kinds:
        ensure_asan_target()
    if "exec" in kinds:
        ensure_exec_target()
        if not config.EXEC_CORPUS_DIR.is_dir() or not any(config.EXEC_CORPUS_DIR.iterdir()):
            log(f"Warning: {config.EXEC_CORPUS_DIR} is empty or missing; exec worker "
                f"will exit. Add seed scripts first.")
    # Stage clean inputs (excluding generator .py sources) for every corpus
    # the roles need. Workers share the staged dir; it is rebuilt per campaign.
    staged: dict[str, str] = {}
    for src in {str(ROLES[r].corpus) for r in roles}:
        staged[src] = str(stage_clean_corpus(Path(src)))
        log(f"staged {src} -> {staged[src]}")
    env = dict(config.AFL_BASE_ENV, AFL_NO_UI="1")
    procs: list[subprocess.Popen] = []
    try:
        log(f"Starting {len(roles)} AFL++ workers ({','.join(roles)}) (output: {out})...")
        # First role is the -M master, whatever its binary: parser and exec
        # campaigns must not share an output dir (different binaries/maps).
        procs.append(_spawn_worker("master", roles[0], out, seconds, extra, env,
                                   staged, is_master=True))
        for i, role in enumerate(roles[1:], start=1):
            procs.append(_spawn_worker(f"s{i}", role, out, seconds, extra, env, staged))
        for p in procs:
            p.wait()
    except KeyboardInterrupt:
        log("Interrupted, killing workers...")
    finally:
        for p in procs:
            if p.poll() is None:
                p.terminate()
    log(f"campaign: {len(roles)} workers ({','.join(roles)}), {seconds}s, out={out}")


def build_worker_cmd(name: str, role: str, out: Path, seconds: int,
                     extra: list[str], corpus: str | Path | None = None,
                     is_master: bool = False) -> list[str]:
    """Assemble the afl-fuzz argv for one worker (pure; no side effects)."""
    r = ROLES[role]
    mode_flag = "-M" if is_master else "-S"
    cmd = ["afl-fuzz", "-i", str(corpus if corpus is not None else r.corpus),
           "-o", str(out), "-x", str(config.SQL_DICT), "-t", r.timeout_ms,
           "-m", "none", mode_flag, name, "-V", str(seconds)]
    flags = [str(config.FUZZ_TARGET_CMPLOG) if f == "cmplog_bin" else f
             for f in (*r.afl_flags, *r.sched)]
    cmd.extend(flags)
    if extra:
        cmd.extend(extra)
    cmd += ["--", str(r.target()), "@@"]
    return cmd


def _spawn_worker(name: str, role: str, out: Path, seconds: int,
                  extra: list[str], env: dict, staged: dict[str, str],
                  is_master: bool = False) -> subprocess.Popen:
    r = ROLES[role]
    src = str(r.corpus)
    cmd = build_worker_cmd(name, role, out, seconds, extra,
                           corpus=staged.get(src, src), is_master=is_master)
    e = os.environ.copy()
    e.update(env)
    e.update(r.worker_env())
    log(f"  $ [{role}] {' '.join(cmd)}")
    return subprocess.Popen(cmd, cwd=str(config.ROOT), env=e)


def cmd_fuzz_status() -> None:
    """Print per-worker progress of the running AFL++ campaign."""
    require_tool("rg")
    found = False
    for stats in sorted(fuzz_out_dir().glob("*/fuzzer_stats")):
        found = True
        log(f"== {stats.parent.name} ==")
        subprocess.run(["rg", ("execs_done|execs_per_sec|corpus_count|saved_crashes|"
                        "saved_hangs|stability|cycles_done"), str(stats)],
                       cwd=str(config.ROOT))
    if not found:
        log(f"No fuzzer_stats found in {fuzz_out_dir()}/")
