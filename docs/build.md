# Build System

All binaries land under `target/` (Rust-style; gitignored):

| Path | Profile | Produces |
|---|---|---|
| `target/debug/magni` | `build` (debug, default) | REPL/CLI binary |
| `target/release/magni_release` | `build --release` (aggressive, LTO, no checks) | Benchmarks, perf A/B |
| `target/fuzz/fuzz_target` | `build --asan` | Parser ASan gate |
| `target/fuzz/fuzz_target_cov` | `build --cov` (+ `--cmplog` / `--laf` variants) | Campaigns, `showmap` |
| `target/fuzz/fuzz_exec_target` | `build --exec` (coverage + ASan) | Exec ASan gate + campaigns |

`magni.py` owns every invocation and flag (`magni/config.py` is the single
source of truth — `DEBUG_FLAGS`/`TEST_FLAGS` live there, not in the
Makefile). `make` is a thin wrapper: `build`, `release`, `test`,
`test-single NAME=`, `vet`/`vet-all`, `perf`, `census`, `test-cli[-full]`,
`test-py`, `fuzz-*`, `clean` (removes `target/debug` + `target/release`;
`--all`/`--fuzz-only` additionally clear `target/fuzz` and AFL outputs).

## Fuzz targets and corpora

- `make fuzz-build` / `fuzz-cov` / `fuzz-exec-build`, then
  `make fuzz-test` (parser seeds) / `make fuzz-exec-test` (exec scripts).
  Targets auto-rebuild when stale.
- Campaigns: `make fuzz-campaign ROLES=... FUZZ_WORKERS=N FUZZ_SECONDS=S`
  (default: master + rotation). Parser and exec runs must use separate
  output dirs (`MAGNI_FUZZ_OUT=fuzz/afl-exec-output` for exec).
  `fuzz-status` / `fuzz-stop` monitor and kill.
- Seeds: `fuzz/generators/` write `fuzz/corpus/` + `fuzz/corpus_exec/`
  deterministically (`make fuzz-corpus`, `make fuzz-exec-corpus`); grown
  finds merge via `corpus promote` (`--exec` for scripts). See
  [fuzz/README.md](../fuzz/README.md) for the full workflow, roles, and
  the SQL-aware mutator.

## Toolchain

Odin nightly + AFL++ 5.00c + Clang/LLVM 23 (see `fuzz/toolchain-version.txt`;
`ODIN` env override selects the compiler). The distro AFL++ LLVM plugins
were built against an older LLVM: `cmplog`/`laf` roles stay broken until
AFL++ is rebuilt from source. `MAGNI_FUZZ_OUT` overrides the campaign
output dir; `MAGNI_REBUILD=1` forces rebuilds.
