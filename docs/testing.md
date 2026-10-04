# Testing

How to run the gates, where tests live, and the conventions every test
must uphold. Test counts below are approximate; the commands print exact
numbers.

## Gates

| Command | What | When |
|---|---|---|
| `make vet` / `python3 magni.py vet` | Fast vet (`odin check`, no LLVM) | Every change |
| `make vet-all` | Full vet via build+test (`-vet -vet-shadowing -warnings-as-errors -strict-style`) | Before commit |
| `make test` | All Odin tests, vector engine on (501 tests) | Every change |
| `MAGNI_VECTOR=0 make test` | Same suite, scalar path (differential control) | Every behavior change |
| `make test-cli` / `make test-cli-full` | CLI smoke (6) / full black-box (88) | CLI/executor changes |
| `make test-py` | `tests_magni/` unit tests (130+) incl. golden help-text surface | Orchestrator/doc changes |
| `make fuzz-test` / `make fuzz-exec-test` | Every parser/exec corpus seed under ASan | Parser/executor/storage changes |
| `make census` | Per-query allocation census (counts/bytes, not timing) | Must stay deterministic across runs |
| `make perf` | Timing baseline (release) | Perf claims only, on a quiet cool box, on-disk (`/tmp` is tmpfs and flatters I/O) |
| `make ci` | `vet-all` + `test` + `test-py` + both ASan corpora | Pre-commit gate |

`make quick` (`check` + `test` + `test-py`) and `make smoke` (`test` + `test-cli`)
are the short loops. Commits only happen on explicit request.

## Layout

- `tests/*_test.odin` — Odin suites, run in one `odin test` process with
  `ODIN_TEST_THREADS=1`. Filter: `python3 magni.py test --name <test_name>`.
- `tests_magni/` — Python suites: `test_cli_smoke.py`, `test_cli_full.py`
  (subprocess harness `clirunner.py`, per-test timeouts), `test_cli_surface.py`
  (golden help texts), `test_build.py`, `test_pure.py`, `test_seedgen.py`.
- `tests/{perf,census,perf_dense}/` — standalone programs (`make perf`,
  `make census`), not part of `make test`.
- `fuzz/corpus/`, `fuzz/corpus_exec/` — generated seed pools (gitignored);
  generators live in `fuzz/generators/` and are the source of truth —
  never hand-edit a seed file, regenerate instead.

## Conventions (load-bearing)

- **Teardown contracts.** Every helper that opens files owns their removal:
  `setup_db`/`teardown_db`, `setup_tree`/`setup_text_tree`/`teardown_tree`,
  `setup_executor_env`/`teardown_executor_env`, pager `create_*`/`destroy_*`
  pairs. Filenames crossing a `free_all(temp)` boundary must be heap-owned
  (`strings.clone` + `delete` in teardown) — temp strings dangle and
  teardown silently leaves `*.db` files behind.
- **Temp discipline.** `context.temp_allocator` is statement/test-scoped;
  `free_all` it between statements. Anything borrowed across a free (parser
  arena strings, filenames) must be cloned or consumed synchronously.
- **Leak-clean.** The runner tracks allocations; `+++ leak` WARNs fail
  nothing but must stay at zero — a new WARN is a regression.
- **Expected errors stay quiet.** Negative tests wrap log output with
  `suppress_expected_errors()`; raw `log.errorf` output pollutes (and can
  fail) test runs.
- **Goldens.** `magni.py` help text is pinned in `tests_magni/golden/`;
  regenerate intentionally with
  `python3 tests_magni/test_cli_surface.py --regenerate` and review the diff.
- **Corpora.** New syntax needs new seeds (parser + exec pools), regenerated
  via `make fuzz-corpus` / `make fuzz-exec-corpus`, gated by the ASan runs.
  Promoted fuzzer finds go through `corpus promote` (`--exec` for scripts).
- **Determinism.** `make census` must print identical numbers run to run;
  `LIMIT` without `ORDER BY` on routed queries is rowid-ordered by
  construction (candidates arrive sorted), so snapshot it freely.
