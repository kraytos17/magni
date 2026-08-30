#!/usr/bin/env bash
# Print available make targets (kept in sync with Makefile).
# Generated via `make help` fallback awk, but kept explicit here for `bash scripts/help.sh`.
cat <<'EOF'
MagniDB — make help

  BUILD
    make build            build debug binary → build/magni
    make release          build release binary (LTO, -lto:thin) → build/magni_release
    make run              build debug and run
    make rebuild          clean + build
    make clean            remove build/ only
    make clean-all        remove build/ + fuzz artifacts

  TEST
    make test             run all Odin tests (debug, 356 tests)
    make test-verbose     verbose (no fancy)
    make test-single <name>  one test: make test-single test_integration_vacuum
    make test-cli         basic CLI smoke (needs build/magni)
    make test-cli-full    comprehensive CLI integration
    make smoke            test + test-cli (quick gate)
    make quick            check + test (fastest)
    make ci               vet-all + test + fuzz-test (CI)

  CHECK / VET
    make check            odin check src (no vet)
    make vet              fast vet (odin check -vet -vet-shadowing)
    make vet shadowing|unused|style|cast   single-flag vet
    make vet-all          LLVM vet via build+test (strict, shadowing)

  PERF
    make perf             timing baseline (release flags)
    make bench            alias for perf

  FUZZ — dispatcher (legacy, still supported)
    make fuzz corpus      regenerate corpus (python3 fuzz/corpus/gen_corpus.py)
    make fuzz build       ASan target → fuzz/fuzz_target
    make fuzz cov         coverage target → fuzz/fuzz_target_cov
    make fuzz test        ASan gate (every corpus seed)
    make fuzz run         interactive AFL++ (pass ARGS=" -V 60")
    make fuzz campaign    parallel headless  (FUZZ_WORKERS=4 FUZZ_SECONDS=3600)
    make fuzz status      per-worker fuzzer_stats
    make fuzz stop        pkill afl-fuzz

  FUZZ — streamlined (preferred, tab-completable, FUZZ_* vars)
    make fuzz-corpus      alias: make corpus
    make corpus           regenerate 1318 seeds → fuzz/corpus/
    make fuzz-build       ASan target
    make fuzz-cov         coverage target (alias fuzz-cov-build)
    make fuzz-test        ASan gate
    make fuzz-run ARGS="-V 60"              interactive single worker
    make fuzz-campaign FUZZ_SECONDS=60 FUZZ_WORKERS=2
    make fuzz-status      show execs, corpus, crashes, hangs, stability
    make fuzz-stop        stop running campaign
    make fuzz-clean       rm -rf fuzz/afl-output fuzz/build __pycache__
    make fuzz-showmap     AFL_MAP_SIZE=7181 map size for current corpus (≈7496)
    make fuzz-cmin        minimize corpus → /tmp/min (afl-cmin)
    make fuzz-one FILE=path  repro single crash under ASan
    make fuzz-help        show fuzz help

  Variables (override on CLI or env):
    FUZZ_WORKERS=4        workers for fuzz-campaign (N <= cores-1)
    FUZZ_SECONDS=3600     duration for campaign (-V)
    FUZZ_OUT=fuzz/afl-output  or MAGNI_FUZZ_OUT=<dir> (env wins)
    ODIN=odin             odin binary
    ARGS=" -V 60"         extra afl-fuzz args for fuzz-run/campaign

  Examples
    make test && make fuzz-test          # fast local gate
    make ci                              # full vet + test + fuzz-test
    make fuzz-corpus && make fuzz-cmin   # regenerate + check minimal (1157)
    make fuzz-campaign FUZZ_SECONDS=60   # 1-min smoke, 4 workers
    make fuzz-status                     # live progress
    make fuzz-one FILE=fuzz/corpus/empty # repro
    make clean-all && make release       # fresh release with LTO
EOF
