#!/usr/bin/env bash
# Print the available make targets (kept in sync with the Makefile).
cat <<'EOF'
MagniDB targets

  BUILD
    build            build debug binary
    release          build release binary (aggressive opt)
    run              build debug and run
    clean            remove build directory
    rebuild          clean and build

  TEST
    test             run all tests (debug)
    test-verbose     run all tests with verbose output
    test-single <n>  run one test, e.g. make test-single test_integration_vacuum
    test-cli         basic CLI smoke checks
    test-cli-full    comprehensive CLI integration tests

  CHECK / VET
    check            parse + type check (no vet)
    vet              vet + shadowing + strict-style (fast check)
    vet <flag>       single-flag vet: shadowing | unused | style | cast
    vet-all          vet via build+test (LLVM)

  PERF / FUZZ
    perf             run timing baseline (release)
    fuzz <cmd>       corpus|build|cov|test|run|campaign|status|stop
                     e.g. make fuzz campaign  (1h/4 workers; FUZZ_SECONDS, FUZZ_WORKERS)
                          make fuzz status
                          make fuzz stop
EOF
