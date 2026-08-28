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
    check-vet        parse + type check with comprehensive vet
    vet              vet + shadowing + strict-style (fast check)
    vet-all          vet via build+test (LLVM)
    vet-shadowing    check variable shadowing only
    vet-unused       check unused variables/imports only
    vet-style        check style (trailing commas, semicolons)
    vet-cast         check redundant casts/transmutes only

  PERF / FUZZ
    perf             run timing baseline (release)
    fuzz-corpus      regenerate the fuzz seed corpus
    fuzz-build       build the ASan fuzz target
    fuzz-test        run every fuzz seed under ASan
    fuzz-run         launch an AFL++ campaign
EOF
