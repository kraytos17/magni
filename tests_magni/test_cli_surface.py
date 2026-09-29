"""Golden test: magni.py CLI surface must not drift unintentionally.

Compares `magni.py help` and every subcommand `--help` against
tests_magni/golden/. If you intentionally change help text, regenerate with:
    python3 tests_magni/test_cli_surface.py --regenerate
"""

import subprocess
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
GOLDEN = HERE / "golden"
MAGNI = HERE.parent / "magni.py"

CASES = [
    (["help"], "help.txt"),
    (["build", "--help"], "help_build.txt"),
    (["test", "--help"], "help_test.txt"),
    (["test-cli", "--help"], "help_test-cli.txt"),
    (["vet", "--help"], "help_vet.txt"),
    (["fuzz", "--help"], "help_fuzz.txt"),
    (["corpus", "--help"], "help_corpus.txt"),
    (["clean", "--help"], "help_clean.txt"),
    (["help", "--help"], "help_help.txt"),
    (["fuzz", "run", "--help"], "help_fuzz_run.txt"),
    (["fuzz", "campaign", "--help"], "help_fuzz_campaign.txt"),
    (["fuzz", "status", "--help"], "help_fuzz_status.txt"),
    (["fuzz", "stop", "--help"], "help_fuzz_stop.txt"),
    (["fuzz", "showmap", "--help"], "help_fuzz_showmap.txt"),
    (["fuzz", "cmin", "--help"], "help_fuzz_cmin.txt"),
    (["corpus", "generate", "--help"], "help_corpus_generate.txt"),
    (["corpus", "test", "--help"], "help_corpus_test.txt"),
    (["corpus", "promote", "--help"], "help_corpus_promote.txt"),
    (["corpus", "minimize", "--help"], "help_corpus_minimize.txt"),
]

# `test-py` is new in the refactor (no pre-refactor golden exists).
NEW_CASES = [
    (["test-py", "--help"], "help_test-py.txt"),
]


def run_help(argv: list[str]) -> str:
    r = subprocess.run([sys.executable, str(MAGNI), *argv],
                       capture_output=True, text=True, cwd=str(MAGNI.parent))
    return r.stdout


class TestCliSurface(unittest.TestCase):
    def test_golden_surface(self):
        failures = []
        for argv, golden in CASES + NEW_CASES:
            expected = (GOLDEN / golden).read_text() if (GOLDEN / golden).exists() else None
            actual = run_help(argv)
            if expected is None:
                failures.append(f"{golden}: no golden file; actual:\n{actual}")
            elif actual != expected:
                failures.append(
                    f"{golden}: drift detected.\n"
                    f"--- golden ---\n{expected}\n--- actual ---\n{actual}")
        self.assertEqual(failures, [])


if __name__ == "__main__":
    if "--regenerate" in sys.argv:
        sys.argv.remove("--regenerate")
        for argv, golden in CASES + NEW_CASES:
            (GOLDEN / golden).write_text(run_help(argv))
            print(f"wrote {golden}")
    else:
        unittest.main()
