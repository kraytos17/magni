"""CLI smoke tests: binary surface basics. Port of tests/cli_smoke.sh.

One behavioral fix vs the shell: the shell's "--eval executes SQL" case
actually exercised --file (mislabeled); it is named correctly here.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tests_magni.clirunner import MagniCLITestCase


class TestCliSmoke(MagniCLITestCase):
    def test_help_succeeds(self):
        p = self.run_raw(["--help"])
        self.assertOk(p)
        self.assertHas(p, "usage")

    def test_h_succeeds(self):
        p = self.run_raw(["-h"])
        self.assertOk(p)
        self.assertHas(p, "usage")

    def test_default_database_path_creates_test_db(self):
        p = self.run_raw(["--eval", "CREATE TABLE t (x INT);"])
        self.assertOk(p)
        self.assertTrue(
            (self.tmp / "test.db").is_file(), "default database path must create test.db in cwd"
        )

    def test_positional_database_path_works(self):
        d = self.db()
        p = self.run_raw(["--eval", "CREATE TABLE t (x INT);", str(d)])
        self.assertOk(p)
        self.assertGreater(d.stat().st_size, 0)

    def test_file_executes_sql(self):
        d = self.db()
        p = self.run_file(
            "CREATE TABLE t (x INT);\nINSERT INTO t VALUES (42);\nSELECT * FROM t;\n", d
        )
        self.assertOk(p)
        self.assertHas(p, "42")

    def test_pipe_mode_reads_sql_from_stdin(self):
        d = self.db()
        p = self.run_stdin(
            "CREATE TABLE t (x INT);\nINSERT INTO t VALUES (99);\nSELECT * FROM t;\n", d
        )
        self.assertOk(p)
        self.assertHas(p, "99")


if __name__ == "__main__":
    unittest.main()
