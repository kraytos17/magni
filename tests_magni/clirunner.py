"""Shared black-box harness for the magni CLI binary.

Replaces tests/cli_smoke.sh and tests/cli_test.sh. Unlike the shell
versions, every run enforces a wall-clock timeout (a hung binary becomes a
failure, not hung CI), asserts real exit codes (the shell discarded them
with `|| true`), and keeps stdout/stderr SEPARATE so the stdout-vs-stderr
output contract is assertable (the shell merged them with 2>&1).

Case-insensitive fixed-string matching mirrors the shell `has` helper;
`assertNoErr`/`assertIsErr` mirror `no_err`/`is_err`.
"""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from magni import config

BINARY = config.TARGET_DEBUG / "magni"

# A hung binary fails instead of hanging CI. Normal cases finish in <2s;
# bulk-load cases stay far below this. (The .snapshots shared-lock hang
# would previously wedge the whole suite with no diagnostic.)
DEFAULT_TIMEOUT = 60


class MagniCLITestCase(unittest.TestCase):
    """One fresh tmpdir per test; helpers spawn the binary as a subprocess."""

    @classmethod
    def setUpClass(cls):
        if not BINARY.is_file():
            raise unittest.SkipTest(f"{BINARY} missing; build first: python3 magni.py build")

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="magni_cli_")
        self.tmp = Path(self._tmp.name)
        self._db_seq = 0

    def tearDown(self):
        self._tmp.cleanup()

    # -- process helpers -------------------------------------------------
    def db(self) -> Path:
        """Fresh (empty, existing) database path, like shell `db()`."""
        self._db_seq += 1
        p = self.tmp / f"db_{self._db_seq}.db"
        p.touch()
        return p

    def run_raw(
        self,
        args: list[str],
        stdin_text: str | None = None,
        cwd: Path | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(
                [str(BINARY), *args],
                input=stdin_text,
                capture_output=True,
                text=True,
                cwd=str(cwd or self.tmp),
                timeout=timeout,
                check=False,
            )  # exit codes asserted explicitly via assertOk
        except subprocess.TimeoutExpired as e:
            self.fail(
                f"timed out after {timeout}s (hung binary?) "
                f"cmd={args} stdout={e.stdout!r} stderr={e.stderr!r}"
            )

    def run_eval(self, sql: str, db: Path | None = None, **kw) -> subprocess.CompletedProcess:
        return self.run_raw(["--eval", sql, str(db or self.db())], **kw)

    def run_file(self, sql: str, db: Path | None = None, **kw) -> subprocess.CompletedProcess:
        f = self.tmp / "script.sql"
        f.write_text(sql)
        return self.run_raw(["--file", str(f), str(db or self.db())], **kw)

    def run_stdin(self, sql: str, db: Path | None = None, **kw) -> subprocess.CompletedProcess:
        return self.run_raw([str(db or self.db())], stdin_text=sql, **kw)

    # -- output assertions (shell parity + stronger) ---------------------
    @staticmethod
    def combined(proc: subprocess.CompletedProcess) -> str:
        return (proc.stdout or "") + "\n" + (proc.stderr or "")

    def assertHas(self, proc: subprocess.CompletedProcess, needle: str):
        self.assertIn(
            needle.lower(),
            self.combined(proc).lower(),
            f"missing {needle!r} in output:\n{self.combined(proc)}",
        )

    def assertHasStdout(self, proc: subprocess.CompletedProcess, needle: str):
        """Result contract: user-facing results live on stdout, not stderr."""
        self.assertIn(
            needle.lower(),
            (proc.stdout or "").lower(),
            f"missing {needle!r} on stdout:\nstdout={proc.stdout!r}\nstderr={proc.stderr!r}",
        )

    def assertNoErr(self, proc: subprocess.CompletedProcess):
        out = self.combined(proc)
        self.assertNotRegex(out, r"(?i)error", f"unexpected error in output:\n{out}")

    def assertIsErr(self, proc: subprocess.CompletedProcess):
        out = self.combined(proc)
        self.assertRegex(out, r"(?i)error", f"expected an error in output:\n{out}")

    def assertOk(self, proc: subprocess.CompletedProcess):
        self.assertEqual(
            proc.returncode, 0, f"nonzero exit {proc.returncode}:\n{self.combined(proc)}"
        )
