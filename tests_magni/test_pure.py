"""Unit tests for magni package pure logic (no Odin/AFL++ needed)."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from magni import config
from magni.corpus import md5, parse_afl_metadata, stage_corpus_into
from magni.fuzz import build_worker_cmd
from magni.roles import ROLES, TARGETS, Role, check_roles, default_roles, role_names
from magni.util import clean_extra


class TestCleanExtra(unittest.TestCase):
    def test_none_and_empty(self):
        self.assertEqual(clean_extra(None), [])
        self.assertEqual(clean_extra([]), [])

    def test_strips_leading_dashdash(self):
        self.assertEqual(clean_extra(["--", "-V", "60"]), ["-V", "60"])

    def test_strips_make_key_value_leaks(self):
        self.assertEqual(clean_extra(["-V", "60", "FOO=bar"]), ["-V", "60"])

    def test_double_dash_without_leading_separator_kept(self):
        # Only a LEADING -- is the argparse artifact; inner ones pass through.
        self.assertEqual(clean_extra(["-V", "--", "60"]), ["-V", "--", "60"])


class TestAflMetadata(unittest.TestCase):
    def test_orig_and_cov(self):
        self.assertEqual(
            parse_afl_metadata("id:000000,orig:hello,+cov,op:havoc"),
            "hello,+cov")

    def test_sync(self):
        self.assertEqual(
            parse_afl_metadata("id:000001,sync:master,src:000000"),
            "sync:master")

    def test_no_metadata_returns_filename(self):
        self.assertEqual(parse_afl_metadata("id:000002"), "id:000002")


class TestMd5(unittest.TestCase):
    def test_known(self):
        import hashlib
        self.assertEqual(md5(b"SELECT 1;"), hashlib.md5(b"SELECT 1;").hexdigest())


class TestRoles(unittest.TestCase):
    def test_all_thirteen_present(self):
        self.assertEqual(set(ROLES), {
            "master", "explore", "fast", "coe", "seek", "cmplog", "asan",
            "laf", "mopt", "oldq", "exec", "grammar", "exec_grammar"})

    def test_default_rotation(self):
        self.assertEqual(default_roles(1), ["master"])
        self.assertEqual(default_roles(4),
                         ["master", "cmplog", "asan", "fast"])
        # Rotation wraps.
        self.assertEqual(default_roles(9)[1:],
                         ["cmplog", "asan", "fast", "explore", "coe", "laf",
                          "mopt", "cmplog"])

    def test_role_names_single_source(self):
        for name in ROLES:
            self.assertIn(name, role_names())

    def test_check_roles_rejects_unknown(self):
        with self.assertRaises(SystemExit):
            check_roles(["master", "bogus"])

    def test_exec_roles_use_exec_corpus_and_timeout(self):
        for r in ("exec", "exec_grammar"):
            self.assertEqual(ROLES[r].corpus, config.EXEC_CORPUS_DIR)
            self.assertEqual(ROLES[r].timeout_ms, config.EXEC_TIMEOUT_MS)
        self.assertEqual(ROLES["master"].corpus, config.CORPUS_DIR)
        self.assertEqual(ROLES["master"].timeout_ms, "1000")

    def test_targets_resolve_to_files_or_known_paths(self):
        self.assertEqual(set(TARGETS), {"cov", "asan", "laf", "exec"})
        for role in ROLES.values():
            self.assertIn(role.target(), set(TARGETS.values()))

    def test_grammar_env_injection(self):
        env = ROLES["grammar"].worker_env()
        self.assertEqual(env.get("AFL_PYTHON_MODULE"), "grammar_mutator")
        self.assertIn(str(config.FUZZ_DIR), env.get("PYTHONPATH", ""))
        self.assertNotIn("AFL_PYTHON_MODULE", ROLES["master"].worker_env())

    def test_role_is_immutable(self):
        with self.assertRaises(Exception):
            ROLES["master"].timeout_ms = "9999"  # type: ignore[misc]


class TestWorkerCmd(unittest.TestCase):
    def test_master_cmd(self):
        cmd = build_worker_cmd("master", "master", Path("/out"), 60, [],
                               is_master=True)
        self.assertEqual(cmd[:9],
                         ["afl-fuzz", "-i", str(config.CORPUS_DIR),
                          "-o", "/out", "-x", str(config.SQL_DICT),
                          "-t", "1000"])
        self.assertIn("-M", cmd)
        self.assertIn("master", cmd)
        self.assertIn("-p", cmd)
        self.assertIn("exploit", cmd)
        self.assertEqual(cmd[-2:], [str(config.FUZZ_TARGET_COV), "@@"])

    def test_secondary_uses_dash_S(self):
        cmd = build_worker_cmd("s1", "fast", Path("/out"), 30, [])
        self.assertIn("-S", cmd)
        self.assertNotIn("-M", cmd)

    def test_cmplog_bin_expands(self):
        cmd = build_worker_cmd("s2", "cmplog", Path("/out"), 30, [])
        self.assertNotIn("cmplog_bin", cmd)
        self.assertIn(str(config.FUZZ_TARGET_CMPLOG), cmd)

    def test_exec_role_timeout_and_corpus(self):
        cmd = build_worker_cmd("s3", "exec", Path("/out"), 60, [])
        self.assertIn("-t", cmd)
        self.assertEqual(cmd[cmd.index("-t") + 1], config.EXEC_TIMEOUT_MS)
        self.assertIn(str(config.EXEC_CORPUS_DIR), cmd)
        self.assertEqual(cmd[-2:], [str(config.FUZZ_EXEC_TARGET), "@@"])

    def test_staged_corpus_override(self):
        cmd = build_worker_cmd("master", "master", Path("/out"), 60, [],
                               corpus="/staged", is_master=True)
        self.assertIn("/staged", cmd)
        self.assertNotIn(str(config.CORPUS_DIR), cmd)

    def test_extra_args_appended_before_target(self):
        cmd = build_worker_cmd("master", "master", Path("/out"), 60,
                               ["-V", "120"], is_master=True)
        self.assertLess(cmd.index("-V"), cmd.index(str(config.FUZZ_TARGET_COV)))

    def test_unknown_role_raises(self):
        with self.assertRaises(KeyError):
            build_worker_cmd("s9", "bogus", Path("/out"), 10, [])


class TestStaging(unittest.TestCase):
    def test_filters_py_and_pycache(self):
        import tempfile
        with tempfile.TemporaryDirectory() as src_s, tempfile.TemporaryDirectory() as d:
            src = Path(src_s)
            (src / "seed1").write_text("SELECT 1;")
            (src / "gen_corpus.py").write_text("x")
            (src / "__pycache__").mkdir()
            dst = Path(d) / "out"
            n = stage_corpus_into(src, dst)
            self.assertEqual(n, 1)
            self.assertEqual([f.name for f in dst.iterdir()], ["seed1"])

    def test_is_staged_seed(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            (p / "a").write_text("x")
            (p / "b.py").write_text("x")
            self.assertTrue(config.is_staged_seed(p / "a"))
            self.assertFalse(config.is_staged_seed(p / "b.py"))
            self.assertFalse(config.is_staged_seed(p / "missing"))


class TestRoleDataclass(unittest.TestCase):
    def test_defaults(self):
        r = Role("cov")
        self.assertEqual(r.corpus, config.CORPUS_DIR)
        self.assertEqual(r.timeout_ms, "1000")
        self.assertFalse(r.python_mutator)


if __name__ == "__main__":
    unittest.main()
