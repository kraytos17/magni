"""Unit tests for magni.build freshness logic (no Odin/AFL++ needed)."""

import os
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from magni import build as build_mod
from magni.build import is_fresh, newest_mtime_under


class TestNewestMtime(unittest.TestCase):
    def test_max_over_mixed_roots(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "a.odin").write_text("x")
            (root / "b.py").write_text("x")  # non-Odin ignored in dirs
            (root / "harness.odin").write_text("x")
            old = time.time() - 100
            os.utime(root / "a.odin", (old, old))
            m = newest_mtime_under([root, root / "harness.odin"])
            self.assertGreater(m, old)
            self.assertLessEqual(m, time.time())

    def test_empty_and_missing(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            self.assertEqual(newest_mtime_under([root]), 0.0)
            self.assertEqual(newest_mtime_under([root / "nope"]), 0.0)


class TestIsFresh(unittest.TestCase):
    def setUp(self):
        import tempfile

        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.src = self.root / "src"
        self.src.mkdir()
        self.src_file = self.src / "a.odin"
        self.src_file.write_text("x")
        self.target = self.root / "tgt"
        self.target.write_text("x")
        self.target.chmod(0o755)
        self._old_watch = dict(build_mod.HARNESS_WATCH)
        build_mod.HARNESS_WATCH["test-harness"] = [str(self.src)]
        build_mod._mtime_cache.pop("test-harness", None)
        self._old_force = build_mod.FORCE_REBUILD
        build_mod.FORCE_REBUILD = False
        self._old_env = os.environ.pop("MAGNI_REBUILD", None)

    def tearDown(self):
        build_mod.HARNESS_WATCH.clear()
        build_mod.HARNESS_WATCH.update(self._old_watch)
        build_mod._mtime_cache.pop("test-harness", None)
        build_mod.FORCE_REBUILD = self._old_force
        if self._old_env is not None:
            os.environ["MAGNI_REBUILD"] = self._old_env
        self._tmp.cleanup()

    def _touch(self, p: Path, dt: float):
        t = time.time() + dt
        os.utime(p, (t, t))
        build_mod._mtime_cache.pop("test-harness", None)

    def test_fresh_when_target_newer(self):
        self._touch(self.src_file, -100)
        self._touch(self.target, 0)
        self.assertTrue(is_fresh(self.target, "test-harness"))

    def test_stale_when_source_newer(self):
        self._touch(self.target, -100)
        self._touch(self.src_file, 0)
        self.assertFalse(is_fresh(self.target, "test-harness"))

    def test_missing_target(self):
        self.assertFalse(is_fresh(self.root / "nope", "test-harness"))

    def test_force_flag(self):
        self._touch(self.src_file, -100)
        self._touch(self.target, 0)
        build_mod.FORCE_REBUILD = True
        self.assertFalse(is_fresh(self.target, "test-harness"))

    def test_env_override(self):
        self._touch(self.src_file, -100)
        self._touch(self.target, 0)
        os.environ["MAGNI_REBUILD"] = "1"
        self.assertFalse(is_fresh(self.target, "test-harness"))

    def test_apply_rebuild_flag(self):
        import argparse

        build_mod.apply_rebuild_flag(argparse.Namespace(rebuild=True))
        self.assertTrue(build_mod.FORCE_REBUILD)
        build_mod.FORCE_REBUILD = False
        build_mod.apply_rebuild_flag(argparse.Namespace(rebuild=False))
        self.assertFalse(build_mod.FORCE_REBUILD)


if __name__ == "__main__":
    unittest.main()
