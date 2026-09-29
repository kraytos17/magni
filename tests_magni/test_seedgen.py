"""Unit tests for seedgen + exec promotion (no Odin/AFL++ needed)."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "fuzz"))

import seedgen
from magni.promote_exec import append_to_promoted, parse_exec_tables


class TestSeedgen(unittest.TestCase):
    def test_write_and_clear_roundtrip(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            corpus = Path(d)
            entries = [("a", "SELECT 1;"), ("b", b"SELECT \xff;")]
            self.assertEqual(seedgen.write_entries(corpus, entries), 2)
            self.assertEqual((corpus / "a").read_bytes(), b"SELECT 1;")
            self.assertEqual((corpus / "b").read_bytes(), b"SELECT \xff;")
            (corpus / "stale").write_text("x")
            seedgen.clear_managed(corpus, {"a", "b"})
            self.assertFalse((corpus / "a").exists())
            self.assertFalse((corpus / "b").exists())
            self.assertTrue((corpus / "stale").exists())

    def test_managed_names(self):
        self.assertEqual(seedgen.managed_names([("x", "a"), ("y", "b")]), {"x", "y"})


class TestParseExecTables(unittest.TestCase):
    def test_reads_real_generator(self):
        seeds = parse_exec_tables()
        names = {name for name, _ in seeds.values()}
        self.assertIn("script_dml", names)
        self.assertIn("script_ddl", names)
        for h, (name, content) in seeds.items():
            self.assertIsInstance(content, bytes)
            self.assertTrue(len(h) == 32)


class TestAppendToPromoted(unittest.TestCase):
    def _promoted_file(self, d: Path) -> Path:
        # Multi-line list like the real corpus_exec/promoted_seeds.py.
        p = d / "promoted_seeds.py"
        p.write_text('"""hdr"""\n\nEXEC_PROMOTED = [\n]\n', encoding="utf-8")
        return p

    def test_append_empty_then_increment(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = self._promoted_file(Path(d))
            append_to_promoted([(b"SELECT 1;", "orig:foo")], set(), p)
            text = p.read_text(encoding="utf-8")
            self.assertIn('("promoted_0001", b\'SELECT 1;\'),  # orig:foo', text)
            append_to_promoted([(b"SELECT 2;", "")], {"promoted_0001"}, p)
            text = p.read_text(encoding="utf-8")
            self.assertIn('("promoted_0002", b\'SELECT 2;\'),', text)

    def test_skips_collisions(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = self._promoted_file(Path(d))
            append_to_promoted([(b"X;", "")],
                               {"promoted_0001", "promoted_0002"}, p)
            self.assertIn('"promoted_0003"', p.read_text(encoding="utf-8"))

    def test_result_still_parses(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = self._promoted_file(Path(d))
            append_to_promoted([(b"SELECT 'a; b';", "orig:q")], set(), p)
            ns: dict = {}
            exec(compile(p.read_text(encoding="utf-8"), str(p), "exec"), ns)
            self.assertEqual(ns["EXEC_PROMOTED"],
                             [("promoted_0001", b"SELECT 'a; b';")])


if __name__ == "__main__":
    unittest.main()
