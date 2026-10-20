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

    def test_regenerate_writes_entries_and_extras(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            corpus = Path(d)
            (corpus / "orphan").write_text("x")
            n = seedgen.regenerate(
                str(corpus),
                [("a", "SELECT 1;")],
                [("guard", "SELECT 2;")],
                label="seeds",
            )
            self.assertEqual(n, 2)
            self.assertEqual((corpus / "a").read_bytes(), b"SELECT 1;")
            self.assertEqual((corpus / "guard").read_bytes(), b"SELECT 2;")
            # "orphan" is not a managed name, so it survives (generators only
            # clear names they own).
            self.assertTrue((corpus / "orphan").exists())

    def test_regenerate_overwrites_owned_names(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            corpus = Path(d)
            (corpus / "a").write_text("junk")
            seedgen.regenerate(str(corpus), [("a", "SELECT 1;")])
            self.assertEqual((corpus / "a").read_bytes(), b"SELECT 1;")

    def test_managed_names(self):
        self.assertEqual(seedgen.managed_names([("x", "a"), ("y", "b")]), {"x", "y"})


class TestParseExecTables(unittest.TestCase):
    def test_reads_real_generator(self):
        seeds = parse_exec_tables()
        names = {name for name, _ in seeds.values()}
        self.assertIn("script_dml", names)
        self.assertIn("script_ddl", names)
        for h, (_name, content) in seeds.items():
            self.assertIsInstance(content, bytes)
            self.assertTrue(len(h) == 32)


class TestAppendToPromoted(unittest.TestCase):
    def _promoted_file(self, d: Path) -> Path:
        # Multi-line list like the real generators/promoted_exec_seeds.py.
        p = d / "promoted_seeds.py"
        p.write_text('"""hdr"""\n\nEXEC_PROMOTED = [\n]\n', encoding="utf-8")
        return p

    def test_append_empty_then_increment(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            p = self._promoted_file(Path(d))
            append_to_promoted([(b"SELECT 1;", "orig:foo")], set(), p)
            text = p.read_text(encoding="utf-8")
            self.assertIn("(\"promoted_0001\", b'SELECT 1;'),  # orig:foo", text)
            append_to_promoted([(b"SELECT 2;", "")], {"promoted_0001"}, p)
            text = p.read_text(encoding="utf-8")
            self.assertIn("(\"promoted_0002\", b'SELECT 2;'),", text)

    def test_skips_collisions(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            p = self._promoted_file(Path(d))
            append_to_promoted([(b"X;", "")], {"promoted_0001", "promoted_0002"}, p)
            self.assertIn('"promoted_0003"', p.read_text(encoding="utf-8"))

    def test_result_still_parses(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            p = self._promoted_file(Path(d))
            append_to_promoted([(b"SELECT 'a; b';", "orig:q")], set(), p)
            ns: dict = {}
            exec(compile(p.read_text(encoding="utf-8"), str(p), "exec"), ns)  # noqa: S102 - test round-trips its own fixture file
            self.assertEqual(ns["EXEC_PROMOTED"], [("promoted_0001", b"SELECT 'a; b';")])

    def test_generator_layout(self):
        # Generators live in fuzz/generators/ (never among the seeds they
        # write): hand-seed scripts plus both promoted tables. Both corpus
        # dirs hold only generated seed files.
        # Fuzz binaries build into target/fuzz/ (parser) and
        # target/fuzz-exec/ (exec) — separate dirs so the two builds
        # never clobber each other.
        from magni import config

        self.assertTrue(config.GEN_CORPUS.is_file())
        self.assertTrue(config.GEN_EXEC_CORPUS.is_file())
        self.assertTrue(config.PROMOTED_SEEDS.is_file())
        self.assertTrue(config.EXEC_PROMOTED.is_file())
        self.assertEqual(config.GEN_CORPUS.parent, config.GENERATORS_DIR)
        self.assertEqual(config.PROMOTED_SEEDS.parent, config.GENERATORS_DIR)
        self.assertEqual(config.EXEC_PROMOTED.parent, config.GENERATORS_DIR)
        for corpus_dir in (config.CORPUS_DIR, config.EXEC_CORPUS_DIR):
            pys = sorted(f.name for f in corpus_dir.iterdir() if f.is_file() and f.suffix == ".py")
            self.assertEqual(pys, [])
        for target in (config.FUZZ_TARGET, config.FUZZ_TARGET_COV):
            self.assertEqual(target.parent, config.TARGET_FUZZ)
        self.assertEqual(config.FUZZ_EXEC_TARGET.parent, config.TARGET_FUZZ_EXEC)


class TestRewriteSkipWhenUnchanged(unittest.TestCase):
    def _pool(self, promoted: Path):
        from magni import config

        return config.CorpusPool(
            label="seed",
            seed_tables=("PROMOTED_SEEDS",),
            gen_script=config.GENERATORS_DIR / "gen_corpus.py",
            corpus_dir=promoted.parent,
            promoted_path=promoted,
            cmin_target=config.FUZZ_TARGET_COV,
            cmin_timeout_ms="1000",
            is_exec=False,
        )

    def _promoted_file(self, d: Path) -> Path:
        p = d / "promoted_seeds.py"
        p.write_text(
            '"""hdr"""\n\nPROMOTED_SEEDS = [\n'
            '    ("promoted_0001", b"SELECT 1;"),  # orig:a\n'
            '    ("promoted_0002", b"SELECT 2;"),\n'
            "]\n",
            encoding="utf-8",
        )
        return p

    def test_no_change_leaves_file_untouched(self):
        import tempfile

        from magni.minimize import rewrite_promoted_seeds

        with tempfile.TemporaryDirectory() as d:
            p = self._promoted_file(Path(d))
            before = p.read_bytes()
            rewrite_promoted_seeds(["promoted_0001", "promoted_0002"], 2, 100, self._pool(p))
            self.assertEqual(p.read_bytes(), before)

    def test_dropped_entry_rewrites(self):
        import tempfile

        from magni.minimize import rewrite_promoted_seeds

        with tempfile.TemporaryDirectory() as d:
            p = self._promoted_file(Path(d))
            rewrite_promoted_seeds(["promoted_0001"], 1, 100, self._pool(p))
            text = p.read_text(encoding="utf-8")
            self.assertIn('"promoted_0001"', text)
            self.assertNotIn('"promoted_0002"', text)


if __name__ == "__main__":
    unittest.main()
