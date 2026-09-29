"""Shared seed-corpus writer for the generator scripts (not a seed itself).

gen_corpus.py and gen_exec_corpus.py both clear managed names and rewrite
every seed deterministically; this module holds that skeleton so the two
generators can't drift. Imported by scripts run as `python3 <path>` (script
dir on sys.path) — never exec-parsed by magni like the generators are.
"""

import os


def managed_names(entries):
    return {name for name, _ in entries}


def clear_managed(corpus_dir, names):
    """Remove previously managed seeds (keeps stale seeds from lingering)."""
    for fn in os.listdir(corpus_dir):
        if fn in names:
            os.remove(os.path.join(corpus_dir, fn))


def write_entries(corpus_dir, entries):
    """Write (name, content) seeds; str is utf-8 encoded, bytes as-is."""
    written = 0
    for name, content in entries:
        data = content.encode("utf-8") if isinstance(content, str) else content
        with open(os.path.join(corpus_dir, name), "wb") as f:
            f.write(data)
        written += 1
    return written
