"""SQL-aware custom mutator for AFL++.

AFL++ loads this via AFL_PYTHON_MODULE=grammar_mutator (with PYTHONPATH
including fuzz/). Complements byte-havoc with mutations that stay
syntactically close to valid SQL, so coverage reaches deeper parser/executor
paths instead of dying in the tokenizer error arm.

Strategies (weighted, one per fuzz() call):
  keyword_swap  (30%) — replace a keyword with a same-class alternative
                        (JOIN variants, set-ops, comparisons, logic ops).
  token_insert  (25%) — insert a random SQL token at a whitespace boundary.
  splice_stmts  (20%) — statement-aware splice of buf + add_buf on ';'
                        boundaries (string-literal aware). Falls back to
                        token_insert when either side has no ';'.
  literal_tweak (15%) — mutate an int/float/hex/string literal in place.
  wrap_struct   (10%) — wrap in a subquery, txn, DDL/index lifecycle, or
                        aggregate-over-join; else append WHERE/UNION/LIMIT.

Also exposes havoc_mutation() so the same strategies stack inside AFL++'s
havoc stage (6% default probability via havoc_mutation_probability()).

Works for both targets: parser (single statement) and exec (multi-statement
scripts) — splice_stmts degrades gracefully to single-statement inputs.

Standalone selftest (no AFL++ needed):
    python3 fuzz/grammar_mutator.py --selftest
"""

import random
import sys

_rng = random.Random()
_last_desc = "init"

MAX_OUT = 1024 * 1024  # safety cap before AFL++ max_size truncation

# Same-class keyword swaps: substitution stays in-grammar, explores a
# neighbouring parse path (join dispatch, set-op dispatch, comparison arms).
SWAP_CLASSES = [
    [b"INNER", b"LEFT", b"RIGHT", b"CROSS", b"FULL"],
    [b"LEFT JOIN", b"LEFT OUTER JOIN", b"RIGHT JOIN", b"RIGHT OUTER JOIN",
     b"INNER JOIN", b"CROSS JOIN", b"JOIN"],
    [b"UNION", b"UNION ALL", b"INTERSECT", b"EXCEPT"],
    [b"=", b"<", b">", b"<=", b">=", b"<>", b"!="],
    [b"AND", b"OR"],
    [b"ASC", b"DESC"],
    [b"COUNT", b"SUM", b"AVG", b"MIN", b"MAX"],
    [b"INT", b"INTEGER", b"TEXT", b"REAL", b"BLOB"],
    [b"COMMIT", b"ROLLBACK"],
    [b"CREATE", b"DROP"],
    [b"SELECT", b"EXPLAIN"],
]

# Tokens worth inserting: the sql.dict entries plus extras the dict can't
# declare (literals, identifiers, multi-word combos). Dict values load from
# fuzz/sql.dict at import so the two can never drift; SWAP_CLASSES stays
# handwritten (semantic classes aren't derivable from a flat dict).
def _load_dict_tokens():
    import os
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql.dict")
    toks = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            _, _, value = line.partition("=")
            value = value.strip().strip('"')
            if value:
                toks.append(value.encode())
    if not toks:
        raise ValueError("no tokens parsed")
    return toks


# Fallback if sql.dict is missing/unreadable: a pinned subset of dict
# values, verbatim (the selftest asserts no drift in the other direction).
# Fuzzing must never break on a helper-file problem.
_DICT_FALLBACK = [
    b"SELECT", b"FROM", b"WHERE", b"JOIN", b"ON", b"USING", b"GROUP BY",
    b"ORDER BY", b"HAVING", b"LIMIT", b"UNION ALL",
    b"AND", b"OR", b"NOT", b"BETWEEN", b"IN", b"LIKE",
    b"IS NULL", b"AS", b"DISTINCT", b"EXPLAIN", b"BEGIN", b"COMMIT",
    b"INDEX",
    b"*", b",", b";", b"(", b")", b"=", b"<>", b">", b"NULL",
]

# Not declarable in sql.dict: multi-word combos, literals, identifiers,
# aggregate snippets. ("NULL" lives in the dict, so it is not repeated here.)
_EXTRA_TOKENS = [
    b"LEFT OUTER JOIN", b"RIGHT JOIN", b"IS NOT NULL",
    b"1", b"0", b"42", b"0xCAFE", b"1.5e3", b"'x'", b"'O''Reilly'",
    b"X'DEADBEEF'",
    b"t", b"users", b"a", b"b", b"x", b"id", b"name", b"sub",
    b"COUNT(*)", b"AS OF SNAPSHOT 1",
]

try:
    DICT_TOKENS = _load_dict_tokens()
except Exception:
    DICT_TOKENS = _DICT_FALLBACK

INSERT_TOKENS = DICT_TOKENS + _EXTRA_TOKENS

WRAP_TEMPLATES = [
    b"SELECT * FROM (%s) AS sub;",
    b"SELECT * FROM (%s) AS s1 JOIN (%s) AS s2 ON s1.x = s2.y;",
    b"%s WHERE x > 1;",
    b"%s UNION ALL %s;",
    b"%s ORDER BY 1 LIMIT 2;",
    b"EXPLAIN %s;",
    b"BEGIN; %s; COMMIT;",
    b"BEGIN; %s; ROLLBACK;",
    b"CREATE TABLE _fz (id INTEGER PRIMARY KEY, v TEXT); %s;",
    b"CREATE INDEX _fzi ON _fz (v); %s; DROP INDEX _fzi;",
    b"SELECT a.id, COUNT(*) FROM (%s) AS a JOIN (%s) AS b ON a.id = b.id GROUP BY a.id;",
]


# AFL++ Python mutator API below (init/fuzz/havoc_mutation/
# havoc_mutation_probability/describe/fuzz_count): fixed names and
# signatures imposed by AFL++; see afl-fuzz docs. All state stays in
# module globals (_rng, _last_desc) — AFL++ loads one module instance.
def init(seed):
    _rng.seed(seed)
    global _last_desc
    _last_desc = f"init(seed={seed})"


def fuzz_count(buf):
    # Mutations per fuzz() call: fixed at 4 (AFL++ multiplies by its own
    # scheduling; this only bounds one custom-mutator application batch).
    return 4


def _split_stmts(data: bytes) -> list:
    """Split on ';' outside string literals (''-aware; the sqltext splitter
    it parallels toggles on every quote, so boundary agreement is
    approximate — close enough for splice points, never for parsing)."""
    parts, start, in_str = [], 0, False
    i = 0
    while i < len(data):
        c = data[i:i + 1]
        if c == b"'":
            # '' is an escaped quote, skip both.
            if in_str and data[i + 1:i + 2] == b"'":
                i += 2
                continue
            in_str = not in_str
        elif c == b";" and not in_str:
            parts.append(data[start:i + 1])
            start = i + 1
        i += 1
    tail = data[start:].strip()
    if tail:
        parts.append(data[start:])
    return [p for p in parts if p.strip()]


def _find_keyword_hit(buf: bytes):
    """Find (pos, class_idx, alt_idx) for a random swappable keyword.

    Candidates match case-insensitively (upper-find); alpha keywords need
    word boundaries (alnum/_ on either side disqualifies — avoids hitting
    identifiers containing keywords). Operators skip the check. None when
    nothing is swappable (caller leaves buf unchanged).
    """
    cands = []
    upper = buf.upper()
    for ci, cls in enumerate(SWAP_CLASSES):
        for ai, kw in enumerate(cls):
            pos = upper.find(kw)
            if pos < 0:
                continue
            # Word-boundary check for alpha keywords (skip for operators).
            if kw[:1].isalpha():
                before = buf[pos - 1:pos] if pos > 0 else b" "
                after = buf[pos + len(kw):pos + len(kw) + 1]
                ok_before = not (before.isalnum() or before == b"_")
                ok_after = not (after.isalnum() or after == b"_")
                if not (ok_before and ok_after):
                    continue
            cands.append((pos, ci, ai))
    if not cands:
        return None
    return _rng.choice(cands)


def _keyword_swap(buf: bytes) -> bytes:
    """Replace one swappable keyword with a same-class alternative,
    preserving the original's case style (upper/lower; mixed falls back to
    the class's uppercase spelling). No-op when nothing is swappable."""
    hit = _find_keyword_hit(buf)
    if hit is None:
        return buf
    pos, ci, ai = hit
    cls = SWAP_CLASSES[ci]
    new_kw = _rng.choice([k for k in cls if k != cls[ai]])
    old = cls[ai]
    # Preserve case style of the original hit (upper/lower/title).
    orig = buf[pos:pos + len(old)]
    if orig.islower():
        new_kw = new_kw.lower()
    return buf[:pos] + new_kw + buf[pos + len(old):]


def _split_points(buf: bytes) -> list:
    """Whitespace/punctuation boundaries — safe token-insert positions."""
    pts = [0, len(buf)]
    for i, ch in enumerate(buf):
        if ch in b" \t\n\r(),;":
            pts.append(i)
    return pts


def _token_insert(buf: bytes) -> bytes:
    """Insert a random token at a whitespace/punctuation boundary plus a
    separating space (no mid-identifier splits; '(' needs no leading space
    since it already delimits)."""
    tok = _rng.choice(INSERT_TOKENS)
    pos = _rng.choice(_split_points(buf))
    sep = b" " if pos > 0 and buf[pos - 1:pos] not in b" \t\n\r(" else b""
    return buf[:pos] + sep + tok + b" " + buf[pos:]


def _splice_stmts(buf: bytes, add_buf: bytes) -> bytes:
    """Statement-aware splice: random prefix of one input's statements +
    random suffix of the other's (either order). Empty side falls back:
    no statements in buf takes add_buf whole, none in add_buf degrades to
    token_insert. Never returns empty (falls back to buf)."""
    a = _split_stmts(buf)
    b = _split_stmts(add_buf) if add_buf else []
    if not a:
        return add_buf[:] if add_buf else buf
    if not b:
        return _token_insert(buf)
    # Recombine: random prefix of one + random suffix of the other.
    cut_a = _rng.randint(0, len(a))
    cut_b = _rng.randint(0, len(b))
    if _rng.random() < 0.5:
        out = a[:cut_a] + b[cut_b:]
    else:
        out = b[:cut_b] + a[cut_a:]
    if not out:
        out = a
    return b" ".join(s.strip() for s in out) + b"\n"


def _literal_tweak(buf: bytes) -> bytes:
    """Mutate one literal in place: strings gain suffixes/quotes, hex swaps
    among small constants, ints randomize full-range or grow (i64-overflow
    probe 9223372036854775808), floats pick edge forms, digit-runs append.
    No literals degrades to token_insert."""
    import re
    # Find int/float/hex literals and quoted strings; tweak one at random.
    pat = re.compile(rb"0[xX][0-9a-fA-F]+|\d+\.?\d*(?:[eE][+-]?\d+)?|'[^']*(?:''[^']*)*'")
    matches = list(pat.finditer(buf))
    if not matches:
        return _token_insert(buf)
    m = _rng.choice(matches)
    lit = m.group(0)
    choice = _rng.random()
    if lit.startswith(b"'"):
        new = _rng.choice([b"'x'", b"''", b"'O''Reilly'", b"'%_'", lit + b" || 'q'"])
    elif lit[:2].lower() == b"0x":
        new = _rng.choice([b"0xCAFE", b"0xff", b"0x0", lit + b"FF"])
    elif choice < 0.3:
        new = str(_rng.randint(-2147483648, 2147483647)).encode()
    elif choice < 0.6:
        new = _rng.choice([b"0.5", b"1e10", b"1.5e-3", b"9223372036854775808"])
    else:
        new = b"NULL" if lit.strip(b"0123456789.-eE+") else lit + b"1"
    return buf[:m.start()] + new + buf[m.end():]


def _wrap_struct(buf: bytes, add_buf: bytes) -> bytes:
    """Wrap the input in a structure template (subquery, self-join, WHERE /
    UNION / ORDER+LIMIT, EXPLAIN). Empty input defaults to SELECT 1; the
    two-slot templates draw the second statement from add_buf (SELECT 2 on
    empty)."""
    stmt = buf.strip() or b"SELECT 1;"
    other = (_split_stmts(add_buf)[0].strip()
             if add_buf and _split_stmts(add_buf) else b"SELECT 2;")
    tmpl = _rng.choice(WRAP_TEMPLATES)
    if tmpl.count(b"%s") == 2:
        return tmpl % (stmt, other)
    return tmpl % stmt


def _mutate(buf: bytes, add_buf: bytes) -> tuple:
    """One weighted strategy application. Returns (out, description)."""
    r = _rng.random()
    if r < 0.30:
        return _keyword_swap(buf), "keyword_swap"
    if r < 0.55:
        return _token_insert(buf), "token_insert"
    if r < 0.75:
        return _splice_stmts(buf, add_buf), "splice_stmts"
    if r < 0.90:
        return _literal_tweak(buf), "literal_tweak"
    return _wrap_struct(buf, add_buf), "wrap_struct"


def fuzz(buf, add_buf, max_size):
    """AFL++ custom-mutator entry: one weighted strategy over (buf, add_buf),
    truncated to max_size (marked +trunc in the description). Never raises:
    any internal failure returns buf unchanged so a mutator bug can't kill
    the campaign (recorded as fallback:<err>)."""
    global _last_desc
    try:
        data = bytes(buf)
        extra = bytes(add_buf) if add_buf is not None else b""
        out, desc = _mutate(data, extra)
        _last_desc = desc
        if len(out) > max_size:
            out = out[:max_size]
            _last_desc = desc + "+trunc"
        return out
    except Exception as e:  # never kill the fuzzer on a mutator bug
        _last_desc = f"fallback:{e}"
        return bytes(buf)


def havoc_mutation(buf, max_size):
    """Havoc-stage entry: same strategies without add_buf (splice degrades
    to token_insert; wrap uses SELECT 2 as the second statement)."""
    out = fuzz(buf, b"", max_size)
    return out


def havoc_mutation_probability():
    # AFL++-consumed application weight (see the module docstring for the
    # effective rate this produces in the havoc stage).
    return 15


def describe(max_description_length):
    """AFL++ introspection: last mutation's description (init/fallback
    markers included), truncated to the requested length."""
    return _last_desc[:max_description_length].encode("utf-8", "replace")
    return _last_desc[:max_description_length].encode("utf-8", "replace")


def _selftest():
    init(42)
    cases = [
        (b"SELECT * FROM a JOIN b ON a.id = b.id;", b"SELECT x FROM t UNION SELECT y FROM u;"),
        (b"CREATE TABLE t (x INT); INSERT INTO t VALUES (1); SELECT * FROM t;",
         b"BEGIN; INSERT INTO t VALUES (2); COMMIT;"),
        (b"SELECT FROM WHERE;", b""),
        (b"", b"SELECT 1;"),
        (b"\x00\xff\xfe garbage \x80 bytes", b"SELECT 1;"),
    ]
    seen = set()
    for i, (buf, add) in enumerate(cases):
        for _ in range(40):
            out = fuzz(buf, add, 65536)
            assert isinstance(out, (bytes, bytearray)), f"case {i}: not bytes"
            assert len(out) <= 65536, f"case {i}: exceeds max_size"
            seen.add(describe(64).split(b"+")[0].decode())
    # Every strategy must fire across 200 seeded mutations.
    want = {"keyword_swap", "token_insert", "splice_stmts", "literal_tweak", "wrap_struct"}
    missing = want - seen
    assert not missing, f"strategies never fired: {missing} (seen={seen})"
    # Pool hygiene: every sql.dict value is insertable (no drift), no dupes.
    assert len(set(INSERT_TOKENS)) == len(INSERT_TOKENS), "duplicate insert tokens"
    for tok in _load_dict_tokens():
        assert tok in INSERT_TOKENS, f"dict token missing from pool: {tok!r}"
    # havoc path exercises the same core without add_buf.
    h = havoc_mutation(b"SELECT 1;", 65536)
    assert isinstance(h, (bytes, bytearray)) and len(h) <= 65536
    # Throughput probe: 2000 mutations should take well under 5s.
    import time
    t0 = time.time()
    for _ in range(2000):
        fuzz(cases[0][0], cases[0][1], 65536)
    dt = time.time() - t0
    print(f"selftest OK: strategies={sorted(seen)} 2000-mut={dt:.2f}s")
    assert dt < 5.0, f"too slow: {dt:.2f}s for 2000 mutations"
    return 0


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        sys.exit(_selftest())
    print("usage: grammar_mutator.py --selftest", file=sys.stderr)
    sys.exit(2)
