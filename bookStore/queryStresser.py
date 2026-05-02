#!/usr/bin/env python3
"""
complexQueryStressTester.py
============================

Multi-threaded stress / fuzz tester for the `idx:books` RediSearch index that
is created by `bookHashPopulatorHFE.py`.
https://cursor.com/agents/bc-dc474151-f5c0-48d2-a72b-839705ee5466

Why a separate tool?
--------------------
The populator script keeps writing / verifying / expiring data. This tool
hammers the *query* side of the engine with a wide variety of complex,
well-formed RediSearch commands so we can flush out bugs in:

  * the query parser (DIALECT 2/3/4)
  * the boolean / intersection / union iterators
  * TEXT scoring (BM25, BM25STD, TFIDF, TFIDF.DOCNORM, DISMAX, DOCSCORE,
    HAMMING)
  * TAG iteration (single and multi-value, wildcard tags)
  * fuzzy & wildcard TEXT matching (prefix, suffix, infix, %fuzzy%)
  * weighted clauses, optional clauses (~), negations (-)
  * INFIELDS / INKEYS / RETURN / NOCONTENT / WITHSCORES / EXPLAINSCORE
  * FT.AGGREGATE pipelines: APPLY, FILTER, GROUPBY/REDUCE, SORTBY, LIMIT
  * FT.AGGREGATE WITHCURSOR + FT.CURSOR READ / FT.CURSOR DEL
  * FT.EXPLAIN, FT.PROFILE SEARCH and FT.PROFILE AGGREGATE
  * Hash Field Expiration interactions (docs/fields disappearing mid-flight)

Everything is dynamically driven from the live `FT.INFO idx:books` output and
from a vocabulary that is sampled from the data already in the database, so
the queries actually hit something instead of scanning empty result sets.

Usage example
-------------

    python3 complexQueryStressTester.py \\
        --redis redis://108.130.19.251:6379 \\
        --threads 16 \\
        --duration 600 \\
        --vocab-sample 500 \\
        --error-log /tmp/qa_errors.log

Stop with Ctrl-C; a final summary is always printed.
"""

from __future__ import annotations

import argparse
import os
import random
import re
import signal
import statistics
import string
import sys
import threading
import time
from collections import Counter, defaultdict, deque
from typing import Dict, List, Optional, Sequence, Tuple

import redis


INDEX_NAME = "idx:books"
KEY_PREFIX = "alon:shmuely:redis:data:store:application:"


# ---------------------------------------------------------------------------
# Tiny utilities
# ---------------------------------------------------------------------------

def parse_kv_list(items: Sequence) -> Dict[str, object]:
    """Convert a flat [k, v, k, v, ...] list (RESP map) into a dict."""
    out: Dict[str, object] = {}
    for i in range(0, len(items) - 1, 2):
        out[str(items[i])] = items[i + 1]
    return out


# RediSearch reserves a *lot* of punctuation. When we use sampled words in
# query strings we escape every non [A-Za-z0-9_] character so that we never
# accidentally invent another query operator.
_ESCAPE_RE = re.compile(r"([,.<>{}\[\]\"':;!@#$%^&*()\-+=~|/\\? \t\n])")


def escape_token(token: str) -> str:
    return _ESCAPE_RE.sub(r"\\\1", token)


def escape_tag_value(value: str) -> str:
    """Tag values: escape every special char (RediSearch tag rules)."""
    return _ESCAPE_RE.sub(r"\\\1", value)


# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------

class IndexSchema:
    """Dynamically reads the schema from FT.INFO so query generation always
    reflects what the server believes the index looks like."""

    def __init__(self, r: redis.Redis, index_name: str):
        info = r.execute_command("FT.INFO", index_name)
        info_dict = parse_kv_list(info)

        self.text_fields: List[str] = []
        # tag_fields[name] = separator
        self.tag_fields: Dict[str, str] = {}

        for attr in info_dict.get("attributes", []) or []:
            attr_dict = parse_kv_list(attr)
            ftype = str(attr_dict.get("type", "")).upper()
            name = str(attr_dict.get("attribute") or attr_dict.get("identifier"))
            if ftype == "TEXT":
                self.text_fields.append(name)
            elif ftype == "TAG":
                sep = str(attr_dict.get("SEPARATOR", ","))
                self.tag_fields[name] = sep

        # Tag fields whose separator is "," typically hold a single token per
        # document (booleans, ids, prices stored as strings, ...). Multi-value
        # tag fields use "|" and behave like arrays in FT.AGGREGATE APPLY,
        # which means string functions like substr/upper/lower fail on them.
        self.scalar_tag_fields: List[str] = [
            f for f, s in self.tag_fields.items() if s == ","
        ]
        self.multi_tag_fields: List[str] = [
            f for f, s in self.tag_fields.items() if s != ","
        ]

        self.all_fields: List[str] = self.text_fields + list(self.tag_fields)
        if not self.all_fields:
            raise RuntimeError(
                f"Index {index_name!r} reports no indexable attributes; "
                f"is the populator running?"
            )

        defn = parse_kv_list(info_dict.get("index_definition", []) or [])
        self.prefixes: List[str] = list(defn.get("prefixes", []) or [KEY_PREFIX])
        self.num_docs: int = int(info_dict.get("num_docs", 0) or 0)


# ---------------------------------------------------------------------------
# Vocabulary sampling
# ---------------------------------------------------------------------------

class Vocabulary:
    """Samples actual hash documents to build per-field token pools so the
    queries we synthesize match real data."""

    # Stop-words RediSearch strips by default; never use them as primary tokens
    _STOPWORDS = {
        "a", "is", "the", "an", "and", "are", "as", "at", "be", "but",
        "by", "for", "if", "in", "into", "it", "no", "not", "of", "on",
        "or", "such", "that", "their", "then", "there", "these", "they",
        "this", "to", "was", "will", "with",
    }
    _WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{2,}")

    def __init__(
        self,
        r: redis.Redis,
        schema: IndexSchema,
        sample_size: int,
        per_field_max: int = 200,
    ):
        self.text_words: Dict[str, List[str]] = defaultdict(list)
        self.tag_values: Dict[str, List[str]] = defaultdict(list)
        self.keys: List[str] = []

        prefix_pattern = (schema.prefixes[0] + "*") if schema.prefixes else "*"
        seen = 0
        # randomise scan starting cursor so successive runs sample differently
        cursor = random.randint(0, 1024) * 0  # SCAN cursor must be 0 to start

        for key in r.scan_iter(match=prefix_pattern, count=max(100, sample_size)):
            seen += 1
            self.keys.append(key)
            try:
                hash_data = r.hgetall(key)
            except redis.exceptions.ResponseError:
                continue

            for field, value in hash_data.items():
                if value is None:
                    continue
                value = str(value)

                if field in schema.text_fields:
                    pool = self.text_words[field]
                    if len(pool) < per_field_max:
                        for tok in self._WORD_RE.findall(value)[:5]:
                            tl = tok.lower()
                            if tl not in self._STOPWORDS and len(pool) < per_field_max:
                                pool.append(tok)

                elif field in schema.tag_fields:
                    sep = schema.tag_fields[field]
                    pool = self.tag_values[field]
                    if len(pool) < per_field_max:
                        # Multi-value tag fields use their separator;
                        # single-value tag fields hold the raw string.
                        parts = value.split(sep) if sep != "," else [value]
                        for p in parts:
                            p = p.strip()
                            if p and len(pool) < per_field_max:
                                pool.append(p)
            if seen >= sample_size:
                break

        # Make every field at least non-empty so the generator never crashes.
        for f in schema.text_fields:
            if not self.text_words[f]:
                self.text_words[f] = ["the", "and", "book"]
        for f in schema.tag_fields:
            if not self.tag_values[f]:
                self.tag_values[f] = ["unknown"]

    def text_word(self, field: str) -> str:
        return random.choice(self.text_words[field])

    def tag_value(self, field: str) -> str:
        return random.choice(self.tag_values[field])

    def sample_keys(self, k: int) -> List[str]:
        if not self.keys:
            return []
        return random.sample(self.keys, min(k, len(self.keys)))


# ---------------------------------------------------------------------------
# Query generation
# ---------------------------------------------------------------------------

class QueryFactory:
    SCORERS = ("BM25", "BM25STD", "TFIDF", "TFIDF.DOCNORM", "DISMAX", "DOCSCORE", "HAMMING")
    DIALECTS = (2, 3, 4)
    LANGUAGES = ("english", "german", "french", "spanish", "italian", "portuguese", "russian")

    AGG_REDUCERS = (
        "COUNT",
        "COUNT_DISTINCT",
        "TOLIST",
        "FIRST_VALUE",
        "RANDOM_SAMPLE",
    )

    def __init__(self, schema: IndexSchema, vocab: Vocabulary, rnd: random.Random):
        self.schema = schema
        self.vocab = vocab
        self.rnd = rnd

    # ---------------- atomic clause builders ----------------

    def _text_atom(self, field: Optional[str] = None) -> str:
        field = field or self.rnd.choice(self.schema.text_fields)
        word = escape_token(self.vocab.text_word(field))
        flavor = self.rnd.choices(
            ["plain", "prefix", "suffix", "infix", "fuzzy1", "fuzzy2", "phrase", "weighted"],
            weights=[28, 18, 8, 8, 12, 4, 12, 10],
            k=1,
        )[0]

        if flavor == "plain":
            atom = word
        elif flavor == "prefix":
            atom = (word[: max(2, len(word) // 2)] or word) + "*"
        elif flavor == "suffix":
            atom = "*" + (word[-max(2, len(word) // 2):] or word)
        elif flavor == "infix":
            mid = word[1:-1] if len(word) > 3 else word
            atom = "*" + mid + "*"
        elif flavor == "fuzzy1":
            atom = "%" + word + "%"
        elif flavor == "fuzzy2":
            atom = "%%" + word + "%%"
        elif flavor == "phrase":
            second_word = escape_token(self.vocab.text_word(field))
            slop = self.rnd.randint(0, 5)
            atom = f'"{word} {second_word}"=>{{$slop:{slop}; $inorder:{str(self.rnd.choice([True,False])).lower()}}}'
        elif flavor == "weighted":
            weight = round(self.rnd.uniform(0.1, 5.0), 2)
            return f"(@{field}:{word} => {{ $weight: {weight} }})"
        else:
            atom = word

        return f"@{field}:{atom}"

    def _tag_atom(self, field: Optional[str] = None) -> str:
        field = field or self.rnd.choice(list(self.schema.tag_fields))
        sep = self.schema.tag_fields[field]
        # Multi-value tag fields can take a union of several tags
        max_terms = 6 if sep == "|" else 3
        n_terms = self.rnd.randint(1, max_terms)

        tags = []
        for _ in range(n_terms):
            v = self.vocab.tag_value(field)
            if self.rnd.random() < 0.20 and len(v) > 3:
                # wildcard tag, only valid in DIALECT >=2 (we always send >=2)
                tags.append("w'" + v[: self.rnd.randint(2, len(v) - 1)] + "*'")
            else:
                tags.append(escape_tag_value(v))
        return "@" + field + ":{" + "|".join(tags) + "}"

    def _negated(self, atom: str) -> str:
        return "-" + atom

    def _optional(self, atom: str) -> str:
        return "~" + atom

    def _grouped(self, atoms: List[str], op: str) -> str:
        joiner = " " if op == "AND" else "|"
        return "(" + joiner.join(atoms) + ")"

    # ---------------- composite query string ----------------

    def make_query_string(self) -> str:
        # 1% chance of pure match-all
        if self.rnd.random() < 0.01:
            return "*"

        clauses: List[str] = []
        n_clauses = self.rnd.randint(2, 7)
        for _ in range(n_clauses):
            kind = self.rnd.choices(
                ["text", "tag", "tag_or_text_neg", "optional_text"],
                weights=[40, 40, 12, 8], k=1,
            )[0]

            if kind == "text":
                clauses.append(self._text_atom())
            elif kind == "tag":
                clauses.append(self._tag_atom())
            elif kind == "tag_or_text_neg":
                base = self._tag_atom() if self.rnd.random() < 0.5 else self._text_atom()
                clauses.append(self._negated(base))
            else:
                clauses.append(self._optional(self._text_atom()))

        # Maybe wrap into a UNION sub-tree.
        if self.rnd.random() < 0.30 and len(clauses) >= 3:
            split = self.rnd.randint(1, len(clauses) - 1)
            left = self._grouped(clauses[:split], "OR")
            right = self._grouped(clauses[split:], "AND")
            query = left + " " + right
        else:
            query = " ".join(clauses)

        return query

    # ---------------- FT.SEARCH command ----------------

    def make_search(self) -> List[str]:
        qstr = self.make_query_string()
        cmd: List[str] = ["FT.SEARCH", INDEX_NAME, qstr]

        if self.rnd.random() < 0.40:
            cmd += ["VERBATIM"]
        if self.rnd.random() < 0.30:
            cmd += ["NOSTOPWORDS"]
        if self.rnd.random() < 0.20 and self.schema.text_fields:
            n = self.rnd.randint(1, len(self.schema.text_fields))
            chosen = self.rnd.sample(self.schema.text_fields, n)
            cmd += ["INFIELDS", str(len(chosen))] + chosen
        if self.rnd.random() < 0.10:
            keys = self.vocab.sample_keys(self.rnd.randint(1, 5))
            if keys:
                cmd += ["INKEYS", str(len(keys))] + keys

        if self.rnd.random() < 0.50:
            cmd += ["NOCONTENT"]
        else:
            if self.rnd.random() < 0.40 and self.schema.all_fields:
                n = self.rnd.randint(1, min(6, len(self.schema.all_fields)))
                ret = self.rnd.sample(self.schema.all_fields, n)
                cmd += ["RETURN", str(len(ret))] + ret

        if self.rnd.random() < 0.30:
            cmd += ["WITHSCORES"]
            if self.rnd.random() < 0.5:
                cmd += ["EXPLAINSCORE"]

        if self.rnd.random() < 0.40:
            cmd += ["SCORER", self.rnd.choice(self.SCORERS)]

        if self.rnd.random() < 0.20:
            cmd += ["LANGUAGE", self.rnd.choice(self.LANGUAGES)]

        # Always cap server-side to avoid blocking workers behind a single
        # pathological query; bias toward sane values but occasionally try
        # very tight or very loose limits.
        cmd += ["TIMEOUT", str(self.rnd.choice([200, 500, 1000, 2000, 5000, 10000]))]

        if self.rnd.random() < 0.20:
            cmd += ["SLOP", str(self.rnd.randint(0, 10))]

        offset = self.rnd.choice([0, 0, 0, 10, 100, 1000])
        limit = self.rnd.choice([1, 10, 50, 200])
        cmd += ["LIMIT", str(offset), str(limit)]

        cmd += ["DIALECT", str(self.rnd.choice(self.DIALECTS))]
        return cmd

    # ---------------- FT.AGGREGATE command ----------------

    def make_aggregate(self) -> Tuple[List[str], bool]:
        """Returns (command, uses_cursor)."""
        qstr = self.make_query_string()
        cmd: List[str] = ["FT.AGGREGATE", INDEX_NAME, qstr]

        if self.rnd.random() < 0.40:
            cmd += ["VERBATIM"]

        # LOAD some fields so APPLY has data
        load_fields = self.rnd.sample(
            self.schema.all_fields,
            self.rnd.randint(1, min(6, len(self.schema.all_fields))),
        )
        cmd += ["LOAD", str(len(load_fields))] + ["@" + f for f in load_fields]

        # Pick a TAG field to GROUPBY (text fields can't be grouped on directly
        # without LOAD; tag fields are safe and common)
        group_field = self.rnd.choice(list(self.schema.tag_fields))
        cmd += ["GROUPBY", "1", "@" + group_field]

        # 1-3 reducers
        n_red = self.rnd.randint(1, 3)
        for i in range(n_red):
            reducer = self.rnd.choice(self.AGG_REDUCERS)
            alias = f"r_{i}"
            if reducer == "COUNT":
                cmd += ["REDUCE", "COUNT", "0", "AS", alias]
            elif reducer == "COUNT_DISTINCT":
                fld = self.rnd.choice(self.schema.all_fields)
                cmd += ["REDUCE", "COUNT_DISTINCT", "1", "@" + fld, "AS", alias]
            elif reducer == "TOLIST":
                fld = self.rnd.choice(self.schema.all_fields)
                cmd += ["REDUCE", "TOLIST", "1", "@" + fld, "AS", alias]
            elif reducer == "FIRST_VALUE":
                fld = self.rnd.choice(self.schema.all_fields)
                cmd += ["REDUCE", "FIRST_VALUE", "1", "@" + fld, "AS", alias]
            elif reducer == "RANDOM_SAMPLE":
                fld = self.rnd.choice(self.schema.all_fields)
                size = self.rnd.randint(1, 5)
                cmd += ["REDUCE", "RANDOM_SAMPLE", "2", "@" + fld, str(size), "AS", alias]

        # Optional APPLY (post-group) using a string function. After
        # GROUPBY, only the group key and reducer aliases survive in the
        # pipeline; referencing any other field yields SEARCH_PROP_NOT_FOUND.
        # We also need a scalar string (multi-value TAGs break substr/upper).
        if self.rnd.random() < 0.5 and group_field in self.schema.scalar_tag_fields:
            string_field = group_field
            expr = self.rnd.choice([
                f"upper(@{string_field})",
                f"lower(@{string_field})",
                f"format(\"k=%s\", @{string_field})",
                f"substr(@{string_field}, 0, 3)",
                f"strlen(@{string_field})",
            ])
            cmd += ["APPLY", expr, "AS", "applied"]

        # Optional FILTER on the COUNT alias (if reducer 0 is COUNT)
        if self.rnd.random() < 0.4:
            cmd += ["FILTER", f"@r_0 > {self.rnd.randint(0, 5)}"]

        # SORTBY on the group field
        if self.rnd.random() < 0.6:
            direction = self.rnd.choice(["ASC", "DESC"])
            cmd += ["SORTBY", "2", "@" + group_field, direction]
            if self.rnd.random() < 0.3:
                cmd += ["MAX", str(self.rnd.randint(5, 50))]

        cmd += ["LIMIT", "0", str(self.rnd.choice([10, 50, 200]))]
        cmd += ["TIMEOUT", str(self.rnd.choice([500, 1000, 2000, 5000, 10000]))]

        uses_cursor = self.rnd.random() < 0.20
        if uses_cursor:
            cmd += ["WITHCURSOR", "COUNT", str(self.rnd.choice([10, 50, 200]))]

        cmd += ["DIALECT", str(self.rnd.choice(self.DIALECTS))]
        return cmd, uses_cursor

    # ---------------- profiling / explain wrappers ----------------

    def make_explain(self) -> List[str]:
        return ["FT.EXPLAIN", INDEX_NAME, self.make_query_string(),
                "DIALECT", str(self.rnd.choice(self.DIALECTS))]

    def make_profile_search(self) -> List[str]:
        inner = self.make_search()
        qstr = inner[2]
        return [
            "FT.PROFILE", INDEX_NAME, "SEARCH", "QUERY", qstr,
            "LIMIT", "0", "10",
            "TIMEOUT", str(self.rnd.choice([1000, 2000, 5000])),
            "DIALECT", str(self.rnd.choice(self.DIALECTS)),
        ]

    def make_profile_aggregate(self) -> List[str]:
        inner, _ = self.make_aggregate()
        qstr = inner[2]
        return [
            "FT.PROFILE", INDEX_NAME, "AGGREGATE", "QUERY", qstr,
            "LOAD", "1", "@id",
            "GROUPBY", "1", "@" + self.rnd.choice(list(self.schema.tag_fields)),
            "REDUCE", "COUNT", "0", "AS", "n",
            "TIMEOUT", str(self.rnd.choice([1000, 2000, 5000])),
            "DIALECT", str(self.rnd.choice(self.DIALECTS)),
        ]


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.total = 0
        self.errors = 0
        self.timeouts = 0
        self.empty = 0
        self.hits = 0
        self.per_op = Counter()
        self.per_op_err = Counter()
        # Keep a bounded ring of latencies per op for percentiles
        self.latencies: Dict[str, deque] = defaultdict(lambda: deque(maxlen=5000))
        self.error_samples: Dict[str, int] = Counter()
        self.slowest: deque = deque(maxlen=10)
        self.start = time.monotonic()

    def record(self, op: str, latency_ms: float, ok: bool, err: Optional[str], hits: int):
        with self.lock:
            self.total += 1
            self.per_op[op] += 1
            self.latencies[op].append(latency_ms)
            if ok:
                self.hits += hits
                if hits == 0:
                    self.empty += 1
            else:
                self.errors += 1
                self.per_op_err[op] += 1
                if err:
                    # Bucket errors by the first 80 chars
                    self.error_samples[err[:80]] += 1
                    if "timed out" in err.lower() or "timeout" in err.lower():
                        self.timeouts += 1
            self.slowest.append((latency_ms, op))

    def snapshot(self) -> str:
        with self.lock:
            elapsed = max(0.001, time.monotonic() - self.start)
            qps = self.total / elapsed
            lines = [
                f"elapsed={elapsed:7.1f}s  total={self.total:>8d}  qps={qps:7.1f}  "
                f"errors={self.errors:>5d}  timeouts={self.timeouts:>4d}  "
                f"empty_results={self.empty:>5d}  total_hits={self.hits:>10d}",
            ]
            for op in sorted(self.per_op):
                lats = sorted(self.latencies[op])
                if lats:
                    p50 = lats[len(lats)//2]
                    p95 = lats[int(len(lats)*0.95) - 1] if len(lats) >= 20 else lats[-1]
                    p99 = lats[int(len(lats)*0.99) - 1] if len(lats) >= 100 else lats[-1]
                    lines.append(
                        f"  {op:<22s} count={self.per_op[op]:>7d}  "
                        f"err={self.per_op_err[op]:>5d}  "
                        f"p50={p50:7.2f}ms  p95={p95:7.2f}ms  p99={p99:7.2f}ms  "
                        f"max={lats[-1]:7.2f}ms"
                    )
            return "\n".join(lines)

    def final_summary(self) -> str:
        snap = self.snapshot()
        with self.lock:
            top_errors = self.error_samples.most_common(10)
            slowest = sorted(self.slowest, reverse=True)[:5]
        extra = ["", "Top error buckets:"]
        if not top_errors:
            extra.append("  (none)")
        for msg, n in top_errors:
            extra.append(f"  [{n:>5d}]  {msg}")
        extra.append("")
        extra.append("Slowest sampled operations:")
        for lat, op in slowest:
            extra.append(f"  {lat:>9.2f}ms   {op}")
        return snap + "\n" + "\n".join(extra)


STOP = threading.Event()


def worker(
    pool: redis.ConnectionPool,
    schema: IndexSchema,
    vocab: Vocabulary,
    stats: Stats,
    error_log_path: Optional[str],
    seed: int,
    duration: Optional[float],
    max_queries: Optional[int],
):
    rnd = random.Random(seed)
    factory = QueryFactory(schema, vocab, rnd)
    r = redis.Redis(connection_pool=pool)

    deadline = (time.monotonic() + duration) if duration else None

    error_log = open(error_log_path, "a", buffering=1) if error_log_path else None

    op_choices = [
        ("FT.SEARCH",            45, lambda: ("FT.SEARCH",          factory.make_search(),    None)),
        ("FT.AGGREGATE",         25, lambda: ("FT.AGGREGATE",       *_agg(factory))),
        ("FT.AGGREGATE+CURSOR",  10, lambda: ("FT.AGGREGATE+CURSOR", *_agg_cursor(factory))),
        ("FT.EXPLAIN",            8, lambda: ("FT.EXPLAIN",         factory.make_explain(),   None)),
        ("FT.PROFILE.SEARCH",     7, lambda: ("FT.PROFILE.SEARCH",  factory.make_profile_search(), None)),
        ("FT.PROFILE.AGGREGATE",  5, lambda: ("FT.PROFILE.AGGREGATE", factory.make_profile_aggregate(), None)),
    ]
    op_names = [c[0] for c in op_choices]
    op_weights = [c[1] for c in op_choices]
    op_funcs = {c[0]: c[2] for c in op_choices}

    local_count = 0
    while not STOP.is_set():
        if deadline and time.monotonic() > deadline:
            break
        if max_queries and stats.total >= max_queries:
            break

        chosen_name = rnd.choices(op_names, weights=op_weights, k=1)[0]
        try:
            op_label, cmd, follow = op_funcs[chosen_name]()
        except Exception as build_err:
            # Generator bug, log and keep going
            stats.record(chosen_name, 0.0, False, f"GEN: {build_err}", 0)
            continue

        t0 = time.monotonic()
        ok, err, hits = True, None, 0
        try:
            resp = r.execute_command(*cmd)
            hits = _extract_hit_count(op_label, resp)
            # If aggregate-with-cursor, drain it a bit
            if op_label == "FT.AGGREGATE+CURSOR" and isinstance(resp, list) and len(resp) == 2:
                cursor_id = resp[1]
                drained = 0
                while cursor_id and drained < 5 and not STOP.is_set():
                    nxt = r.execute_command("FT.CURSOR", "READ", INDEX_NAME, cursor_id)
                    if not (isinstance(nxt, list) and len(nxt) == 2):
                        break
                    cursor_id = nxt[1]
                    drained += 1
                if cursor_id:
                    try:
                        r.execute_command("FT.CURSOR", "DEL", INDEX_NAME, cursor_id)
                    except redis.exceptions.ResponseError:
                        pass
        except redis.exceptions.ResponseError as e:
            ok = False
            err = f"ResponseError: {e}"
        except redis.exceptions.ConnectionError as e:
            ok = False
            err = f"ConnectionError: {e}"
        except redis.exceptions.TimeoutError as e:
            ok = False
            err = f"TimeoutError: {e}"
        except Exception as e:
            ok = False
            err = f"{type(e).__name__}: {e}"
        latency_ms = (time.monotonic() - t0) * 1000.0

        stats.record(op_label, latency_ms, ok, err, hits)
        if not ok and error_log is not None:
            try:
                error_log.write(
                    f"--- {time.strftime('%Y-%m-%dT%H:%M:%S')} {op_label}\n"
                    f"ERR: {err}\nCMD: {' '.join(_quote_arg(x) for x in cmd)}\n\n"
                )
            except Exception:
                pass

        local_count += 1

    if error_log is not None:
        error_log.close()


def _agg(factory: QueryFactory):
    cmd, _ = factory.make_aggregate()
    return cmd, None


def _agg_cursor(factory: QueryFactory):
    # Force a WITHCURSOR command
    cmd, with_cursor = factory.make_aggregate()
    if not with_cursor:
        cmd += ["WITHCURSOR", "COUNT", "50"]
    return cmd, None


def _extract_hit_count(op: str, resp) -> int:
    if op == "FT.SEARCH" and isinstance(resp, list) and resp:
        try:
            return int(resp[0])
        except (TypeError, ValueError):
            return 0
    if op.startswith("FT.AGGREGATE") and isinstance(resp, list):
        body = resp[0] if op == "FT.AGGREGATE+CURSOR" else resp
        if isinstance(body, list) and body:
            try:
                return int(body[0])
            except (TypeError, ValueError):
                return 0
    return 0


def _quote_arg(arg) -> str:
    s = str(arg)
    if any(ch in s for ch in [" ", "\t", '"', "'", "$"]):
        return '"' + s.replace('"', '\\"') + '"'
    return s


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def install_signal_handlers():
    def handler(signum, frame):
        STOP.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, handler)
        except (ValueError, OSError):
            pass


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--redis", default=os.environ.get("REDIS_URL", "redis://localhost:6379"),
                   dest="redis_url", help="Redis URL.")
    p.add_argument("--threads", type=int, default=8, help="Number of worker threads.")
    p.add_argument("--max-connections", type=int, default=None,
                   help="Connection pool size (default: 2 * threads).")
    p.add_argument("--duration", type=float, default=None,
                   help="Run for N seconds (default: until --queries or Ctrl-C).")
    p.add_argument("--queries", type=int, default=None,
                   help="Total queries to issue across all workers.")
    p.add_argument("--vocab-sample", type=int, default=300,
                   help="Number of hashes to scan to build the query vocabulary.")
    p.add_argument("--seed", type=int, default=None, help="Master RNG seed.")
    p.add_argument("--error-log", default=None,
                   help="Append failing commands and their errors to this file.")
    p.add_argument("--status-interval", type=float, default=5.0,
                   help="Seconds between live status prints.")
    p.add_argument("--socket-timeout", type=float, default=15.0,
                   help="Per-command client socket timeout in seconds. "
                        "Should be greater than the largest server-side "
                        "TIMEOUT value the generator emits (10s).")
    args = p.parse_args()

    install_signal_handlers()

    print(f"[init] connecting to {args.redis_url}")
    pool = redis.ConnectionPool.from_url(
        args.redis_url,
        max_connections=args.max_connections or max(8, args.threads * 2),
        decode_responses=True,
        socket_timeout=args.socket_timeout,
        socket_connect_timeout=10,
    )
    r = redis.Redis(connection_pool=pool)
    pong = r.ping()
    print(f"[init] PING -> {pong}")

    schema = IndexSchema(r, INDEX_NAME)
    print(f"[init] index {INDEX_NAME!r}: {schema.num_docs} docs, "
          f"{len(schema.text_fields)} TEXT, {len(schema.tag_fields)} TAG fields")

    print(f"[init] sampling vocabulary from {args.vocab_sample} docs ...")
    vocab = Vocabulary(r, schema, sample_size=args.vocab_sample)
    n_text_pools = sum(1 for f in schema.text_fields if vocab.text_words[f])
    n_tag_pools = sum(1 for f in schema.tag_fields if vocab.tag_values[f])
    sampled_keys = len(vocab.keys)
    print(f"[init] vocabulary built: {sampled_keys} keys sampled, "
          f"{n_text_pools} TEXT pools, {n_tag_pools} TAG pools")

    stats = Stats()

    master_seed = args.seed if args.seed is not None else random.randint(0, 2**31 - 1)
    print(f"[init] master seed = {master_seed}")
    print(f"[run] launching {args.threads} workers"
          + (f" for {args.duration}s" if args.duration else "")
          + (f", up to {args.queries} queries total" if args.queries else "")
          + (f", logging errors to {args.error_log}" if args.error_log else ""))

    threads: List[threading.Thread] = []
    for tid in range(args.threads):
        t = threading.Thread(
            target=worker,
            args=(pool, schema, vocab, stats, args.error_log,
                  master_seed + tid, args.duration, args.queries),
            name=f"qa-worker-{tid}",
            daemon=True,
        )
        t.start()
        threads.append(t)

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(args.status_interval)
            print(stats.snapshot(), flush=True)
            if STOP.is_set():
                break
    except KeyboardInterrupt:
        STOP.set()

    print("\n[shutdown] waiting for workers ...")
    for t in threads:
        t.join(timeout=15)

    print("\n=========== FINAL SUMMARY ===========")
    print(stats.final_summary())


if __name__ == "__main__":
    sys.exit(main())
