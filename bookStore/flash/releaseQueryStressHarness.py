#!/usr/bin/env python3
"""
RedisSearch release stress harness for idx:books.

Builds complex FT.SEARCH / FT.AGGREGATE / FT.PROFILE workloads from the live
schema and sampled data, then writes a JSON summary report.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import redis


INDEX_NAME = "idx:books"
DEFAULT_PREFIX = "alon:shmuely:redis:data:store:application:"
ESCAPE_RE = re.compile(r"([,.<>{}\[\]\"':;!@#$%^&*()\-+=~|/\\? \t\n])")
WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{2,}")


def esc_tag(value: str) -> str:
    return ESCAPE_RE.sub(r"\\\1", value)


def as_map(flat: List) -> Dict[str, object]:
    return {str(flat[i]): flat[i + 1] for i in range(0, len(flat) - 1, 2)}


@dataclass
class Schema:
    text_fields: List[str]
    tag_fields: Dict[str, str]
    prefixes: List[str]
    num_docs: int


def load_schema(r: redis.Redis) -> Schema:
    info = as_map(r.execute_command("FT.INFO", INDEX_NAME))
    text_fields: List[str] = []
    tag_fields: Dict[str, str] = {}
    for attr in info.get("attributes", []):
        a = as_map(attr)
        typ = str(a.get("type", "")).upper()
        name = str(a.get("attribute") or a.get("identifier"))
        if typ == "TEXT":
            text_fields.append(name)
        elif typ == "TAG":
            tag_fields[name] = str(a.get("SEPARATOR", ","))
    definition = as_map(info.get("index_definition", []))
    prefixes = list(definition.get("prefixes", []) or [DEFAULT_PREFIX])
    return Schema(
        text_fields=text_fields,
        tag_fields=tag_fields,
        prefixes=prefixes,
        num_docs=int(info.get("num_docs", 0) or 0),
    )


@dataclass
class Vocabulary:
    text_tokens: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))
    tag_values: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))
    guaranteed_clauses: List[str] = field(default_factory=list)
    sampled_keys: int = 0


def build_vocab(r: redis.Redis, schema: Schema, sample_docs: int) -> Vocabulary:
    vocab = Vocabulary()

    for field in schema.tag_fields:
        try:
            vals = r.execute_command("FT.TAGVALS", INDEX_NAME, field) or []
            vocab.tag_values[field] = [str(v) for v in vals[:200] if str(v)]
        except redis.exceptions.ResponseError:
            vocab.tag_values[field] = []

    pattern = (schema.prefixes[0] + "*") if schema.prefixes else "*"
    for i, key in enumerate(r.scan_iter(match=pattern, count=max(100, sample_docs))):
        if i >= sample_docs:
            break
        vocab.sampled_keys += 1
        try:
            data = r.hgetall(key)
        except redis.exceptions.ResponseError:
            continue
        for f in schema.text_fields:
            value = str(data.get(f, ""))
            if not value:
                continue
            pool = vocab.text_tokens[f]
            for tok in WORD_RE.findall(value)[:6]:
                if len(pool) < 120:
                    pool.append(tok)

    for field in schema.text_fields:
        if not vocab.text_tokens[field]:
            vocab.text_tokens[field] = ["redis", "search", "book", "quality"]
    for field in schema.tag_fields:
        if not vocab.tag_values[field]:
            vocab.tag_values[field] = ["unknown"]

    preferred_hit_fields = ("status", "format", "is_available")
    for field in preferred_hit_fields:
        values = vocab.tag_values.get(field, [])
        if values:
            uniq = list(dict.fromkeys(values))[:8]
            clause = f"@{field}:{{{'|'.join(esc_tag(v) for v in uniq)}}}"
            vocab.guaranteed_clauses.append(clause)

    if not vocab.guaranteed_clauses:
        # Fallback: use any tag field with known values.
        for field, values in vocab.tag_values.items():
            if values:
                uniq = list(dict.fromkeys(values))[:8]
                clause = f"@{field}:{{{'|'.join(esc_tag(v) for v in uniq)}}}"
                vocab.guaranteed_clauses.append(clause)
                break

    return vocab


def pick_text(vocab: Vocabulary, field: str, rnd: random.Random) -> str:
    tok = rnd.choice(vocab.text_tokens[field])
    return ESCAPE_RE.sub(r"\\\1", tok)


def pick_tag(vocab: Vocabulary, field: str, rnd: random.Random) -> str:
    return esc_tag(rnd.choice(vocab.tag_values[field]))


def text_atom(schema: Schema, vocab: Vocabulary, rnd: random.Random) -> str:
    field = rnd.choice(schema.text_fields)
    token = pick_text(vocab, field, rnd)
    flavor = rnd.choices(
        ["plain", "prefix", "fuzzy", "phrase"],
        weights=[45, 25, 15, 15],
        k=1,
    )[0]
    if flavor == "plain":
        expr = token
    elif flavor == "prefix":
        expr = (token[: max(2, len(token) // 2)] or token) + "*"
    elif flavor == "fuzzy":
        expr = f"%{token}%"
    else:
        token2 = pick_text(vocab, field, rnd)
        expr = f'"{token} {token2}"'
    return f"@{field}:{expr}"


def tag_atom(schema: Schema, vocab: Vocabulary, rnd: random.Random) -> str:
    field = rnd.choice(list(schema.tag_fields))
    values = vocab.tag_values[field]
    n_values = rnd.randint(1, min(3, len(values)))
    chosen = rnd.sample(values, k=n_values) if len(values) >= n_values else [rnd.choice(values)]
    return f"@{field}:{{{'|'.join(esc_tag(v) for v in chosen)}}}"


def build_complex_query(schema: Schema, vocab: Vocabulary, rnd: random.Random) -> str:
    t1, t2, t3, t4 = [text_atom(schema, vocab, rnd) for _ in range(4)]
    g1, g2, g3, g4 = [tag_atom(schema, vocab, rnd) for _ in range(4)]
    guarantee = rnd.choice(vocab.guaranteed_clauses) if vocab.guaranteed_clauses else tag_atom(schema, vocab, rnd)

    branch_left = f"(({t1} {g1})|({t2} {g2} ~{t3}))"
    branch_right = f"(({g3}|{t4}) -{g4})"
    core = f"({branch_left} {branch_right})"
    # Ensure non-empty responses by keeping one broad, known-good disjunction branch.
    return f"({core}|({guarantee}))"


def make_search(schema: Schema, vocab: Vocabulary, rnd: random.Random) -> List[str]:
    q = build_complex_query(schema, vocab, rnd)
    offset = rnd.choice([0, 10, 100, 300])
    limit = rnd.choice([10, 50, 100])
    cmd = ["FT.SEARCH", INDEX_NAME, q]
    if rnd.random() < 0.5:
        cmd += ["NOCONTENT"]
    else:
        ret = rnd.sample(schema.text_fields + list(schema.tag_fields), k=min(6, len(schema.text_fields) + len(schema.tag_fields)))
        cmd += ["RETURN", str(len(ret))] + ret
    if rnd.random() < 0.4:
        cmd += ["WITHSCORES"]
    cmd += ["TIMEOUT", str(rnd.choice([500, 1000, 2000, 5000]))]
    cmd += ["LIMIT", str(offset), str(limit), "DIALECT", str(rnd.choice([2, 3, 4]))]
    return cmd


def make_aggregate(schema: Schema, vocab: Vocabulary, rnd: random.Random) -> List[str]:
    tag_fields = list(schema.tag_fields)
    if len(tag_fields) >= 2:
        group_field, group_field_2 = rnd.sample(tag_fields, k=2)
    else:
        group_field = group_field_2 = tag_fields[0]
    distinct_field = rnd.choice(tag_fields)
    q = build_complex_query(schema, vocab, rnd)
    load_1 = rnd.choice(tag_fields)
    load_2 = rnd.choice(schema.text_fields)
    cmd = [
        "FT.AGGREGATE",
        INDEX_NAME,
        q,
        "LOAD",
        "2",
        f"@{load_1}",
        f"@{load_2}",
        "GROUPBY",
        "2",
        f"@{group_field}",
        f"@{group_field_2}",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "cnt",
        "REDUCE",
        "COUNT_DISTINCT",
        "1",
        f"@{distinct_field}",
        "AS",
        "uniq",
        "SORTBY",
        "2",
        "@cnt",
        "DESC",
        "LIMIT",
        "0",
        str(rnd.choice([20, 50, 100])),
        "TIMEOUT",
        str(rnd.choice([1000, 2000, 5000])),
        "DIALECT",
        str(rnd.choice([2, 3, 4])),
    ]
    if rnd.random() < 0.25:
        cmd += ["WITHCURSOR", "COUNT", str(rnd.choice([20, 50]))]
    return cmd


def make_profile(schema: Schema, vocab: Vocabulary, rnd: random.Random) -> List[str]:
    q = build_complex_query(schema, vocab, rnd)
    return [
        "FT.PROFILE",
        INDEX_NAME,
        "SEARCH",
        "QUERY",
        q,
        "LIMIT",
        "0",
        "20",
        "TIMEOUT",
        str(rnd.choice([1000, 2000, 5000])),
        "DIALECT",
        str(rnd.choice([2, 3, 4])),
    ]


def op_name(cmd: List[str]) -> str:
    head = cmd[0]
    if head == "FT.PROFILE":
        return "FT.PROFILE.SEARCH"
    return head


@dataclass
class Stats:
    total: int = 0
    hits: int = 0
    non_empty_hits: int = 0
    errors: int = 0
    op_counts: Counter = field(default_factory=Counter)
    op_errors: Counter = field(default_factory=Counter)
    latencies: Dict[str, List[float]] = field(default_factory=lambda: defaultdict(list))
    error_buckets: Counter = field(default_factory=Counter)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, op: str, latency_ms: float, ok: bool, hits: int, err: Optional[str]) -> None:
        with self.lock:
            self.total += 1
            self.op_counts[op] += 1
            self.latencies[op].append(latency_ms)
            self.hits += hits
            if ok and hits > 0:
                self.non_empty_hits += 1
            if not ok:
                self.errors += 1
                self.op_errors[op] += 1
                if err:
                    self.error_buckets[err[:120]] += 1


def extract_hits(cmd: List[str], resp) -> int:
    if not isinstance(resp, list) or not resp:
        return 0
    if cmd[0] == "FT.SEARCH":
        try:
            return int(resp[0])
        except (TypeError, ValueError):
            return 0
    if cmd[0] == "FT.AGGREGATE":
        body = resp[0] if len(resp) == 2 and isinstance(resp[0], list) else resp
        try:
            return int(body[0]) if body else 0
        except (TypeError, ValueError):
            return 0
    if cmd[0] == "FT.PROFILE" and isinstance(resp[0], list) and resp[0]:
        try:
            return int(resp[0][0])
        except (TypeError, ValueError):
            return 0
    return 0


def worker(
    redis_url: str,
    socket_timeout: float,
    schema: Schema,
    vocab: Vocabulary,
    stats: Stats,
    stop_event: threading.Event,
    seed: int,
) -> None:
    rnd = random.Random(seed)
    r = redis.Redis.from_url(redis_url, decode_responses=True, socket_timeout=socket_timeout, socket_connect_timeout=8)
    while not stop_event.is_set():
        maker = rnd.choices(
            [make_search, make_aggregate, make_profile],
            weights=[60, 30, 10],
            k=1,
        )[0]
        cmd = maker(schema, vocab, rnd)
        op = op_name(cmd)
        t0 = time.monotonic()
        ok, err = True, None
        hits = 0
        try:
            resp = r.execute_command(*cmd)
            hits = extract_hits(cmd, resp)
            has_cursor = "WITHCURSOR" in cmd
            if has_cursor and cmd[0] == "FT.AGGREGATE" and isinstance(resp, list) and len(resp) == 2 and not isinstance(resp[1], list):
                cursor = resp[1]
                for _ in range(3):
                    if not cursor:
                        break
                    nxt = r.execute_command("FT.CURSOR", "READ", INDEX_NAME, cursor, "COUNT", "50")
                    if not (isinstance(nxt, list) and len(nxt) == 2):
                        break
                    cursor = nxt[1]
                if cursor:
                    try:
                        r.execute_command("FT.CURSOR", "DEL", INDEX_NAME, cursor)
                    except redis.exceptions.ResponseError:
                        pass
        except Exception as exc:  # noqa: BLE001
            ok = False
            err = f"{type(exc).__name__}: {exc}"
        stats.add(op, (time.monotonic() - t0) * 1000.0, ok, hits, err)


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, int(round((p / 100.0) * (len(ordered) - 1)))))
    return ordered[idx]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run complex RedisSearch stress workload against idx:books.")
    parser.add_argument("--redis", required=True, dest="redis_url", help="Redis URL, e.g. redis://host:6379")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--duration", type=int, default=60, help="Workload duration in seconds")
    parser.add_argument("--sample-docs", type=int, default=300, help="Docs sampled for vocabulary")
    parser.add_argument("--socket-timeout", type=float, default=15.0)
    parser.add_argument("--status-interval", type=int, default=5)
    parser.add_argument("--report-file", default="stress-report.json")
    parser.add_argument("--seed", type=int, default=1337)
    args = parser.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True, socket_timeout=args.socket_timeout, socket_connect_timeout=8)
    if not r.ping():
        raise RuntimeError("PING failed")

    schema = load_schema(r)
    if not schema.text_fields or not schema.tag_fields:
        raise RuntimeError("Schema missing TEXT or TAG fields; cannot build complex query mix")
    vocab = build_vocab(r, schema, args.sample_docs)

    stats = Stats()
    stop_event = threading.Event()
    threads = [
        threading.Thread(
            target=worker,
            args=(args.redis_url, args.socket_timeout, schema, vocab, stats, stop_event, args.seed + i),
            daemon=True,
            name=f"stress-{i}",
        )
        for i in range(args.threads)
    ]

    start = time.monotonic()
    for t in threads:
        t.start()
    while time.monotonic() - start < args.duration:
        time.sleep(args.status_interval)
        elapsed = time.monotonic() - start
        with stats.lock:
            qps = stats.total / elapsed if elapsed else 0.0
            print(
                f"[status] elapsed={elapsed:6.1f}s total={stats.total} qps={qps:7.1f} "
                f"errors={stats.errors} hits={stats.hits}"
            )

    stop_event.set()
    for t in threads:
        t.join(timeout=10)

    elapsed = time.monotonic() - start
    report = {
        "redis_url": args.redis_url,
        "index": INDEX_NAME,
        "elapsed_sec": round(elapsed, 2),
        "threads": args.threads,
        "schema": {
            "num_docs": schema.num_docs,
            "text_fields": schema.text_fields,
            "tag_fields": schema.tag_fields,
            "prefixes": schema.prefixes,
        },
        "vocabulary": {
            "sampled_keys": vocab.sampled_keys,
            "text_field_pool_sizes": {k: len(v) for k, v in vocab.text_tokens.items()},
            "tag_field_pool_sizes": {k: len(v) for k, v in vocab.tag_values.items()},
        },
        "totals": {
            "queries": stats.total,
            "errors": stats.errors,
            "error_rate": round((stats.errors / stats.total) if stats.total else 0.0, 6),
            "total_hits": stats.hits,
            "non_empty_responses": stats.non_empty_hits,
            "non_empty_response_rate": round((stats.non_empty_hits / stats.total) if stats.total else 0.0, 6),
            "qps": round(stats.total / elapsed, 2) if elapsed else 0.0,
        },
        "per_operation": {},
        "top_errors": stats.error_buckets.most_common(15),
    }

    for op, count in stats.op_counts.items():
        lat = stats.latencies.get(op, [])
        report["per_operation"][op] = {
            "count": count,
            "errors": stats.op_errors.get(op, 0),
            "p50_ms": round(percentile(lat, 50), 3),
            "p95_ms": round(percentile(lat, 95), 3),
            "p99_ms": round(percentile(lat, 99), 3),
            "max_ms": round(max(lat) if lat else 0.0, 3),
        }

    report_path = Path(args.report_file)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"[done] wrote report to {report_path.resolve()}")
    print(
        f"[done] total={report['totals']['queries']} errors={report['totals']['errors']} "
        f"qps={report['totals']['qps']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
