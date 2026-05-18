#!/usr/bin/env python3
"""
netfuzz.py — network-mode companion to redis-fuzz-search.

The in-process LibAFL fuzzer (./fuzz) links redis-server into its own binary
and cannot be pointed at a remote Redis. This script mirrors the fuzzer's
generation strategy (same seeded `idx` index, same FT.* command vocabulary,
same biased argument names) but sends the traffic over the wire to *any*
Redis endpoint, including non-default ports, Redis Cloud / Enterprise / Flex.

Trade-off vs. ./fuzz:
  - No coverage feedback. This is blind stress with smart inputs, not
    coverage-guided fuzzing.
  - Pro: works against managed/remote Redis where you can't link the server.

Usage:
  pip3 install redis
  python3 netfuzz.py --redis redis://host:6379
  python3 netfuzz.py --redis redis://user:pass@host:11000/0 --threads 8 --duration 600
  python3 netfuzz.py --redis redis://host:6379 --no-seed --error-log /tmp/qa_errors.log
"""

from __future__ import annotations

import argparse
import os
import random
import signal
import string
import struct
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Tuple

import redis  # pip install redis

# ---------------------------------------------------------------------------
# Vocabulary — mirrors src/smith.rs (search_arg_override, gen_string_for,
# gen_key_for) and src/harness.c (harness_seed_search).
# ---------------------------------------------------------------------------

INDEX_NAME = "idx"
DOC_PREFIX = "doc:"
SUG_KEY = "sug"
DICT_KEY = "dict"
SYN_GROUP = "g1"

DOC_KEYS = [f"{DOC_PREFIX}{i}" for i in (1, 2, 3)]
FIELDS = ["title", "body", "n", "t", "loc", "v"]

QUERY_PRESETS = [
    "*",
    "hello",
    "@title:hello",
    "@body:demo",
    "@n:[0 100]",
    "@t:{tag1}",
    "hello | world",
    "(@title:foo) (@n:[0 50])",
    "@v:[VECTOR_RANGE 0.5 $vec]",
    "foo bar baz",
    "@title:(hello world)",
    "-@t:{tag2}",
    "@n:[-inf +inf]",
    "%hello%",         # fuzzy
    "hel*",            # prefix
    "*ell*",           # infix (requires WITHSUFFIXTRIE; will error otherwise)
]

VOCAB = [
    "hello", "world", "demo", "document", "foo", "bar", "baz",
    "title", "body", "n", "t", "loc", "v",
    "tag1", "tag2", "blue", "red",
    "TEXT", "TAG", "NUMERIC", "GEO", "VECTOR", "SORTABLE", "NOSTEM",
    "ON", "HASH", "JSON", "PREFIX", "SCHEMA", "LANGUAGE", "SCORE",
    "english", "french", "german",
]

PUNCT = "*?[]{}()^$|.+-_,;:/@!%~#="


# ---------------------------------------------------------------------------
# Counters / error logging
# ---------------------------------------------------------------------------

@dataclass
class Counters:
    sent: int = 0
    ok: int = 0
    err: int = 0
    timeouts: int = 0
    conn_errors: int = 0
    by_cmd_ok: dict = field(default_factory=dict)
    by_cmd_err: dict = field(default_factory=dict)
    last_errors: List[str] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record(self, cmd: str, exc: Optional[BaseException]) -> None:
        with self.lock:
            self.sent += 1
            if exc is None:
                self.ok += 1
                self.by_cmd_ok[cmd] = self.by_cmd_ok.get(cmd, 0) + 1
            else:
                self.err += 1
                self.by_cmd_err[cmd] = self.by_cmd_err.get(cmd, 0) + 1
                if isinstance(exc, (redis.exceptions.ConnectionError,
                                    redis.exceptions.TimeoutError)):
                    self.timeouts += isinstance(exc, redis.exceptions.TimeoutError)
                    self.conn_errors += isinstance(exc, redis.exceptions.ConnectionError)
                msg = f"{cmd}: {type(exc).__name__}: {exc}"
                self.last_errors.append(msg[:400])
                if len(self.last_errors) > 32:
                    self.last_errors.pop(0)


# ---------------------------------------------------------------------------
# Random helpers
# ---------------------------------------------------------------------------

def rnd_string(min_len: int = 1, max_len: int = 32, special_prob: float = 0.30) -> str:
    n = random.randint(min_len, max_len)
    out = []
    for _ in range(n):
        if random.random() < special_prob and random.random() < 0.25:
            out.append(random.choice(PUNCT))
        else:
            out.append(random.choice(string.ascii_letters + string.digits + "_"))
    return "".join(out)


def rnd_int_interesting() -> int:
    interesting = [
        -2_147_483_648, -65536, -32768, -1024, -1, 0, 1, 7, 16, 32, 63, 64,
        127, 128, 255, 256, 511, 512, 1023, 1024, 32767, 32768, 65535, 65536,
        2_147_483_647,
    ]
    if random.random() < 0.6:
        return random.choice(interesting)
    return random.randint(-10000, 10000)


def rnd_double() -> str:
    if random.random() < 0.55:
        return str(rnd_int_interesting())
    if random.random() < 0.05:
        return random.choice(["inf", "-inf", "nan"])
    return f"{random.choice(['-', ''])}{random.randint(0, 999999)}.{random.randint(0, 999999)}"


def rnd_token() -> str:
    """Pick a token: usually from the seeded vocab, sometimes random."""
    if random.random() < 0.65:
        return random.choice(VOCAB)
    return rnd_string()


def rnd_field() -> str:
    return random.choice(FIELDS)


def rnd_query() -> str:
    if random.random() < 0.85:
        return random.choice(QUERY_PRESETS)
    return rnd_string(1, 64, special_prob=0.5)


def rnd_vec16() -> bytes:
    """Random 4-dim FLOAT32 vector — matches the index's DIM 4 when seeded."""
    return struct.pack("<4f", *(random.uniform(-1.0, 1.0) for _ in range(4)))


# ---------------------------------------------------------------------------
# Seed: mirrors src/harness.c harness_seed_search().
# ---------------------------------------------------------------------------

def seed_search(r: redis.Redis, index: str = INDEX_NAME, prefix: str = DOC_PREFIX,
                with_vector: bool = True) -> None:
    """(Re)create the canonical index + sample docs + sug/dict/syn.

    Drops the index if it exists, then recreates with TEXT/NUMERIC/TAG/GEO
    (and optionally VECTOR). Idempotent — safe to call repeatedly.
    """
    # Drop existing
    try:
        r.execute_command("FT.DROPINDEX", index, "DD")
    except redis.exceptions.ResponseError:
        pass  # didn't exist

    schema = [
        "title", "TEXT", "SORTABLE",
        "body",  "TEXT",
        "n",     "NUMERIC", "SORTABLE",
        "t",     "TAG", "SORTABLE",
        "loc",   "GEO",
    ]
    if with_vector:
        schema += ["v", "VECTOR", "FLAT", "6",
                   "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2"]

    r.execute_command(
        "FT.CREATE", index, "ON", "HASH", "PREFIX", "1", prefix,
        "SCHEMA", *schema,
    )

    # Sample docs
    for doc, payload in (
        (f"{prefix}1", {"title": "hello world", "body": "redis search demo document",
                        "n": 42, "t": "tag1,blue", "loc": "-122.4194,37.7749"}),
        (f"{prefix}2", {"title": "foo bar baz", "body": "another sample document",
                        "n": 7,  "t": "tag2,red",  "loc": "-0.1276,51.5074"}),
        (f"{prefix}3", {"title": "lorem ipsum", "body": "third doc for fuzzing",
                        "n": 99, "t": "tag1,green", "loc": "139.6917,35.6895"}),
    ):
        if with_vector:
            payload["v"] = rnd_vec16()
        r.hset(doc, mapping=payload)

    # Auxiliary structures
    r.execute_command("FT.SUGADD", SUG_KEY, "hello", "1")
    r.execute_command("FT.SUGADD", SUG_KEY, "help",  "2")
    r.execute_command("FT.SUGADD", SUG_KEY, "world", "1")
    r.execute_command("FT.DICTADD", DICT_KEY, "hello", "world", "redis", "search")
    try:
        r.execute_command("FT.SYNUPDATE", index, SYN_GROUP, "hello", "hi")
    except redis.exceptions.ResponseError:
        pass


# ---------------------------------------------------------------------------
# FT.* command builders. Each returns (cmd_name, [args...]).
# ---------------------------------------------------------------------------

def cmd_search() -> Tuple[str, list]:
    args = [INDEX_NAME, rnd_query()]
    if random.random() < 0.3: args += ["NOCONTENT"]
    if random.random() < 0.2: args += ["LIMIT", "0", str(random.randint(0, 50))]
    if random.random() < 0.2: args += ["RETURN", "1", rnd_field()]
    if random.random() < 0.15: args += ["SORTBY", rnd_field(),
                                        random.choice(["ASC", "DESC"])]
    if random.random() < 0.1:  args += ["DIALECT", str(random.choice([2, 3, 4]))]
    if random.random() < 0.05: args += ["TIMEOUT", str(random.randint(1, 500))]
    return "FT.SEARCH", args


def cmd_aggregate() -> Tuple[str, list]:
    args = [INDEX_NAME, rnd_query()]
    op = random.choice(["GROUPBY", "FILTER", "LIMIT", "SORTBY", "APPLY"])
    if op == "GROUPBY":
        args += ["GROUPBY", "1", f"@{rnd_field()}",
                 "REDUCE", "COUNT", "0", "AS", rnd_token()]
    elif op == "FILTER":
        args += ["FILTER", f"@n > {rnd_int_interesting()}"]
    elif op == "LIMIT":
        args += ["LIMIT", "0", str(random.randint(0, 100))]
    elif op == "SORTBY":
        args += ["SORTBY", "2", f"@{rnd_field()}",
                 random.choice(["ASC", "DESC"])]
    else:
        args += ["APPLY", f"@n * 2", "AS", "doubled"]
    if random.random() < 0.2: args += ["DIALECT", str(random.choice([2, 3]))]
    return "FT.AGGREGATE", args


def cmd_explain() -> Tuple[str, list]:
    return random.choice(["FT.EXPLAIN", "FT.EXPLAINCLI"]), \
           [INDEX_NAME, rnd_query()]


def cmd_profile() -> Tuple[str, list]:
    sub = random.choice(["SEARCH", "AGGREGATE"])
    inner = rnd_query() if sub == "SEARCH" else rnd_query()
    return "FT.PROFILE", [INDEX_NAME, sub, "QUERY", inner]


def cmd_info() -> Tuple[str, list]:
    return "FT.INFO", [INDEX_NAME]


def cmd_alter() -> Tuple[str, list]:
    new_field = rnd_string(3, 8)
    typ = random.choice([("TEXT",), ("NUMERIC",), ("TAG",),
                         ("TAG", "SEPARATOR", ",")])
    return "FT.ALTER", [INDEX_NAME, "SCHEMA", "ADD", new_field, *typ]


def cmd_tagvals() -> Tuple[str, list]:
    return "FT.TAGVALS", [INDEX_NAME, "t"]


def cmd_sugadd() -> Tuple[str, list]:
    return "FT.SUGADD", [SUG_KEY, rnd_token(), str(random.randint(1, 10))]


def cmd_sugget() -> Tuple[str, list]:
    args = [SUG_KEY, rnd_token()[:4]]
    if random.random() < 0.5: args += ["FUZZY"]
    if random.random() < 0.4: args += ["MAX", str(random.randint(1, 20))]
    if random.random() < 0.3: args += ["WITHSCORES"]
    return "FT.SUGGET", args


def cmd_sugdel() -> Tuple[str, list]:
    return "FT.SUGDEL", [SUG_KEY, rnd_token()]


def cmd_suglen() -> Tuple[str, list]:
    return "FT.SUGLEN", [SUG_KEY]


def cmd_dictadd() -> Tuple[str, list]:
    return "FT.DICTADD", [DICT_KEY] + [rnd_token() for _ in range(random.randint(1, 5))]


def cmd_dictdel() -> Tuple[str, list]:
    return "FT.DICTDEL", [DICT_KEY, rnd_token()]


def cmd_dictdump() -> Tuple[str, list]:
    return "FT.DICTDUMP", [DICT_KEY]


def cmd_spellcheck() -> Tuple[str, list]:
    return "FT.SPELLCHECK", [INDEX_NAME, rnd_query()]


def cmd_synupdate() -> Tuple[str, list]:
    args = [INDEX_NAME, SYN_GROUP] + [rnd_token() for _ in range(random.randint(1, 4))]
    return "FT.SYNUPDATE", args


def cmd_syndump() -> Tuple[str, list]:
    return "FT.SYNDUMP", [INDEX_NAME]


def cmd_hset_doc() -> Tuple[str, list]:
    key = random.choice(DOC_KEYS)
    fields = []
    for f in random.sample(FIELDS[:-1], k=random.randint(1, 4)):  # skip vector
        if f == "n":
            fields += [f, str(rnd_int_interesting())]
        elif f == "loc":
            fields += [f, f"{random.uniform(-180, 180):.4f},{random.uniform(-85, 85):.4f}"]
        elif f == "t":
            fields += [f, ",".join(rnd_token() for _ in range(random.randint(1, 3)))]
        else:
            fields += [f, " ".join(rnd_token() for _ in range(random.randint(1, 5)))]
    return "HSET", [key, *fields]


def cmd_del_doc() -> Tuple[str, list]:
    return "DEL", [random.choice(DOC_KEYS)]


# Weight table: heavier on FT.SEARCH / FT.AGGREGATE (matches defconfig.json).
GENERATORS: List[Tuple[Callable[[], Tuple[str, list]], float]] = [
    (cmd_search,     4.0),
    (cmd_aggregate,  3.0),
    (cmd_explain,    1.5),
    (cmd_profile,    2.0),
    (cmd_info,       0.5),
    (cmd_alter,      0.5),
    (cmd_tagvals,    0.5),
    (cmd_sugadd,     1.0),
    (cmd_sugget,     1.5),
    (cmd_sugdel,     0.5),
    (cmd_suglen,     0.3),
    (cmd_dictadd,    0.5),
    (cmd_dictdel,    0.3),
    (cmd_dictdump,   0.3),
    (cmd_spellcheck, 1.0),
    (cmd_synupdate,  0.5),
    (cmd_syndump,    0.3),
    (cmd_hset_doc,   1.5),
    (cmd_del_doc,    0.3),
]


def pick_command() -> Tuple[str, list]:
    fns, weights = zip(*GENERATORS)
    fn = random.choices(fns, weights=weights, k=1)[0]
    return fn()


# ---------------------------------------------------------------------------
# Worker / driver
# ---------------------------------------------------------------------------

def worker(pool: redis.ConnectionPool, counters: Counters,
           stop: threading.Event, error_log: Optional[object]) -> None:
    r = redis.Redis(connection_pool=pool)
    while not stop.is_set():
        cmd, args = pick_command()
        exc: Optional[BaseException] = None
        try:
            r.execute_command(cmd, *args)
        except redis.exceptions.ResponseError as e:
            exc = e  # expected error class — most fuzz output goes here
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            exc = e
            time.sleep(0.05)
        except Exception as e:  # noqa: BLE001 — capture anything unexpected
            exc = e
        counters.record(cmd, exc)
        if exc is not None and error_log is not None:
            try:
                error_log.write(f"{time.time():.3f}\t{cmd} {' '.join(map(str, args))[:240]}\t"
                                f"{type(exc).__name__}: {exc}\n")
            except Exception:
                pass


def status_loop(counters: Counters, stop: threading.Event, started: float) -> None:
    last_sent = 0
    last_t = started
    while not stop.is_set():
        time.sleep(1.0)
        now = time.time()
        with counters.lock:
            sent = counters.sent
            ok = counters.ok
            err = counters.err
            ce = counters.conn_errors
            to = counters.timeouts
        ops = (sent - last_sent) / max(now - last_t, 1e-3)
        last_sent, last_t = sent, now
        sys.stdout.write(
            f"\r[{int(now - started):>5}s] sent={sent:<8} ok={ok:<8} "
            f"err={err:<7} conn={ce} timeout={to} ops/s={ops:7.0f}   "
        )
        sys.stdout.flush()
    sys.stdout.write("\n")


def main() -> int:
    p = argparse.ArgumentParser(
        description="Network-mode FT.* fuzz/stress driver.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__)
    p.add_argument("--redis",
                   default=os.environ.get("REDIS_URL", "redis://localhost:6379"),
                   help="Redis URL (e.g. redis://host:6379, "
                        "redis://user:pass@host:11000/0). Default: %(default)s")
    p.add_argument("--threads", type=int, default=8,
                   help="Concurrent client threads (default: %(default)s)")
    p.add_argument("--duration", type=float, default=60.0,
                   help="Run duration in seconds, 0 = forever (default: %(default)s)")
    p.add_argument("--max-connections", type=int, default=None,
                   help="Connection pool size (default: 2x threads)")
    p.add_argument("--no-seed", action="store_true",
                   help="Skip seeding the index / docs (assume already present)")
    p.add_argument("--no-vector", action="store_true",
                   help="Omit VECTOR field when seeding (smaller schema)")
    p.add_argument("--error-log", type=str, default=None,
                   help="Append per-error lines to this path")
    p.add_argument("--socket-timeout", type=float, default=5.0,
                   help="Per-command timeout in seconds (default: %(default)s)")
    p.add_argument("--seed-only", action="store_true",
                   help="Seed the index and exit without sending fuzz traffic")
    args = p.parse_args()

    # Connect
    pool = redis.ConnectionPool.from_url(
        args.redis,
        max_connections=args.max_connections or max(args.threads * 2, 16),
        socket_timeout=args.socket_timeout,
        socket_connect_timeout=args.socket_timeout,
        decode_responses=False,
    )
    r = redis.Redis(connection_pool=pool)
    try:
        r.ping()
    except Exception as e:
        print(f"FATAL: cannot connect to {args.redis}: {e}", file=sys.stderr)
        return 1
    print(f"Connected to {args.redis}")

    # Seed
    if not args.no_seed:
        print(f"Seeding index `{INDEX_NAME}` (prefix `{DOC_PREFIX}*`)...")
        try:
            seed_search(r, with_vector=not args.no_vector)
            print("  seeded: idx + 3 docs + sug + dict + syn")
        except Exception as e:
            print(f"  seed failed: {type(e).__name__}: {e}", file=sys.stderr)
            print("  continuing anyway — pass --no-seed to suppress", file=sys.stderr)
    if args.seed_only:
        return 0

    # Error log
    error_log = open(args.error_log, "a") if args.error_log else None
    if error_log:
        error_log.write(f"--- netfuzz start {time.time():.3f} {args.redis} ---\n")
        error_log.flush()

    # Drive
    counters = Counters()
    stop = threading.Event()

    def handle_signal(signum, _frame):
        print(f"\nReceived signal {signum}, shutting down...")
        stop.set()
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    started = time.time()
    status_thread = threading.Thread(target=status_loop,
                                     args=(counters, stop, started), daemon=True)
    status_thread.start()

    workers = [threading.Thread(target=worker,
                                args=(pool, counters, stop, error_log),
                                daemon=True)
               for _ in range(args.threads)]
    for t in workers:
        t.start()

    try:
        if args.duration > 0:
            stop.wait(args.duration)
        else:
            while not stop.is_set():
                stop.wait(60.0)
    finally:
        stop.set()
        for t in workers:
            t.join(timeout=5.0)
        status_thread.join(timeout=2.0)
        if error_log:
            error_log.write(f"--- netfuzz end   {time.time():.3f} "
                            f"sent={counters.sent} ok={counters.ok} "
                            f"err={counters.err} ---\n")
            error_log.close()

    # Final summary
    elapsed = time.time() - started
    print(f"\nDone in {elapsed:.1f}s — sent={counters.sent} ok={counters.ok} "
          f"err={counters.err} (avg {counters.sent / max(elapsed, 1e-3):.0f} ops/s)")
    print("\nTop OK commands:")
    for cmd, n in sorted(counters.by_cmd_ok.items(), key=lambda x: -x[1])[:10]:
        print(f"  {cmd:<24} {n}")
    print("\nTop ERR commands:")
    for cmd, n in sorted(counters.by_cmd_err.items(), key=lambda x: -x[1])[:10]:
        print(f"  {cmd:<24} {n}")
    if counters.last_errors:
        print("\nLast errors (up to 10):")
        for line in counters.last_errors[-10:]:
            print(f"  {line}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
