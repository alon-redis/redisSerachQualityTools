import argparse
import collections
import os
import random
import threading
import time
from datetime import datetime

import redis


INDEX_NAME = "idx:books"

# ---------------------------------------------------------------------------
# Closed vocabularies mirrored from bookHashPopulatorOnDisk.py so the simple
# workload picks values that actually exist in the indexed data.
# ---------------------------------------------------------------------------
EDITIONS = [
    "english", "spanish", "french", "german", "italian", "chinese",
    "japanese", "russian", "arabic", "portuguese", "korean", "dutch",
    "swedish", "norwegian", "danish", "finnish", "polish", "turkish",
    "hindi", "urdu", "greek", "hebrew", "thai", "vietnamese",
    "indonesian", "hungarian", "czech", "slovak", "romanian",
    "bulgarian", "ukrainian", "serbian", "croatian", "slovenian", "latvian",
]
GENRES = [
    "comics (superheroes)", "fiction", "non-fiction", "science fiction",
    "fantasy", "mystery", "romance", "history", "horror", "biography",
    "thriller", "self-help", "poetry", "cookbooks", "memoir",
    "young adult", "children's literature", "drama", "travel", "science",
    "art", "philosophy", "psychology", "religion", "true crime",
    "graphic novel", "adventure", "political", "health", "humor",
]
INVENTORY_STATUSES = ["available", "maintenance", "on_loan", "for_sale"]
FORMATS = ["hardcover", "paperback", "ebook"]
IS_AVAILABLE_VALUES = ["true", "false"]

# Verification anchor that the populator always writes (book_id=0).
VERIFICATION_AUTHOR_TOKEN = "Shmuely"
VERIFICATION_TITLE_TOKEN = "QA"

# Populator-side key shape (must stay in sync with bookHashPopulatorOnDisk.py).
# REDIS_KEY_BASE = "alon:shmuely:redis:data:store:application"
# make_key(book_id) -> f"{REDIS_KEY_BASE}:{book_id}"
POPULATOR_KEY_PREFIX = "alon:shmuely:redis:data:store:application:"
VERIFICATION_KEY = POPULATOR_KEY_PREFIX + "0"


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------
COUNTERS = {
    "queries_total": 0,
    "queries_errors": 0,
    "queries_zero_results": 0,
    "docs_returned": 0,
    # DEL workload counters. del_attempts == sum of all the other del_* below
    # by construction, so the breakdown is exhaustive.
    "del_attempts": 0,
    "del_actually_deleted": 0,
    "del_missed": 0,
    "del_skipped_verification": 0,
    "del_skipped_other_prefix": 0,
    "del_empty_keyspace": 0,
    "del_errors": 0,
}
COUNTERS_LOCK = threading.Lock()

# Per-category counters: { category_name -> {total, errors, zero, docs} }.
# Pre-registered in main() based on workload so the live line has a stable
# column ordering.
CATEGORY_COUNTERS = collections.OrderedDict()
CATEGORY_COUNTERS_LOCK = threading.Lock()


def increment_counter(name, amount=1):
    with COUNTERS_LOCK:
        COUNTERS[name] += amount


def get_counters_snapshot():
    with COUNTERS_LOCK:
        return dict(COUNTERS)


def get_category_counters_snapshot():
    with CATEGORY_COUNTERS_LOCK:
        return collections.OrderedDict(
            (name, dict(c)) for name, c in CATEGORY_COUNTERS.items()
        )


def record_query_outcome(category, success, docs_returned=0, zero=False):
    """Update both global and per-category counters for a single query."""
    with COUNTERS_LOCK:
        COUNTERS["queries_total"] += 1
        if not success:
            COUNTERS["queries_errors"] += 1
        if docs_returned:
            COUNTERS["docs_returned"] += docs_returned
        if zero:
            COUNTERS["queries_zero_results"] += 1
    with CATEGORY_COUNTERS_LOCK:
        c = CATEGORY_COUNTERS.get(category)
        if c is None:
            c = {"total": 0, "errors": 0, "zero": 0, "docs": 0}
            CATEGORY_COUNTERS[category] = c
        c["total"] += 1
        if not success:
            c["errors"] += 1
        if docs_returned:
            c["docs"] += docs_returned
        if zero:
            c["zero"] += 1


# ---------------------------------------------------------------------------
# Recent-errors ring buffer (last N error reproducers, periodically flushed
# to disk so the file always holds the latest N).
# ---------------------------------------------------------------------------
class _ErrorLog:
    def __init__(self, capacity, path):
        self.capacity = max(1, capacity)
        self.path = path
        self.entries = collections.deque(maxlen=self.capacity)
        self.lock = threading.Lock()
        self.dirty = False

    def record(self, category, qstr, extras, limit, err):
        ts = datetime.now().isoformat(sep=" ", timespec="milliseconds")
        err_msg = (
            f"{type(err).__name__}: {err}" if isinstance(err, BaseException) else str(err)
        )
        cmd_args = [
            "FT.SEARCH", INDEX_NAME, qstr,
            "NOCONTENT", "LIMIT", "0", str(limit),
            *extras,
        ]
        # redis-cli MONITOR-style line: each arg double-quoted, easy to paste.
        cmd_str = " ".join(f"\"{a}\"" for a in cmd_args)
        with self.lock:
            self.entries.append((ts, category, cmd_str, err_msg))
            self.dirty = True

    def flush(self):
        with self.lock:
            if not self.dirty:
                return
            snapshot = list(self.entries)
            self.dirty = False
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
            with open(self.path, "w") as f:
                f.write(
                    f"# Last {len(snapshot)} errors (oldest first, newest last). "
                    f"Capacity: {self.capacity}.\n\n"
                )
                for ts, cat, cmd, err in snapshot:
                    f.write(f"[{ts}] category={cat}\n")
                    f.write(f"  cmd: {cmd}\n")
                    f.write(f"  err: {err}\n\n")
        except OSError:
            # Best-effort: never let logging crash a worker.
            pass


# Module-level singleton wired up in main(). None disables logging.
ERROR_LOG = None


def maybe_record_error(category, qstr, extras, limit, err):
    if ERROR_LOG is not None:
        ERROR_LOG.record(category, qstr, extras, limit, err)


def error_log_flusher(stop_event, interval=2.0):
    while not stop_event.is_set():
        time.sleep(interval)
        if ERROR_LOG is not None:
            ERROR_LOG.flush()


# ---------------------------------------------------------------------------
# TAG-value escaping
# ---------------------------------------------------------------------------
_TAG_SPECIAL_CHARS = set([
    " ", ",", ".", "<", ">", "{", "}", "[", "]", '"', "'", ":",
    ";", "!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "-",
    "+", "=", "~", "|", "/", "\\", "?",
])


def escape_tag_value(value):
    out = []
    for ch in value:
        if ch in _TAG_SPECIAL_CHARS:
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


# ---------------------------------------------------------------------------
# Simple workload (single-clause queries; one category per template)
# ---------------------------------------------------------------------------
def _q_catch_all():
    return "*"


def _q_format():
    return f"@format:{{{escape_tag_value(random.choice(FORMATS))}}}"


def _q_is_available():
    return f"@is_available:{{{escape_tag_value(random.choice(IS_AVAILABLE_VALUES))}}}"


def _q_status():
    return f"@status:{{{escape_tag_value(random.choice(INVENTORY_STATUSES))}}}"


def _q_genres():
    return f"@genres:{{{escape_tag_value(random.choice(GENRES))}}}"


def _q_editions():
    return f"@editions:{{{escape_tag_value(random.choice(EDITIONS))}}}"


def _q_author_anchor():
    return f"@author:{VERIFICATION_AUTHOR_TOKEN}"


def _q_title_anchor():
    return f"@title:{VERIFICATION_TITLE_TOKEN}"


SIMPLE_QUERY_BUILDERS = [
    ("catch_all", _q_catch_all),
    ("format", _q_format),
    ("is_available", _q_is_available),
    ("status", _q_status),
    ("genres", _q_genres),
    ("editions", _q_editions),
    ("author_anchor", _q_author_anchor),
    ("title_anchor", _q_title_anchor),
]
SIMPLE_CATEGORY_NAMES = [name for name, _ in SIMPLE_QUERY_BUILDERS]


def build_simple_query():
    """Return (query_string, extra_args, category_name)."""
    name, fn = random.choice(SIMPLE_QUERY_BUILDERS)
    return fn(), [], name


# ---------------------------------------------------------------------------
# Advanced workload
# ---------------------------------------------------------------------------
# Built from 4 categories, picked uniformly per request:
#   1) boolean  - 2-5 mixed clauses, AND/OR/NOT, optional grouping
#   2) text_ops - 1-3 TEXT-operator clauses (plain/prefix/fuzzy/phrase/contains)
#   3) in_list  - pseudo-range IN-list on numeric-as-TAG fields
#   4) dialect2 - DIALECT 2-only patterns: TAG wildcard or PARAMS substitution
# All advanced queries are dispatched with DIALECT 2.

NUMERIC_TAG_RANGES = {
    "year_published": (1900, 2023),
    "chapter_count": (5, 50),
    "pages": (50, 1500),
    "edition_number": (1, 10),
    "review_count": (0, 5000),
    "citation_count": (0, 1000),
    "publishing_delay": (-356, 1000),
    "word_count": (10000, 150000),
    "reading_time_minutes": (30, 1200),
    "global_sales": (1000, 1000000),
    "translations_count": (1, 50),
    "author_age_at_publication": (20, 80),
    "weight_grams": (-100, 2000),
    "rating_votes": (1, 1000),
}

TEXT_FIELDS = [
    "author", "description", "title", "publisher",
    "book_series", "main_character", "location", "address",
]

# ASCII-only token pool, stopwords excluded so they don't inflate zero-result
# counts (RediSearch drops stopwords at index time).
TEXT_TOKEN_POOL = [
    "new", "old", "book", "year", "house", "world", "story", "city",
    "time", "life", "man", "woman", "john", "david", "smith", "james",
    "mary", "river", "street", "company", "group", "system",
    "shmuely", "qa", "architect",
]

ADVANCED_CATEGORY_NAMES = ["boolean", "text_ops", "in_list", "dialect2"]
ADVANCED_MAX_CLAUSES = 5
ADVANCED_MAX_IN_LIST = 50
ADVANCED_NOT_PROBABILITY = 0.2
ADVANCED_GROUP_PROBABILITY = 0.5


def _adv_single_tag_clause():
    bucket = random.choice(["format", "is_available", "status", "genres", "editions"])
    if bucket == "format":
        return f"@format:{{{escape_tag_value(random.choice(FORMATS))}}}"
    if bucket == "is_available":
        return f"@is_available:{{{escape_tag_value(random.choice(IS_AVAILABLE_VALUES))}}}"
    if bucket == "status":
        return f"@status:{{{escape_tag_value(random.choice(INVENTORY_STATUSES))}}}"
    if bucket == "genres":
        return f"@genres:{{{escape_tag_value(random.choice(GENRES))}}}"
    return f"@editions:{{{escape_tag_value(random.choice(EDITIONS))}}}"


def _adv_tag_or_clause(max_values=8):
    bucket = random.choice(["format", "status", "genres", "editions"])
    pool = {
        "format": FORMATS,
        "status": INVENTORY_STATUSES,
        "genres": GENRES,
        "editions": EDITIONS,
    }[bucket]
    k = random.randint(2, min(max_values, len(pool)))
    chosen = random.sample(pool, k)
    return f"@{bucket}:{{{'|'.join(escape_tag_value(v) for v in chosen)}}}"


def _adv_in_list_clause(max_values=ADVANCED_MAX_IN_LIST):
    field = random.choice(list(NUMERIC_TAG_RANGES.keys()))
    lo, hi = NUMERIC_TAG_RANGES[field]
    domain = hi - lo + 1
    n = random.randint(1, min(max_values, domain))
    start = random.randint(lo, hi - n + 1)
    values = [str(v) for v in range(start, start + n)]
    return f"@{field}:{{{'|'.join(values)}}}"


def _adv_text_clause():
    field = random.choice(TEXT_FIELDS)
    token = random.choice(TEXT_TOKEN_POOL)
    op = random.choice(["plain", "prefix", "fuzzy", "phrase", "contains"])
    if op == "plain":
        return f"@{field}:{token}"
    if op == "prefix":
        return f"@{field}:{token}*"
    if op == "fuzzy":
        return f"@{field}:%{token}%"
    if op == "phrase":
        token2 = random.choice(TEXT_TOKEN_POOL)
        return f'@{field}:"{token} {token2}"'
    return f"@{field}:*{token}*"  # contains (DIALECT 2)


def _maybe_negate(clause):
    if clause.startswith("-"):
        return clause
    if random.random() < ADVANCED_NOT_PROBABILITY:
        return f"-{clause}"
    return clause


def _adv_boolean_query():
    n = random.randint(2, ADVANCED_MAX_CLAUSES)
    clauses = []
    for _ in range(n):
        kind = random.choice(["tag", "tag_or", "text", "in_list"])
        if kind == "tag":
            clause = _adv_single_tag_clause()
        elif kind == "tag_or":
            clause = _adv_tag_or_clause()
        elif kind == "text":
            clause = _adv_text_clause()
        else:
            clause = _adv_in_list_clause()
        clauses.append(_maybe_negate(clause))
    if len(clauses) >= 3 and random.random() < ADVANCED_GROUP_PROBABILITY:
        i = random.randint(0, len(clauses) - 2)
        clauses[i:i + 2] = [f"({clauses[i]} {clauses[i + 1]})"]
    return " ".join(clauses)


def _adv_text_query():
    n = random.randint(1, 3)
    return " ".join(_adv_text_clause() for _ in range(n))


def _adv_range_query():
    q = _adv_in_list_clause()
    if random.random() < 0.5:
        q = f"{q} {_adv_single_tag_clause()}"
    return q


def _adv_dialect2_query():
    """Return (query_string, extras_excluding_DIALECT)."""
    pattern = random.choice(["tag_wildcard", "params"])
    if pattern == "tag_wildcard":
        word = random.choice(EDITIONS)
        prefix_len = random.randint(2, min(4, len(word)))
        prefix = word[:prefix_len]
        return f"@editions:{{w'{prefix}*'}}", []
    simple_editions = [e for e in EDITIONS if " " not in e]
    simple_genres = [g for g in GENRES if " " not in g and "(" not in g and "'" not in g]
    lang = random.choice(simple_editions)
    genre = random.choice(simple_genres)
    qstr = "@editions:{$lang} @genres:{$g}"
    extras = ["PARAMS", "4", "lang", lang, "g", genre]
    return qstr, extras


def build_advanced_query():
    """Pick uniformly across the 4 advanced categories. Always sets DIALECT 2.

    Returns (query_string, extra_args, category_name).
    """
    cat = random.randint(1, 4)
    if cat == 1:
        qstr, extras, name = _adv_boolean_query(), [], "boolean"
    elif cat == 2:
        qstr, extras, name = _adv_text_query(), [], "text_ops"
    elif cat == 3:
        qstr, extras, name = _adv_range_query(), [], "in_list"
    else:
        q, extras = _adv_dialect2_query()
        qstr, name = q, "dialect2"
    extras = extras + ["DIALECT", "2"]
    return qstr, extras, name


# ---------------------------------------------------------------------------
# Worker / status / stopper
# ---------------------------------------------------------------------------
def parse_ft_search_response_nocontent(resp):
    """Layout when NOCONTENT is set: [total_in_index, doc_id_1, doc_id_2, ...]."""
    if not isinstance(resp, list) or not resp:
        return 0, 0
    total = resp[0] if isinstance(resp[0], int) else 0
    docs_returned = max(0, len(resp) - 1)
    return total, docs_returned


def run_worker(connection_pool, build_query_fn, pipeline_depth, limit, stop_event):
    r = redis.Redis(connection_pool=connection_pool)

    while not stop_event.is_set():
        # Each batch entry is (query_string, extras, category).
        batch_queries = [build_query_fn() for _ in range(pipeline_depth)]

        if pipeline_depth == 1:
            qstr, extras, category = batch_queries[0]
            try:
                # Flex/disk index requires NOCONTENT (or RETURN 0); otherwise
                # the server returns SEARCH_FLEX_SEARCH_NOCONTENT_OR_RETURN_0_REQUIRED.
                resp = r.execute_command(
                    "FT.SEARCH", INDEX_NAME, qstr,
                    "NOCONTENT", "LIMIT", "0", str(limit),
                    *extras,
                )
                _, docs_returned = parse_ft_search_response_nocontent(resp)
                record_query_outcome(
                    category, success=True,
                    docs_returned=docs_returned, zero=(docs_returned == 0),
                )
            except (
                redis.exceptions.ResponseError,
                redis.exceptions.ConnectionError,
                redis.exceptions.TimeoutError,
            ) as e:
                record_query_outcome(category, success=False)
                maybe_record_error(category, qstr, extras, limit, e)
            continue

        try:
            pipe = r.pipeline(transaction=False)
            for qstr, extras, _category in batch_queries:
                pipe.execute_command(
                    "FT.SEARCH", INDEX_NAME, qstr,
                    "NOCONTENT", "LIMIT", "0", str(limit),
                    *extras,
                )
            results = pipe.execute(raise_on_error=False)
        except (
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
        ) as e:
            for qstr, extras, category in batch_queries:
                record_query_outcome(category, success=False)
                maybe_record_error(category, qstr, extras, limit, e)
            continue

        for (qstr, extras, category), resp in zip(batch_queries, results):
            if isinstance(resp, Exception):
                record_query_outcome(category, success=False)
                maybe_record_error(category, qstr, extras, limit, resp)
                continue
            _, docs_returned = parse_ft_search_response_nocontent(resp)
            record_query_outcome(
                category, success=True,
                docs_returned=docs_returned, zero=(docs_returned == 0),
            )


# ---------------------------------------------------------------------------
# Background DEL workload (RANDOMKEY + DEL)
# ---------------------------------------------------------------------------
# Runs in its own thread on a *dedicated* connection pool so it never competes
# with FT.SEARCH workers for connections or pool slots. Two pacing modes:
#   - "rate"  : sleep-to-rate, target N deletes/second (--del-rate)
#   - "burst" : every S seconds, fire N deletes back-to-back (--del-burst,
#               --del-burst-interval)
# Hard safety:
#   - VERIFICATION_KEY (book_id 0) is never deleted.
#   - When --del-only-prefix is set (default), keys not under POPULATOR_KEY_PREFIX
#     are skipped (RANDOMKEY scans the *entire* keyspace).

def _attempt_del_once(r, only_prefix):
    """One RANDOMKEY[+DEL] iteration. Updates counters; never raises."""
    increment_counter("del_attempts")
    try:
        key = r.randomkey()
    except (
        redis.exceptions.ResponseError,
        redis.exceptions.ConnectionError,
        redis.exceptions.TimeoutError,
    ):
        increment_counter("del_errors")
        return
    if key is None:
        increment_counter("del_empty_keyspace")
        return
    if key == VERIFICATION_KEY:
        increment_counter("del_skipped_verification")
        return
    if only_prefix and not key.startswith(POPULATOR_KEY_PREFIX):
        increment_counter("del_skipped_other_prefix")
        return
    try:
        deleted = r.delete(key)
        if deleted:
            increment_counter("del_actually_deleted")
        else:
            increment_counter("del_missed")
    except (
        redis.exceptions.ResponseError,
        redis.exceptions.ConnectionError,
        redis.exceptions.TimeoutError,
    ):
        increment_counter("del_errors")


def run_del_worker(connection_pool, mode, rate, burst, burst_interval,
                   only_prefix, stop_event):
    r = redis.Redis(connection_pool=connection_pool)

    if mode == "rate":
        if rate <= 0:
            return
        period = 1.0 / rate
        next_tick = time.monotonic()
        while not stop_event.is_set():
            _attempt_del_once(r, only_prefix)
            next_tick += period
            sleep_for = next_tick - time.monotonic()
            if sleep_for > 0:
                # stop_event.wait wakes early on shutdown, unlike time.sleep.
                if stop_event.wait(timeout=sleep_for):
                    return
            else:
                # Fell behind schedule (Redis slow / GC pause); reset to "now"
                # so we don't blast a runaway catch-up burst.
                next_tick = time.monotonic()
        return

    # burst mode
    if burst <= 0 or burst_interval <= 0:
        return
    while not stop_event.is_set():
        for _ in range(burst):
            if stop_event.is_set():
                return
            _attempt_del_once(r, only_prefix)
        if stop_event.wait(timeout=burst_interval):
            return


# ANSI clear-to-end-of-line so the live status line repaints cleanly when the
# rendered text shrinks (e.g. once 5-digit numbers drop back to 4).
_CLEAR_EOL = "\033[K"


def _format_cat_summary(cats):
    parts = []
    for name, c in cats.items():
        parts.append(f"{name}={c['total']}({c['errors']}e)")
    return " ".join(parts)


def _format_del_summary(c):
    # Compact: del=attempts ok=actually_deleted mis=missed skp=skipped_other_prefix
    #          vrf=skipped_verification emp=empty_keyspace err=errors
    return (
        f"del={c['del_attempts']} ok={c['del_actually_deleted']} "
        f"mis={c['del_missed']} skp={c['del_skipped_other_prefix']} "
        f"vrf={c['del_skipped_verification']} emp={c['del_empty_keyspace']} "
        f"err={c['del_errors']}"
    )


def print_live_status(stop_event, show_del=False):
    cats = get_category_counters_snapshot()
    initial_cat = _format_cat_summary(cats)
    initial = "\rQ:0 q/s:0 E:0 e/s:0 Z:0 D:0"
    if show_del:
        initial += " | del=0 ok=0 mis=0 skp=0 vrf=0 emp=0 err=0"
    if initial_cat:
        initial += f" | {initial_cat}"
    print(initial + _CLEAR_EOL, end="", flush=True)

    last_total = 0
    last_errors = 0
    while not stop_event.is_set():
        time.sleep(1)
        c = get_counters_snapshot()
        cats = get_category_counters_snapshot()
        qps = c["queries_total"] - last_total
        eps = c["queries_errors"] - last_errors
        last_total = c["queries_total"]
        last_errors = c["queries_errors"]
        line = (
            f"\rQ:{c['queries_total']} q/s:{qps} "
            f"E:{c['queries_errors']} e/s:{eps} "
            f"Z:{c['queries_zero_results']} D:{c['docs_returned']}"
        )
        if show_del:
            line += f" | {_format_del_summary(c)}"
        cat_str = _format_cat_summary(cats)
        if cat_str:
            line += f" | {cat_str}"
        print(line + _CLEAR_EOL, end="", flush=True)


def stopper(stop_event, duration, max_queries):
    deadline = (time.monotonic() + duration) if duration > 0 else None
    while not stop_event.is_set():
        if deadline is not None and time.monotonic() >= deadline:
            stop_event.set()
            return
        if max_queries > 0:
            with COUNTERS_LOCK:
                if COUNTERS["queries_total"] >= max_queries:
                    stop_event.set()
                    return
        time.sleep(0.1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        description=(
            "Book store search query generator (simple + advanced workloads, "
            "live metrics, per-category counters, last-N error log). "
            "Targets the index built by bookHashPopulatorOnDisk.py."
        )
    )
    arg_parser.add_argument("--redis", default="redis://localhost:6379", dest="redis_url",
                            help="Redis URL to connect to (e.g. redis://host:port).")
    arg_parser.add_argument("--max-connections", default=50, type=int, dest="max_connections",
                            help="Max Redis connections in the pool.")
    arg_parser.add_argument("--clients", default=8, type=int, dest="clients",
                            help="Number of concurrent client worker threads (closed-loop).")
    arg_parser.add_argument("--pipeline", default=1, type=int, dest="pipeline_depth",
                            help="Pipeline depth per worker (1 = no pipelining).")
    arg_parser.add_argument("--workload", choices=["simple", "advanced"], default="simple",
                            dest="workload",
                            help=("Workload type. 'simple' = single-clause queries; "
                                  "'advanced' = uniform mix of boolean / TEXT-ops / "
                                  "pseudo-range IN-list / DIALECT 2 patterns."))
    arg_parser.add_argument("--duration", default=0, type=int, dest="duration",
                            help="Run duration in seconds (0 = unlimited, stop with Ctrl+C).")
    arg_parser.add_argument("--max-queries", default=0, type=int, dest="max_queries",
                            help="Stop after this many queries (0 = unlimited).")
    arg_parser.add_argument("--limit", default=10, type=int, dest="limit",
                            help=("LIMIT count for FT.SEARCH. NOCONTENT is always sent "
                                  "because the Flex/disk index rejects content retrieval "
                                  "(SEARCH_FLEX_SEARCH_NOCONTENT_OR_RETURN_0_REQUIRED)."))
    arg_parser.add_argument("--seed", default=None, type=int, dest="seed",
                            help="Random seed for reproducibility.")

    _default_error_log = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "searchQueries",
        f"qg_last_errors_{os.getpid()}.txt",
    )
    arg_parser.add_argument("--error-log-size", default=100, type=int, dest="error_log_size",
                            help="Capacity of the recent-errors ring buffer (0 disables the log).")
    arg_parser.add_argument("--error-log-path", default=_default_error_log, dest="error_log_path",
                            help=("Path to the recent-errors log file. Overwritten every "
                                  "~2s with the latest N error reproducers (redis-cli pasteable)."))

    # --- Background DEL workload (RANDOMKEY + DEL) -------------------------
    arg_parser.add_argument("--del", action="store_true", dest="enable_del",
                            help=("Enable RANDOMKEY+DEL background workload. Runs in a "
                                  "dedicated thread on a separate connection pool so it "
                                  "never competes with FT.SEARCH workers. The verification "
                                  "key (book_id 0) is always protected."))
    arg_parser.add_argument("--del-mode", choices=["rate", "burst"], default="rate",
                            dest="del_mode",
                            help=("'rate' = sustained N deletes/sec via --del-rate; "
                                  "'burst' = N deletes every S seconds via --del-burst "
                                  "and --del-burst-interval."))
    arg_parser.add_argument("--del-rate", default=1.0, type=float, dest="del_rate",
                            help="(rate mode) Target deletes per second. Default 1.")
    arg_parser.add_argument("--del-burst", default=10, type=int, dest="del_burst",
                            help="(burst mode) Number of deletes per burst.")
    arg_parser.add_argument("--del-burst-interval", default=5.0, type=float,
                            dest="del_burst_interval",
                            help="(burst mode) Seconds between bursts.")
    arg_parser.add_argument("--del-only-prefix", action="store_true", default=True,
                            dest="del_only_prefix",
                            help=("Only delete keys with the populator's prefix "
                                  f"('{POPULATOR_KEY_PREFIX}'). Default ON; protects "
                                  "non-app keys that RANDOMKEY may return."))
    arg_parser.add_argument("--no-del-only-prefix", action="store_false",
                            dest="del_only_prefix",
                            help=("Allow DEL on any key returned by RANDOMKEY (still "
                                  "protects the verification key)."))
    arg_parser.add_argument("--del-pool-size", default=2, type=int, dest="del_pool_size",
                            help="Connection pool size for the dedicated DEL worker.")

    args = arg_parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    if args.pipeline_depth < 1:
        raise SystemExit("--pipeline must be >= 1")
    if args.clients < 1:
        raise SystemExit("--clients must be >= 1")
    if args.max_connections < args.clients:
        print(
            f"Note: --max-connections ({args.max_connections}) < --clients ({args.clients}); "
            f"workers will contend for connections."
        )

    if args.workload == "simple":
        build_query_fn = build_simple_query
        category_names = SIMPLE_CATEGORY_NAMES
    else:
        build_query_fn = build_advanced_query
        category_names = ADVANCED_CATEGORY_NAMES

    # Pre-register categories so the live line has a stable column ordering.
    for name in category_names:
        CATEGORY_COUNTERS[name] = {"total": 0, "errors": 0, "zero": 0, "docs": 0}

    if args.error_log_size > 0:
        ERROR_LOG = _ErrorLog(args.error_log_size, args.error_log_path)
        print(f"Error log: {ERROR_LOG.path} (capacity={ERROR_LOG.capacity})")

    if args.enable_del:
        if args.del_mode == "rate" and args.del_rate <= 0:
            raise SystemExit("--del-rate must be > 0 in rate mode (got "
                             f"{args.del_rate}).")
        if args.del_mode == "burst" and (args.del_burst <= 0 or args.del_burst_interval <= 0):
            raise SystemExit("--del-burst and --del-burst-interval must be > 0 "
                             "in burst mode.")

    print(
        f"Connecting to Redis at {args.redis_url} (pool={args.max_connections}), "
        f"workload={args.workload}, clients={args.clients}, pipeline={args.pipeline_depth}, "
        f"duration={args.duration}s, max_queries={args.max_queries}, limit={args.limit}"
    )
    if args.enable_del:
        if args.del_mode == "rate":
            print(
                f"DEL workload ENABLED: mode=rate, target={args.del_rate}/s, "
                f"only_prefix={args.del_only_prefix}, pool={args.del_pool_size}"
            )
        else:
            print(
                f"DEL workload ENABLED: mode=burst, {args.del_burst} deletes every "
                f"{args.del_burst_interval}s, only_prefix={args.del_only_prefix}, "
                f"pool={args.del_pool_size}"
            )

    pool = redis.ConnectionPool.from_url(
        args.redis_url, max_connections=args.max_connections, decode_responses=True
    )

    try:
        redis.Redis(connection_pool=pool).ping()
    except redis.exceptions.RedisError as e:
        raise SystemExit(f"Failed to connect to Redis: {e}")

    try:
        redis.Redis(connection_pool=pool).ft(INDEX_NAME).info()
    except redis.exceptions.ResponseError as e:
        raise SystemExit(
            f"Search index '{INDEX_NAME}' does not exist. "
            f"Run bookHashPopulatorOnDisk.py first. ({e})"
        )

    # Dedicated pool for the DEL worker so it never starves FT.SEARCH workers.
    del_pool = None
    if args.enable_del:
        del_pool = redis.ConnectionPool.from_url(
            args.redis_url, max_connections=args.del_pool_size, decode_responses=True
        )

    stop_event = threading.Event()
    status_stop_event = threading.Event()
    flusher_stop_event = threading.Event()

    workers = [
        threading.Thread(
            target=run_worker,
            args=(pool, build_query_fn, args.pipeline_depth, args.limit, stop_event),
            daemon=True,
            name=f"qg-worker-{i}",
        )
        for i in range(args.clients)
    ]
    status_thread = threading.Thread(
        target=print_live_status, args=(status_stop_event, args.enable_del),
        daemon=True, name="qg-status",
    )
    stopper_thread = threading.Thread(
        target=stopper, args=(stop_event, args.duration, args.max_queries),
        daemon=True, name="qg-stopper",
    )
    flusher_thread = threading.Thread(
        target=error_log_flusher, args=(flusher_stop_event,),
        daemon=True, name="qg-error-flusher",
    )
    del_thread = None
    if args.enable_del:
        del_thread = threading.Thread(
            target=run_del_worker,
            args=(
                del_pool, args.del_mode, args.del_rate,
                args.del_burst, args.del_burst_interval,
                args.del_only_prefix, stop_event,
            ),
            daemon=True, name="qg-del",
        )

    status_thread.start()
    stopper_thread.start()
    flusher_thread.start()
    if del_thread is not None:
        del_thread.start()
    for w in workers:
        w.start()

    try:
        for w in workers:
            w.join()
    except KeyboardInterrupt:
        print("\nInterrupted, shutting down...")
        stop_event.set()
        for w in workers:
            w.join(timeout=2)

    status_stop_event.set()
    status_thread.join(timeout=2)
    flusher_stop_event.set()
    flusher_thread.join(timeout=2)
    stopper_thread.join(timeout=2)
    if del_thread is not None:
        del_thread.join(timeout=2)

    if ERROR_LOG is not None:
        ERROR_LOG.flush()

    final = get_counters_snapshot()
    cats = get_category_counters_snapshot()
    print("\n\nRun Summary")
    print(f"Total queries:        {final['queries_total']}")
    print(f"Errors:               {final['queries_errors']}")
    print(f"Zero-result queries:  {final['queries_zero_results']}")
    print(f"Docs returned:        {final['docs_returned']}")
    if cats:
        col_w = max(len(name) for name in cats)
        print("\nPer-category:")
        print(
            f"  {'category'.ljust(col_w)}  {'total':>10}  {'errors':>8}  "
            f"{'zero':>8}  {'docs':>10}"
        )
        for name, c in cats.items():
            print(
                f"  {name.ljust(col_w)}  {c['total']:>10d}  {c['errors']:>8d}  "
                f"{c['zero']:>8d}  {c['docs']:>10d}"
            )
    if args.enable_del:
        print("\nDEL Summary")
        print(f"  Attempts:                   {final['del_attempts']}")
        print(f"  Actually deleted:           {final['del_actually_deleted']}")
        print(f"  Missed (already gone):      {final['del_missed']}")
        print(f"  Skipped (verification key): {final['del_skipped_verification']}")
        print(f"  Skipped (other prefix):     {final['del_skipped_other_prefix']}")
        print(f"  Empty keyspace (no key):    {final['del_empty_keyspace']}")
        print(f"  Errors:                     {final['del_errors']}")
    if ERROR_LOG is not None:
        print(
            f"\nError log: {ERROR_LOG.path} "
            f"({len(ERROR_LOG.entries)}/{ERROR_LOG.capacity} entries kept)"
        )
