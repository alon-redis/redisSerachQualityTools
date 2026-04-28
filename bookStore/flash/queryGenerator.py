import argparse
import random
import threading
import time

import redis


INDEX_NAME = "idx:books"

# Closed vocabularies mirrored from bookHashPopulatorOnDisk.py so the simple
# workload picks values that actually exist in the indexed data.
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


COUNTERS = {
    "queries_total": 0,
    "queries_errors": 0,
    "queries_zero_results": 0,
    "docs_returned": 0,
}
COUNTERS_LOCK = threading.Lock()


def increment_counter(name, amount=1):
    with COUNTERS_LOCK:
        COUNTERS[name] += amount


def get_counters_snapshot():
    with COUNTERS_LOCK:
        return dict(COUNTERS)


# RediSearch TAG values must escape any of these characters.
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
    _q_catch_all,
    _q_format,
    _q_is_available,
    _q_status,
    _q_genres,
    _q_editions,
    _q_author_anchor,
    _q_title_anchor,
]


def build_simple_query():
    return random.choice(SIMPLE_QUERY_BUILDERS)()


def build_advanced_query():
    # Stub: complex queries (boolean combinations, prefix/fuzzy, FT.AGGREGATE,
    # multi-field filters, etc.) will live here. Kept as a placeholder so the
    # CLI/wiring is ready for the next iteration.
    raise NotImplementedError(
        "Advanced workload is not implemented yet. Use --workload simple."
    )


def parse_ft_search_response_nocontent(resp):
    """Parse a raw FT.SEARCH ... NOCONTENT RESP2 response.

    Layout when NOCONTENT is set: [total_in_index, doc_id_1, doc_id_2, ...].
    Returns (total_in_index, docs_in_response).
    """
    if not isinstance(resp, list) or not resp:
        return 0, 0
    total = resp[0] if isinstance(resp[0], int) else 0
    docs_returned = max(0, len(resp) - 1)
    return total, docs_returned


def run_worker(connection_pool, build_query_fn, pipeline_depth, limit, stop_event):
    r = redis.Redis(connection_pool=connection_pool)

    while not stop_event.is_set():
        batch_queries = [build_query_fn() for _ in range(pipeline_depth)]

        if pipeline_depth == 1:
            q = batch_queries[0]
            try:
                # Flex/disk index requires NOCONTENT (or RETURN 0); otherwise
                # the server returns SEARCH_FLEX_SEARCH_NOCONTENT_OR_RETURN_0_REQUIRED.
                resp = r.execute_command(
                    "FT.SEARCH", INDEX_NAME, q, "NOCONTENT", "LIMIT", "0", str(limit)
                )
                _, docs_returned = parse_ft_search_response_nocontent(resp)
                increment_counter("queries_total")
                increment_counter("docs_returned", docs_returned)
                if docs_returned == 0:
                    increment_counter("queries_zero_results")
            except (
                redis.exceptions.ResponseError,
                redis.exceptions.ConnectionError,
                redis.exceptions.TimeoutError,
            ):
                increment_counter("queries_total")
                increment_counter("queries_errors")
            continue

        try:
            pipe = r.pipeline(transaction=False)
            for q in batch_queries:
                pipe.execute_command(
                    "FT.SEARCH", INDEX_NAME, q, "NOCONTENT", "LIMIT", "0", str(limit)
                )
            results = pipe.execute(raise_on_error=False)
        except (
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
        ):
            increment_counter("queries_total", pipeline_depth)
            increment_counter("queries_errors", pipeline_depth)
            continue

        for resp in results:
            increment_counter("queries_total")
            if isinstance(resp, Exception):
                increment_counter("queries_errors")
                continue
            _, docs_returned = parse_ft_search_response_nocontent(resp)
            increment_counter("docs_returned", docs_returned)
            if docs_returned == 0:
                increment_counter("queries_zero_results")


def print_live_status(stop_event):
    print(
        "\rQueries: 0 (qps=0), Errors: 0 (eps=0), ZeroResults: 0, DocsReturned: 0",
        end="",
        flush=True,
    )
    last_total = 0
    last_errors = 0
    while not stop_event.is_set():
        time.sleep(1)
        c = get_counters_snapshot()
        qps = c["queries_total"] - last_total
        eps = c["queries_errors"] - last_errors
        last_total = c["queries_total"]
        last_errors = c["queries_errors"]
        print(
            f"\rQueries: {c['queries_total']} (qps={qps}), "
            f"Errors: {c['queries_errors']} (eps={eps}), "
            f"ZeroResults: {c['queries_zero_results']}, "
            f"DocsReturned: {c['docs_returned']}",
            end="",
            flush=True,
        )


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


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        description=(
            "Book store search query generator (simple workload + advanced stub, "
            "live metrics). Targets the index built by bookHashPopulatorOnDisk.py."
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
                            dest="workload", help="Workload type. 'advanced' is a stub for now.")
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

    if args.workload == "advanced":
        raise SystemExit(
            "Advanced workload is a stub for now. Use --workload simple. "
            "We'll implement advanced queries in the next iteration."
        )

    build_query_fn = build_simple_query

    print(
        f"Connecting to Redis at {args.redis_url} (pool={args.max_connections}), "
        f"workload={args.workload}, clients={args.clients}, pipeline={args.pipeline_depth}, "
        f"duration={args.duration}s, max_queries={args.max_queries}, limit={args.limit}"
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

    stop_event = threading.Event()
    status_stop_event = threading.Event()

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
        target=print_live_status, args=(status_stop_event,), daemon=True, name="qg-status"
    )
    stopper_thread = threading.Thread(
        target=stopper, args=(stop_event, args.duration, args.max_queries),
        daemon=True, name="qg-stopper",
    )

    status_thread.start()
    stopper_thread.start()
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
    stopper_thread.join(timeout=2)

    final = get_counters_snapshot()
    print("\n\nRun Summary")
    print(f"Total queries:        {final['queries_total']}")
    print(f"Errors:               {final['queries_errors']}")
    print(f"Zero-result queries:  {final['queries_zero_results']}")
    print(f"Docs returned:        {final['docs_returned']}")
