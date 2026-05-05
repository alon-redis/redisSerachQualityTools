import argparse
import random
import re
import signal
import threading
import time

import redis
from faker import Faker
from redis.cluster import RedisCluster
from redis.commands.search.field import TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query


REDIS_KEY_BASE = "alon:shmuely:redis:data:store:application"
INDEX_NAME = "idx:books"
SLOT_BUCKETS = 64

# Compiled once at import time; `extract_bucket_tag_from_key` is on the rename
# hot path, so paying the regex compile cost per call is wasteful.
BUCKET_TAG_PATTERN = re.compile(
    rf"^{re.escape(REDIS_KEY_BASE)}:\{{(b\d{{2}})\}}:\d+$"
)
KEY_PREFIX = f"{REDIS_KEY_BASE}:"

# Hoisted out of `generate_random_book` so the literal sequences are not
# rebuilt on every book.
EDITIONS_POOL = (
    "english", "spanish", "french", "german", "italian", "chinese",
    "japanese", "russian", "arabic", "portuguese", "korean", "dutch",
    "swedish", "norwegian", "danish", "finnish", "polish", "turkish",
    "hindi", "urdu", "greek", "hebrew", "thai", "vietnamese",
    "indonesian", "hungarian", "czech", "slovak", "romanian",
    "bulgarian", "ukrainian", "serbian", "croatian", "slovenian", "latvian",
)
GENRES_POOL = (
    "comics (superheroes)", "fiction", "non-fiction", "science fiction",
    "fantasy", "mystery", "romance", "history", "horror", "biography",
    "thriller", "self-help", "poetry", "cookbooks", "memoir",
    "young adult", "children's literature", "drama", "travel", "science",
    "art", "philosophy", "psychology", "religion", "true crime",
    "graphic novel", "adventure", "political", "health", "humor",
)
INVENTORY_STATUSES = ("available", "maintenance", "on_loan", "for_sale")
FORMAT_OPTIONS = ("hardcover", "paperback", "ebook")
AVAILABILITY_OPTIONS = (True, False)

fake = Faker()

COUNTERS = {
    "data_verification_successful": 0,
    "data_verification_error": 0,
    "successful_write": 0,
    "unsuccessful_write": 0,
    "successful_delete": 0,
    "unsuccessful_delete": 0,
    "successful_rename": 0,
    "unsuccessful_rename": 0,
}
COUNTERS_LOCK = threading.Lock()


def increment_counter(name, amount=1):
    with COUNTERS_LOCK:
        COUNTERS[name] += amount


def get_counters_snapshot():
    with COUNTERS_LOCK:
        return dict(COUNTERS)


def get_counter_values(*names):
    """Return only the requested counter values under a single lock acquisition.

    Cheaper than `get_counters_snapshot()` for hot loops that need 2-3 fields,
    because it avoids copying the whole dict.
    """
    with COUNTERS_LOCK:
        return tuple(COUNTERS[name] for name in names)


def make_key(book_id):
    bucket_tag = f"b{book_id % SLOT_BUCKETS:02d}"
    return f"{REDIS_KEY_BASE}:{{{bucket_tag}}}:{book_id}"


def is_book_key(key_name):
    return str(key_name).startswith(KEY_PREFIX)


def extract_bucket_tag_from_key(key_name):
    match = BUCKET_TAG_PATTERN.match(str(key_name))
    if match is None:
        return None
    return match.group(1)


def parse_expiration_range(value):
    try:
        x_str, y_str = value.split("-", 1)
        x, y = int(x_str), int(y_str)
    except (ValueError, AttributeError):
        raise argparse.ArgumentTypeError(
            f"--expiration-time must be in format X-Y (integers in seconds), got: {value!r}"
        )
    if x < 1 or y < 1 or x > y:
        raise argparse.ArgumentTypeError(
            f"--expiration-time requires 1 <= X <= Y, got: X={x}, Y={y}"
        )
    return (x, y)


def write_book_hash(r, key, mapping, expiration_range):
    if expiration_range is None:
        r.hset(key, mapping=mapping)
        return
    x, y = expiration_range
    ttl_seconds = random.randint(x, y)
    r.hsetex(key, mapping=mapping, ex=ttl_seconds)


def create_redis_client(redis_url, max_connections, use_oss_cluster_api=False):
    """Build a single Redis client (standalone or cluster).

    Both `redis.Redis` (backed by a `ConnectionPool`) and `RedisCluster` are
    thread-safe, so the returned client is intended to be shared across all
    worker threads instead of being rebuilt per thread/iteration.
    """
    if use_oss_cluster_api:
        try:
            return RedisCluster.from_url(
                redis_url,
                decode_responses=True,
                max_connections=max_connections,
            )
        except TypeError:
            # Compatibility fallback for redis-py variants that don't expose max_connections here.
            return RedisCluster.from_url(
                redis_url,
                decode_responses=True,
            )

    pool = redis.ConnectionPool.from_url(
        redis_url,
        max_connections=max_connections,
        decode_responses=True,
    )
    return redis.Redis(connection_pool=pool)


def close_redis_client(client):
    """Best-effort shutdown of a Redis client and its underlying connection pool(s)."""
    if client is None:
        return
    try:
        client.close()
    except Exception as exc:
        print(f"Error closing Redis client: {exc}")
    try:
        if isinstance(client, RedisCluster):
            disconnect = getattr(client, "disconnect_connection_pools", None)
            if disconnect is not None:
                disconnect()
        else:
            pool = getattr(client, "connection_pool", None)
            if pool is not None:
                pool.disconnect()
    except Exception as exc:
        print(f"Error disconnecting Redis pool(s): {exc}")


class PrimaryNodeRotator:
    """Round-robin RANDOMKEY targeting across cluster primary nodes.

    `RedisCluster.randomkey()` only hits a single node per call, so without
    direction it biases toward whichever node redis-py happens to pick. This
    rotator cycles through `get_primaries()` so every primary is sampled
    fairly across consecutive calls. Falls back to `RedisCluster.RANDOM`
    when the topology can't be enumerated.
    """

    def __init__(self, client):
        self._client = client
        self._lock = threading.Lock()
        self._idx = 0

    def next_target(self):
        try:
            primaries = list(self._client.get_primaries())
        except Exception:
            primaries = []
        if not primaries:
            return RedisCluster.RANDOM
        with self._lock:
            node = primaries[self._idx % len(primaries)]
            self._idx += 1
        return node


def random_book_key(client, rotator):
    """RANDOMKEY wrapper that rotates targets across primaries in cluster mode."""
    if rotator is None:
        return client.randomkey()
    return client.randomkey(target_nodes=rotator.next_target())


def index_exists(client, index_name):
    try:
        client.ft(index_name).info()
        print(f"Search index '{index_name}' already exists.")
        return True
    except redis.exceptions.ResponseError:
        print(f"Search index '{index_name}' does not exist. Creating it now...")
        return False
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to check index existence. Error: {str(e)}")
        return False


def create_search_index(client):
    try:
        if index_exists(client, INDEX_NAME):
            print("Search index already exists.")
            return

        print("Creating search index (Flex/disk index compatible: no SORTABLE, no NUMERIC, no GEO fields; SKIPINITIALSCAN enabled).")
        client.ft(INDEX_NAME).create_index(
            [
                TextField("author"),
                TagField("id"),
                TextField("description"),
                TagField("editions", separator="|"),
                TagField("genres", separator="|"),
                TagField("pages"),
                TextField("title"),
                TagField("year_published"),
                TagField("rating_votes"),
                TagField("score"),
                TagField("status", separator="|"),
                TagField("stock_id", separator="|"),
                TagField("format"),
                TagField("is_available"),
                TagField("price"),
                TagField("isbn"),
                TagField("geo", separator="|"),
                TextField("publisher"),
                TextField("book_series"),
                TextField("main_character"),
                TextField("location"),
                TextField("address"),
                TagField("edition_number"),
                TagField("chapter_count"),
                TagField("review_count"),
                TagField("citation_count"),
                TagField("publishing_delay"),
                TagField("word_count"),
                TagField("timestamp"),
                TagField("reading_time_minutes"),
                TagField("global_sales"),
                TagField("translations_count"),
                TagField("author_age_at_publication"),
                TagField("weight_grams"),
                TagField("width_cm"),
                TagField("height_cm"),
                TagField("depth_cm"),
            ],
            definition=IndexDefinition(
                index_type=IndexType.HASH,
                prefix=[f"{REDIS_KEY_BASE}:"]
            ),
            skip_initial_scan=True,
        )
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to create search index. Error: {str(e)}")


def generate_random_book(book_id):
    return {
        "author": fake.name(),
        "id": str(book_id),
        "description": fake.paragraph(random.randint(25, 80)),
        "editions": random.sample(EDITIONS_POOL, k=random.randint(1, 5)),
        "genres": random.sample(GENRES_POOL, k=random.randint(1, 6)),
        "inventory": [
            {
                "status": random.choice(INVENTORY_STATUSES),
                "stock_id": f"{book_id}_{num}"
            }
            for num in range(random.randint(1, 10))
        ],
        "metrics": {
            "rating_votes": random.randint(1, 1000),
            "score": round(random.uniform(1, 5), 2)
        },
        "pages": random.randint(50, 1500),
        "title": " ".join(fake.words(nb=random.randint(1, 5))),
        "url": fake.url(),
        "year_published": random.randint(1900, 2023),
        "format": random.choice(FORMAT_OPTIONS),
        "is_available": random.choice(AVAILABILITY_OPTIONS),
        "price": round(random.uniform(5, 100), 2),
        "isbn": fake.isbn13(),
        "address": fake.address().replace("\n", ", "),
        "geo": f"{fake.longitude()},{fake.latitude()}",
        "weight_grams": random.randint(-100, 2000),
        "dimensions": {
            "width_cm": round(random.uniform(10, 30), 2),
            "height_cm": round(random.uniform(20, 40), 2),
            "depth_cm": round(random.uniform(1, 10), 2)
        },
        "edition_number": random.randint(1, 10),
        "chapter_count": random.randint(5, 50),
        "review_count": random.randint(0, 5000),
        "citation_count": random.randint(0, 1000),
        "timestamp": fake.unix_time(),
        "publishing_delay": random.randint(-356, 1000),
        "word_count": random.randint(10000, 150000),
        "reading_time_minutes": random.randint(30, 1200),
        "global_sales": random.randint(1000, 1000000),
        "translations_count": random.randint(1, 50),
        "publisher": fake.company(),
        "book_series": fake.word(),
        "main_character": fake.first_name(),
        "location": fake.city(),
        "author_age_at_publication": random.randint(20, 80),
    }


def flatten_book_for_hash(book_data):
    inventory_status = [item["status"] for item in book_data["inventory"]]
    inventory_stock_id = [item["stock_id"] for item in book_data["inventory"]]

    return {
        "author": str(book_data["author"]),
        "id": str(book_data["id"]),
        "description": str(book_data["description"]),
        "editions": "|".join(book_data["editions"]),
        "genres": "|".join(book_data["genres"]),
        "status": "|".join(inventory_status),
        "stock_id": "|".join(inventory_stock_id),
        "rating_votes": str(book_data["metrics"]["rating_votes"]),
        "score": str(book_data["metrics"]["score"]),
        "pages": str(book_data["pages"]),
        "title": str(book_data["title"]),
        "url": str(book_data["url"]),
        "year_published": str(book_data["year_published"]),
        "format": str(book_data["format"]),
        "is_available": str(book_data["is_available"]).lower(),
        "price": str(book_data["price"]),
        "isbn": str(book_data["isbn"]),
        "address": str(book_data["address"]),
        "geo": str(book_data["geo"]),
        "weight_grams": str(book_data["weight_grams"]),
        "width_cm": str(book_data["dimensions"]["width_cm"]),
        "height_cm": str(book_data["dimensions"]["height_cm"]),
        "depth_cm": str(book_data["dimensions"]["depth_cm"]),
        "edition_number": str(book_data["edition_number"]),
        "chapter_count": str(book_data["chapter_count"]),
        "review_count": str(book_data["review_count"]),
        "citation_count": str(book_data["citation_count"]),
        "timestamp": str(book_data["timestamp"]),
        "publishing_delay": str(book_data["publishing_delay"]),
        "word_count": str(book_data["word_count"]),
        "reading_time_minutes": str(book_data["reading_time_minutes"]),
        "global_sales": str(book_data["global_sales"]),
        "translations_count": str(book_data["translations_count"]),
        "publisher": str(book_data["publisher"]),
        "book_series": str(book_data["book_series"]),
        "main_character": str(book_data["main_character"]),
        "location": str(book_data["location"]),
        "author_age_at_publication": str(book_data["author_age_at_publication"]),
    }


def print_live_status(stop_event):
    while not stop_event.is_set():
        counters = get_counters_snapshot()
        print(
            f"\rCurrent Status, Successful Verification: {counters['data_verification_successful']}, "
            f"Error Verification: {counters['data_verification_error']}, "
            f"Successful Writes: {counters['successful_write']}, "
            f"Unsuccessful Writes: {counters['unsuccessful_write']}, "
            f"Successful Deletes: {counters['successful_delete']}, "
            f"Delete Errors: {counters['unsuccessful_delete']}, "
            f"Successful Renames: {counters['successful_rename']}, "
            f"Rename Errors: {counters['unsuccessful_rename']}",
            end="",
            flush=True
        )
        time.sleep(1)


def write_data_verification(client):
    try:
        book_data = generate_random_book(0)
        book_data["author"] = "Alon Shmuely"
        book_data["title"] = "QA architect"
        book_data["address"] = "98765 Ein Dor Apt. 0001 Rishon Lezion, IL 1948"
        client.hset(make_key(0), mapping=flatten_book_for_hash(book_data))
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to write data verification. Error: {str(e)}")


def read_data_verification(client, stop_event, verify_sleep=0.05):
    expected_key = make_key(0)
    # TIMEOUT (ms) caps server-side execution so a slow shard can't stall the
    # verifier; DIALECT 2 pins parser semantics so failure modes don't drift
    # across server versions.
    query = Query("Shmuely").no_content().paging(0, 1).timeout(50).dialect(2)

    while not stop_event.is_set():
        try:
            docs = client.ft(INDEX_NAME).search(query).docs
            if docs and getattr(docs[0], "id", None) == expected_key:
                increment_counter("data_verification_successful")
            else:
                increment_counter("data_verification_error")
        except (IndexError, redis.exceptions.ResponseError, redis.exceptions.ConnectionError) as e:
            print(f"\nData verification failed. Error: {str(e)}")
            increment_counter("data_verification_error")

        if stop_event.wait(verify_sleep):
            break


def _build_pipeline(client):
    """Construct a non-transactional pipeline, tolerating older redis-py variants
    whose `RedisCluster.pipeline()` doesn't accept `transaction=`."""
    try:
        return client.pipeline(transaction=False)
    except TypeError:
        return client.pipeline()


def _flush_write_batch(client, batch, expiration_range):
    """Queue every (key, flat_mapping) pair into a pipeline and execute it.

    Returns (success_count, failure_count). Uses `raise_on_error=False` so a
    single bad command doesn't poison the rest of the batch; per-command
    failures are counted via the returned exception sentinels.
    """
    if not batch:
        return 0, 0

    pipe = _build_pipeline(client)
    if expiration_range is None:
        for key, flat in batch:
            pipe.hset(key, mapping=flat)
    else:
        x, y = expiration_range
        for key, flat in batch:
            pipe.hsetex(key, mapping=flat, ex=random.randint(x, y))

    try:
        results = pipe.execute(raise_on_error=False)
    except redis.exceptions.ConnectionError as exc:
        print(f"\nPipeline flush failed. Error: {exc}")
        return 0, len(batch)

    success = sum(1 for r in results if not isinstance(r, BaseException))
    return success, len(batch) - success


def generating_books(client, max_books, max_random, expiration_range, stop_event, pipeline_size=100):
    """Generate up to `max_books` book hashes, batching writes via pipeline.

    `pipeline_size` controls how many HSET/HSETEX commands are queued per
    flush. Larger batches drop RTT overhead dramatically; in cluster mode,
    redis-py routes the queued commands per-shard in parallel.
    """
    pipeline_size = max(1, int(pipeline_size))
    remaining = max_books
    batch = []

    try:
        while remaining > 0 and not stop_event.is_set():
            batch_n = min(pipeline_size, remaining)
            batch.clear()
            for _ in range(batch_n):
                book_id = random.randint(1, max_random)
                key = make_key(book_id)
                flat = flatten_book_for_hash(generate_random_book(book_id))
                batch.append((key, flat))

            success, failure = _flush_write_batch(client, batch, expiration_range)
            if success:
                increment_counter("successful_write", amount=success)
            if failure:
                increment_counter("unsuccessful_write", amount=failure)
            remaining -= batch_n
    except redis.exceptions.ConnectionError as e:
        print(f"\nFailed to generate books. Error: {str(e)}")
        increment_counter("unsuccessful_write", amount=remaining if remaining > 0 else 1)


def deleting_books(client, writer_done_event, stop_event, del_ratio, rotator, poll_sleep=0.01):
    if del_ratio <= 0:
        return

    try:
        verification_key = make_key(0)

        while not stop_event.is_set():
            sw, sd, ud = get_counter_values(
                "successful_write", "successful_delete", "unsuccessful_delete"
            )
            target_deletes = int(sw * del_ratio)
            sent_deletes = sd + ud

            if writer_done_event.is_set() and sent_deletes >= target_deletes:
                break

            if sent_deletes >= target_deletes:
                if stop_event.wait(poll_sleep):
                    break
                continue

            random_key = random_book_key(client, rotator)

            if random_key is None:
                if stop_event.wait(poll_sleep):
                    break
                continue

            if random_key == verification_key:
                continue

            if not is_book_key(random_key):
                continue

            deleted = client.delete(random_key)

            if deleted == 1:
                increment_counter("successful_delete")
            else:
                increment_counter("unsuccessful_delete")

    except redis.exceptions.ConnectionError as e:
        print(f"\nFailed while deleting books. Error: {str(e)}")
        increment_counter("unsuccessful_delete")


def renaming_books(client, writer_done_event, stop_event, rename_ratio, max_random, rotator, poll_sleep=0.01):
    if rename_ratio <= 0:
        return

    try:
        verification_key = make_key(0)

        while not stop_event.is_set():
            sw, sr, ur = get_counter_values(
                "successful_write", "successful_rename", "unsuccessful_rename"
            )
            target_renames = int(sw * rename_ratio)
            sent_renames = sr + ur

            if writer_done_event.is_set() and sent_renames >= target_renames:
                break

            if sent_renames >= target_renames:
                if stop_event.wait(poll_sleep):
                    break
                continue

            random_key = random_book_key(client, rotator)

            if random_key is None:
                if stop_event.wait(poll_sleep):
                    break
                continue

            if random_key == verification_key:
                continue

            if not is_book_key(random_key):
                continue

            source_bucket_tag = extract_bucket_tag_from_key(random_key)
            if source_bucket_tag is None:
                continue

            new_book_id = random.randint(1, max_random)
            renamed_key = f"{REDIS_KEY_BASE}:{{{source_bucket_tag}}}:{new_book_id}"

            if renamed_key == verification_key or renamed_key == random_key:
                continue

            try:
                client.rename(random_key, renamed_key)
                increment_counter("successful_rename")
            except redis.exceptions.RedisError:
                increment_counter("unsuccessful_rename")

    except redis.exceptions.ConnectionError as e:
        print(f"\nFailed while renaming books. Error: {str(e)}")
        increment_counter("unsuccessful_rename")


def install_signal_handlers(*events):
    """Install SIGINT/SIGTERM handlers that set every provided Event for graceful shutdown."""
    def _handler(signum, _frame):
        print(f"\nReceived signal {signum}; initiating graceful shutdown...")
        for event in events:
            event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handler)
        except (ValueError, OSError):
            # Not running in the main thread, or platform doesn't support this signal.
            pass


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Running the book store application v2.4 (Flex/disk index compatible: no SORTABLE, no NUMERIC, no GEO fields)")
    arg_parser.add_argument("--redis", default="redis://localhost:6379", dest="redis_url", help="Redis URL to connect to.")
    arg_parser.add_argument("--max-connections", default=10, type=int, dest="max_connections", help="Maximum number of Redis connections.")
    arg_parser.add_argument("--max-books", default=3000, type=int, dest="max_books", help="Maximum number of books")
    arg_parser.add_argument("--max-random", default=3000, type=int, dest="max_random", help="Maximum random number of books")
    arg_parser.add_argument("--flush", action="store_true", help="Flush the Redis database on startup")
    arg_parser.add_argument(
        "--oss-cluster-api",
        action="store_true",
        dest="use_oss_cluster_api",
        help="Use Redis OSS Cluster API mode (RedisCluster client). Default: disabled (standalone Redis API).",
    )
    arg_parser.add_argument("--verify-sleep", default=0.05, type=float, dest="verify_sleep", help="Sleep time in seconds between verification queries")
    arg_parser.add_argument(
        "--expiration-time",
        default=None,
        type=parse_expiration_range,
        dest="expiration_range",
        help="Optional random hash key expiration in seconds, format X-Y (e.g., 60-300). "
             "When set, each book hash is written via HSETEX with a random TTL in [X, Y]. "
             "The verification hash is always exempt. Default: no expiration.",
    )
    arg_parser.add_argument(
        "--del-ratio",
        default=0.25,
        type=float,
        dest="del_ratio",
        help="Delete-to-write ratio. For each written book, approximately this ratio of DEL commands "
             "is sent from a dedicated deleter thread using RANDOMKEY. Default: 0.25 (enabled).",
    )
    arg_parser.add_argument(
        "--rename-ratio",
        default=0.01,
        type=float,
        dest="rename_ratio",
        help="Rename-to-write ratio. For each written book, approximately this ratio of RENAME commands "
             "is sent from a dedicated rename thread using RANDOMKEY. Default: 0.01 (enabled).",
    )
    arg_parser.add_argument(
        "--pipeline-size",
        default=100,
        type=int,
        dest="pipeline_size",
        help="Number of HSET/HSETEX commands batched per pipeline flush in the writer thread. "
             "Use 1 to disable pipelining. Default: 100.",
    )
    args = arg_parser.parse_args()

    redis_client = None
    try:
        if args.del_ratio < 0:
            raise ValueError(f"--del-ratio must be >= 0, got: {args.del_ratio}")
        if args.rename_ratio < 0:
            raise ValueError(f"--rename-ratio must be >= 0, got: {args.rename_ratio}")
        if args.pipeline_size < 1:
            raise ValueError(f"--pipeline-size must be >= 1, got: {args.pipeline_size}")

        api_mode = "oss-cluster-api" if args.use_oss_cluster_api else "standalone-api"
        print(
            f"Connecting to Redis at {args.redis_url} with a max of {args.max_connections} connections "
            f"(mode: {api_mode})"
        )
        redis_client = create_redis_client(
            args.redis_url, args.max_connections, args.use_oss_cluster_api
        )
        cluster_rotator = (
            PrimaryNodeRotator(redis_client) if args.use_oss_cluster_api else None
        )

        if args.flush:
            print("Flushing Redis database...")
            redis_client.flushall()

        create_search_index(redis_client)
        write_data_verification(redis_client)

        if args.expiration_range is not None:
            x, y = args.expiration_range
            print(
                f"Hash key expiration enabled: each book hash gets a random TTL between {x} and {y} seconds via HSETEX "
                f"(verification doc at {make_key(0)} is exempt and never expires)."
            )
        else:
            print("Hash key expiration disabled (no TTL on book hashes).")

        if args.pipeline_size > 1:
            print(f"Writer pipelining enabled: batch size = {args.pipeline_size} commands per flush.")
        else:
            print("Writer pipelining disabled (--pipeline-size=1).")

        del_enabled = args.del_ratio > 0
        if del_enabled:
            print(
                f"Delete thread enabled: target DEL/WRITE ratio={args.del_ratio}. "
                f"Safety guards: only '{REDIS_KEY_BASE}:' keys are deleted, and {make_key(0)} is never deleted."
            )
        else:
            print("Delete thread disabled because --del-ratio <= 0.")

        rename_enabled = args.rename_ratio > 0
        if rename_enabled:
            print(
                f"Rename thread enabled: target RENAME/WRITE ratio={args.rename_ratio}. "
                f"Safety guards: only '{REDIS_KEY_BASE}:' keys are renamed, and {make_key(0)} is never renamed."
            )
        else:
            print("Rename thread disabled because --rename-ratio <= 0.")

        stop_event = threading.Event()
        status_stop_event = threading.Event()
        writer_done_event = threading.Event()

        # SIGINT/SIGTERM trip every shutdown gate so the orderly join sequence below
        # unblocks promptly instead of leaving threads spinning.
        install_signal_handlers(stop_event, writer_done_event, status_stop_event)

        verification_thread = threading.Thread(
            target=read_data_verification,
            args=(redis_client, stop_event, args.verify_sleep),
            daemon=True,
        )
        write_thread = threading.Thread(
            target=generating_books,
            args=(redis_client, args.max_books, args.max_random, args.expiration_range, stop_event, args.pipeline_size),
            daemon=True,
        )
        delete_thread = None
        if del_enabled:
            delete_thread = threading.Thread(
                target=deleting_books,
                args=(redis_client, writer_done_event, stop_event, args.del_ratio, cluster_rotator, 0.01),
                daemon=True,
            )
        rename_thread = None
        if rename_enabled:
            rename_thread = threading.Thread(
                target=renaming_books,
                args=(redis_client, writer_done_event, stop_event, args.rename_ratio, args.max_random, cluster_rotator, 0.01),
                daemon=True,
            )
        status_thread = threading.Thread(
            target=print_live_status,
            args=(status_stop_event,),
            daemon=True,
        )

        status_thread.start()
        verification_thread.start()
        write_thread.start()
        if delete_thread:
            delete_thread.start()
        if rename_thread:
            rename_thread.start()

        write_thread.join()
        writer_done_event.set()

        if delete_thread:
            delete_thread.join()
        if rename_thread:
            rename_thread.join()

        stop_event.set()
        verification_thread.join()

        status_stop_event.set()
        status_thread.join()

        counters = get_counters_snapshot()

        print("\n\nRun Summary")
        print(f"Successful verification: {counters['data_verification_successful']}")
        print(f"Error verification: {counters['data_verification_error']}")
        print(f"Successful writes: {counters['successful_write']}")
        print(f"Error writes: {counters['unsuccessful_write']}")
        print(f"Successful deletes: {counters['successful_delete']}")
        print(f"Error deletes: {counters['unsuccessful_delete']}")
        print(f"Successful renames: {counters['successful_rename']}")
        print(f"Error renames: {counters['unsuccessful_rename']}")

    except redis.exceptions.ConnectionError as e:
        print(f"Failed to connect to Redis. Error: {str(e)}")
    except ValueError as e:
        print(f"Invalid input. Error: {str(e)}")
    finally:
        close_redis_client(redis_client)
