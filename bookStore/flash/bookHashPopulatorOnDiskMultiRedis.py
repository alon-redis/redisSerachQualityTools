import argparse
import random
import threading
import time

import redis
from faker import Faker
from redis.commands.search.field import TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query


REDIS_KEY_BASE = "alon:shmuely:redis:data:store:application"
INDEX_NAME = "idx:books"

fake = Faker()

# Per-server counters keyed by server label ("r1", "r2", ...)
COUNTERS: dict[str, dict[str, int]] = {}
COUNTERS_LOCK = threading.Lock()


def init_counters(labels: list[str]):
    for label in labels:
        COUNTERS[label] = {
            "data_verification_successful": 0,
            "data_verification_error": 0,
            "successful_write": 0,
            "unsuccessful_write": 0,
        }


def increment_counter(label: str, name: str, amount: int = 1):
    with COUNTERS_LOCK:
        COUNTERS[label][name] += amount


def get_counters_snapshot() -> dict[str, dict[str, int]]:
    with COUNTERS_LOCK:
        return {label: dict(c) for label, c in COUNTERS.items()}


def make_key(book_id):
    return f"{REDIS_KEY_BASE}:{book_id}"


def create_redis_connection_pool(redis_url: str, max_connections: int):
    return redis.ConnectionPool.from_url(
        redis_url,
        max_connections=max_connections,
        decode_responses=True,
    )


def index_exists(connection_pool, index_name: str, label: str) -> bool:
    try:
        r = redis.Redis(connection_pool=connection_pool)
        r.ft(index_name).info()
        print(f"[{label}] Search index '{index_name}' already exists.")
        return True
    except redis.exceptions.ResponseError:
        print(f"[{label}] Search index '{index_name}' does not exist. Creating it now...")
        return False
    except redis.exceptions.ConnectionError as e:
        print(f"[{label}] Failed to check index existence. Error: {e}")
        return False


def create_search_index(connection_pool, label: str):
    try:
        r = redis.Redis(connection_pool=connection_pool)

        if index_exists(connection_pool, INDEX_NAME, label):
            return

        print(f"[{label}] Creating search index (Flex/disk index compatible).")
        r.ft(INDEX_NAME).create_index(
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
                prefix=[f"{REDIS_KEY_BASE}:"],
            ),
            skip_initial_scan=True,
        )
    except redis.exceptions.ConnectionError as e:
        print(f"[{label}] Failed to create search index. Error: {e}")


def generate_random_book(book_id):
    return {
        "author": fake.name(),
        "id": str(book_id),
        "description": fake.paragraph(random.randint(25, 80)),
        "editions": random.sample(
            [
                "english", "spanish", "french", "german", "italian", "chinese",
                "japanese", "russian", "arabic", "portuguese", "korean", "dutch",
                "swedish", "norwegian", "danish", "finnish", "polish", "turkish",
                "hindi", "urdu", "greek", "hebrew", "thai", "vietnamese",
                "indonesian", "hungarian", "czech", "slovak", "romanian",
                "bulgarian", "ukrainian", "serbian", "croatian", "slovenian", "latvian",
            ],
            k=random.randint(1, 5),
        ),
        "genres": random.sample(
            [
                "comics (superheroes)", "fiction", "non-fiction", "science fiction",
                "fantasy", "mystery", "romance", "history", "horror", "biography",
                "thriller", "self-help", "poetry", "cookbooks", "memoir",
                "young adult", "children's literature", "drama", "travel", "science",
                "art", "philosophy", "psychology", "religion", "true crime",
                "graphic novel", "adventure", "political", "health", "humor",
            ],
            k=random.randint(1, 6),
        ),
        "inventory": [
            {
                "status": random.choice(["available", "maintenance", "on_loan", "for_sale"]),
                "stock_id": f"{book_id}_{num}",
            }
            for num in range(random.randint(1, 10))
        ],
        "metrics": {
            "rating_votes": random.randint(1, 1000),
            "score": round(random.uniform(1, 5), 2),
        },
        "pages": random.randint(50, 1500),
        "title": " ".join(fake.words(nb=random.randint(1, 5))),
        "url": fake.url(),
        "year_published": random.randint(1900, 2023),
        "format": random.choice(["hardcover", "paperback", "ebook"]),
        "is_available": random.choice([True, False]),
        "price": round(random.uniform(5, 100), 2),
        "isbn": fake.isbn13(),
        "address": fake.address().replace("\n", ", "),
        "geo": f"{fake.longitude()},{fake.latitude()}",
        "weight_grams": random.randint(-100, 2000),
        "dimensions": {
            "width_cm": round(random.uniform(10, 30), 2),
            "height_cm": round(random.uniform(20, 40), 2),
            "depth_cm": round(random.uniform(1, 10), 2),
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


def flatten_book_for_hash(book_data: dict) -> dict:
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


def print_live_status(stop_event: threading.Event, labels: list[str]):
    while not stop_event.is_set():
        snap = get_counters_snapshot()
        parts = []
        for label in labels:
            c = snap[label]
            parts.append(
                f"[{label}] OK-write:{c['successful_write']} "
                f"ERR-write:{c['unsuccessful_write']} "
                f"OK-verify:{c['data_verification_successful']} "
                f"ERR-verify:{c['data_verification_error']}"
            )
        print(f"\r{' | '.join(parts)}", end="", flush=True)
        time.sleep(1)


def write_data_verification(pools: dict[str, redis.ConnectionPool]):
    """Write a known sentinel book (id=0) to every server."""
    book_data = generate_random_book(0)
    book_data["author"] = "Alon Shmuely"
    book_data["title"] = "QA architect"
    book_data["address"] = "98765 Ein Dor Apt. 0001 Rishon Lezion, IL 1948"
    flat = flatten_book_for_hash(book_data)
    key = make_key(0)

    for label, pool in pools.items():
        try:
            r = redis.Redis(connection_pool=pool)
            r.hset(key, mapping=flat)
        except redis.exceptions.ConnectionError as e:
            print(f"[{label}] Failed to write data verification sentinel. Error: {e}")


def read_data_verification(
    connection_pool: redis.ConnectionPool,
    label: str,
    stop_event: threading.Event,
    verify_sleep: float = 0.05,
):
    """Continuously verify the sentinel book is searchable on a single server."""
    try:
        r = redis.Redis(connection_pool=connection_pool)
        expected_key = make_key(0)
        query = Query("Shmuely").no_content().paging(0, 1)

        while not stop_event.is_set():
            try:
                docs = r.ft(INDEX_NAME).search(query).docs
                if docs and getattr(docs[0], "id", None) == expected_key:
                    increment_counter(label, "data_verification_successful")
                else:
                    increment_counter(label, "data_verification_error")
            except (IndexError, redis.exceptions.ResponseError, redis.exceptions.ConnectionError) as e:
                print(f"\n[{label}] Data verification failed. Error: {e}")
                increment_counter(label, "data_verification_error")

            time.sleep(verify_sleep)

    except redis.exceptions.ConnectionError as e:
        print(f"\n[{label}] Failed to start data verification. Error: {e}")


def generating_books(
    pools: dict[str, redis.ConnectionPool],
    max_books: int,
    max_random: int,
):
    """
    Generate each book exactly once and write the identical payload to every
    configured Redis server so that both receive the same traffic.
    """
    clients = {label: redis.Redis(connection_pool=pool) for label, pool in pools.items()}

    for _ in range(1, max_books + 1):
        book_id = random.randint(1, max_random)
        # Generate data ONCE — same record goes to every server
        flat = flatten_book_for_hash(generate_random_book(book_id))
        key = make_key(book_id)

        for label, client in clients.items():
            try:
                client.hset(key, mapping=flat)
                increment_counter(label, "successful_write")
            except redis.exceptions.ConnectionError as e:
                print(f"\n[{label}] Failed to write book {book_id}. Error: {e}")
                increment_counter(label, "unsuccessful_write")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        description=(
            "Book store populator v2.2 — multi-Redis edition. "
            "Sends identical dynamically-generated traffic to up to two Redis servers."
        )
    )
    arg_parser.add_argument(
        "--redis",
        default="redis://localhost:6379",
        dest="redis_url",
        help="Primary Redis URL.",
    )
    arg_parser.add_argument(
        "--redis2",
        default=None,
        dest="redis_url2",
        help="Secondary Redis URL. When provided, every write and verification is mirrored here.",
    )
    arg_parser.add_argument(
        "--max-connections",
        default=10,
        type=int,
        dest="max_connections",
        help="Maximum number of connections per Redis server.",
    )
    arg_parser.add_argument(
        "--max-books",
        default=3000,
        type=int,
        dest="max_books",
        help="Total number of book write operations to perform.",
    )
    arg_parser.add_argument(
        "--max-random",
        default=3000,
        type=int,
        dest="max_random",
        help="Upper bound for random book IDs (controls key-space size).",
    )
    arg_parser.add_argument(
        "--flush",
        action="store_true",
        help="FLUSHALL every configured Redis server on startup.",
    )
    arg_parser.add_argument(
        "--verify-sleep",
        default=0.05,
        type=float,
        dest="verify_sleep",
        help="Seconds to sleep between verification queries (per server).",
    )
    args = arg_parser.parse_args()

    # Build the ordered map of server label -> URL
    server_urls: dict[str, str] = {"r1": args.redis_url}
    if args.redis_url2:
        server_urls["r2"] = args.redis_url2

    labels = list(server_urls.keys())
    init_counters(labels)

    try:
        pools: dict[str, redis.ConnectionPool] = {}
        for label, url in server_urls.items():
            print(f"[{label}] Connecting to Redis at {url} (max_connections={args.max_connections})")
            pools[label] = create_redis_connection_pool(url, args.max_connections)

        if args.flush:
            for label, pool in pools.items():
                print(f"[{label}] Flushing Redis database...")
                redis.Redis(connection_pool=pool).flushall()

        for label, pool in pools.items():
            create_search_index(pool, label)

        write_data_verification(pools)

        stop_event = threading.Event()
        status_stop_event = threading.Event()

        threads: list[threading.Thread] = []

        # One verification thread per server
        for label, pool in pools.items():
            t = threading.Thread(
                target=read_data_verification,
                args=(pool, label, stop_event, args.verify_sleep),
                name=f"verify-{label}",
            )
            threads.append(t)

        write_thread = threading.Thread(
            target=generating_books,
            args=(pools, args.max_books, args.max_random),
            name="writer",
        )

        status_thread = threading.Thread(
            target=print_live_status,
            args=(status_stop_event, labels),
            name="status",
        )

        status_thread.start()
        for t in threads:
            t.start()
        write_thread.start()

        write_thread.join()
        stop_event.set()
        for t in threads:
            t.join()

        status_stop_event.set()
        status_thread.join()

        snap = get_counters_snapshot()
        print("\n\nRun Summary")
        for label in labels:
            c = snap[label]
            print(f"  [{label}]")
            print(f"    Successful writes:      {c['successful_write']}")
            print(f"    Failed writes:          {c['unsuccessful_write']}")
            print(f"    Successful verifications: {c['data_verification_successful']}")
            print(f"    Failed verifications:   {c['data_verification_error']}")

    except redis.exceptions.ConnectionError as e:
        print(f"Failed to connect to Redis. Error: {e}")
