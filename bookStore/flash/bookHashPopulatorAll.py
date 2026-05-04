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


def make_key(book_id):
    return f"{REDIS_KEY_BASE}:{book_id}"


def is_book_key(key_name):
    return str(key_name).startswith(f"{REDIS_KEY_BASE}:")


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


def create_redis_connection_pool(redis_url, max_connections):
    return redis.ConnectionPool.from_url(
        redis_url,
        max_connections=max_connections,
        decode_responses=True
    )


def index_exists(connection_pool, index_name):
    try:
        r = redis.Redis(connection_pool=connection_pool)
        r.ft(index_name).info()
        print(f"Search index '{index_name}' already exists.")
        return True
    except redis.exceptions.ResponseError:
        print(f"Search index '{index_name}' does not exist. Creating it now...")
        return False
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to check index existence. Error: {str(e)}")
        return False


def create_search_index(connection_pool):
    try:
        r = redis.Redis(connection_pool=connection_pool)

        if index_exists(connection_pool, INDEX_NAME):
            print("Search index already exists.")
            return

        print("Creating search index (Flex/disk index compatible: no SORTABLE, no NUMERIC, no GEO fields; SKIPINITIALSCAN enabled).")
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
        "editions": random.sample(
            [
                "english", "spanish", "french", "german", "italian", "chinese",
                "japanese", "russian", "arabic", "portuguese", "korean", "dutch",
                "swedish", "norwegian", "danish", "finnish", "polish", "turkish",
                "hindi", "urdu", "greek", "hebrew", "thai", "vietnamese",
                "indonesian", "hungarian", "czech", "slovak", "romanian",
                "bulgarian", "ukrainian", "serbian", "croatian", "slovenian", "latvian"
            ],
            k=random.randint(1, 5)
        ),
        "genres": random.sample(
            [
                "comics (superheroes)", "fiction", "non-fiction", "science fiction",
                "fantasy", "mystery", "romance", "history", "horror", "biography",
                "thriller", "self-help", "poetry", "cookbooks", "memoir",
                "young adult", "children's literature", "drama", "travel", "science",
                "art", "philosophy", "psychology", "religion", "true crime",
                "graphic novel", "adventure", "political", "health", "humor"
            ],
            k=random.randint(1, 6)
        ),
        "inventory": [
            {
                "status": random.choice(["available", "maintenance", "on_loan", "for_sale"]),
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


def write_data_verification(connection_pool):
    try:
        r = redis.Redis(connection_pool=connection_pool)
        book_data = generate_random_book(0)
        book_data["author"] = "Alon Shmuely"
        book_data["title"] = "QA architect"
        book_data["address"] = "98765 Ein Dor Apt. 0001 Rishon Lezion, IL 1948"
        r.hset(make_key(0), mapping=flatten_book_for_hash(book_data))
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to write data verification. Error: {str(e)}")


def read_data_verification(connection_pool, stop_event, verify_sleep=0.05):
    try:
        r = redis.Redis(connection_pool=connection_pool)
        expected_key = make_key(0)
        query = Query("Shmuely").no_content().paging(0, 1)

        while not stop_event.is_set():
            try:
                docs = r.ft(INDEX_NAME).search(query).docs
                if docs and getattr(docs[0], "id", None) == expected_key:
                    increment_counter("data_verification_successful")
                else:
                    increment_counter("data_verification_error")
            except (IndexError, redis.exceptions.ResponseError, redis.exceptions.ConnectionError) as e:
                print(f"\nData verification failed. Error: {str(e)}")
                increment_counter("data_verification_error")

            time.sleep(verify_sleep)

    except redis.exceptions.ConnectionError as e:
        print(f"\nFailed to start data verification. Error: {str(e)}")


def generating_books(connection_pool, max_books, max_random, expiration_range=None):
    try:
        r = redis.Redis(connection_pool=connection_pool)
        for _ in range(1, max_books + 1):
            book_id = random.randint(1, max_random)
            book_data = generate_random_book(book_id)
            flat = flatten_book_for_hash(book_data)
            key = make_key(book_id)
            write_book_hash(r, key, flat, expiration_range)
            increment_counter("successful_write")
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to generate books. Error: {str(e)}")
        increment_counter("unsuccessful_write")


def deleting_books(connection_pool, writer_done_event, del_ratio, poll_sleep=0.01):
    if del_ratio <= 0:
        return

    try:
        r = redis.Redis(connection_pool=connection_pool)
        verification_key = make_key(0)

        while True:
            counters = get_counters_snapshot()
            target_deletes = int(counters["successful_write"] * del_ratio)
            sent_deletes = counters["successful_delete"] + counters["unsuccessful_delete"]

            if writer_done_event.is_set() and sent_deletes >= target_deletes:
                break

            if sent_deletes >= target_deletes:
                time.sleep(poll_sleep)
                continue

            random_key = r.randomkey()

            if random_key is None:
                time.sleep(poll_sleep)
                continue

            if random_key == verification_key:
                continue

            if not is_book_key(random_key):
                continue

            deleted = r.delete(random_key)

            if deleted == 1:
                increment_counter("successful_delete")
            else:
                increment_counter("unsuccessful_delete")

    except redis.exceptions.ConnectionError as e:
        print(f"\nFailed while deleting books. Error: {str(e)}")
        increment_counter("unsuccessful_delete")


def renaming_books(connection_pool, writer_done_event, rename_ratio, max_random, poll_sleep=0.01):
    if rename_ratio <= 0:
        return

    try:
        r = redis.Redis(connection_pool=connection_pool)
        verification_key = make_key(0)

        while True:
            counters = get_counters_snapshot()
            target_renames = int(counters["successful_write"] * rename_ratio)
            sent_renames = counters["successful_rename"] + counters["unsuccessful_rename"]

            if writer_done_event.is_set() and sent_renames >= target_renames:
                break

            if sent_renames >= target_renames:
                time.sleep(poll_sleep)
                continue

            random_key = r.randomkey()

            if random_key is None:
                time.sleep(poll_sleep)
                continue

            if random_key == verification_key:
                continue

            if not is_book_key(random_key):
                continue

            new_book_id = random.randint(1, max_random)
            renamed_key = make_key(new_book_id)

            if renamed_key == verification_key or renamed_key == random_key:
                continue

            try:
                r.rename(random_key, renamed_key)
                increment_counter("successful_rename")
            except redis.exceptions.RedisError:
                increment_counter("unsuccessful_rename")

    except redis.exceptions.ConnectionError as e:
        print(f"\nFailed while renaming books. Error: {str(e)}")
        increment_counter("unsuccessful_rename")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Running the book store application v2.3 (Flex/disk index compatible: no SORTABLE, no NUMERIC, no GEO fields)")
    arg_parser.add_argument("--redis", default="redis://localhost:6379", dest="redis_url", help="Redis URL to connect to.")
    arg_parser.add_argument("--max-connections", default=10, type=int, dest="max_connections", help="Maximum number of Redis connections.")
    arg_parser.add_argument("--max-books", default=3000, type=int, dest="max_books", help="Maximum number of books")
    arg_parser.add_argument("--max-random", default=3000, type=int, dest="max_random", help="Maximum random number of books")
    arg_parser.add_argument("--flush", action="store_true", help="Flush the Redis database on startup")
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
    args = arg_parser.parse_args()

    try:
        if args.del_ratio < 0:
            raise ValueError(f"--del-ratio must be >= 0, got: {args.del_ratio}")
        if args.rename_ratio < 0:
            raise ValueError(f"--rename-ratio must be >= 0, got: {args.rename_ratio}")

        print(f"Connecting to Redis at {args.redis_url} with a max of {args.max_connections} connections")
        redis_pool = create_redis_connection_pool(args.redis_url, args.max_connections)

        if args.flush:
            print("Flushing Redis database...")
            r = redis.Redis(connection_pool=redis_pool)
            r.flushall()

        create_search_index(redis_pool)
        write_data_verification(redis_pool)

        if args.expiration_range is not None:
            x, y = args.expiration_range
            print(
                f"Hash key expiration enabled: each book hash gets a random TTL between {x} and {y} seconds via HSETEX "
                f"(verification doc at {make_key(0)} is exempt and never expires)."
            )
        else:
            print("Hash key expiration disabled (no TTL on book hashes).")

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

        verification_thread = threading.Thread(
            target=read_data_verification,
            args=(redis_pool, stop_event, args.verify_sleep)
        )
        write_thread = threading.Thread(
            target=generating_books,
            args=(redis_pool, args.max_books, args.max_random, args.expiration_range)
        )
        delete_thread = None
        if del_enabled:
            delete_thread = threading.Thread(
                target=deleting_books,
                args=(redis_pool, writer_done_event, args.del_ratio)
            )
        rename_thread = None
        if rename_enabled:
            rename_thread = threading.Thread(
                target=renaming_books,
                args=(redis_pool, writer_done_event, args.rename_ratio, args.max_random)
            )
        status_thread = threading.Thread(
            target=print_live_status,
            args=(status_stop_event,)
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
