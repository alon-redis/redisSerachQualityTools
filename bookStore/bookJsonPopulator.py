import argparse
import json
import random
import threading
import time

import redis
from faker import Faker
from redis.commands.json.path import Path
from redis.commands.search.field import GeoField, NumericField, TagField, TextField
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

        print("Creating search index.")
        r.ft(INDEX_NAME).create_index(
            [
                TextField("$.author", as_name="author", sortable=True),
                TagField("$.id", as_name="id", sortable=True),
                TextField("$.description", as_name="description"),
                TagField("$.editions[*]", as_name="editions", sortable=True),
                TagField("$.genres[*]", as_name="genres", sortable=True),
                NumericField("$.pages", as_name="pages", sortable=True),
                TextField("$.title", as_name="title", sortable=True),
                NumericField("$.year_published", as_name="year_published", sortable=True),
                NumericField("$.metrics.rating_votes", as_name="rating_votes", sortable=True),
                NumericField("$.metrics.score", as_name="score", sortable=True),
                TagField("$.inventory[*].status", as_name="status", sortable=True),
                TagField("$.inventory[*].stock_id", as_name="stock_id", sortable=True),
                TagField("$.format", as_name="format", sortable=True),
                TagField("$.is_available", as_name="is_available", sortable=True),
                NumericField("$.price", as_name="price", sortable=True),
                TagField("$.isbn", as_name="isbn", sortable=True),
                GeoField("$.geo", as_name="geo"),
                TextField("$.publisher", as_name="publisher", sortable=True),
                TextField("$.book_series", as_name="book_series", sortable=True),
                TextField("$.main_character", as_name="main_character", sortable=True),
                TextField("$.location", as_name="location", sortable=True),
                TextField("$.address", as_name="address"),
                NumericField("$.edition_number", as_name="edition_number", sortable=True),
                NumericField("$.chapter_count", as_name="chapter_count", sortable=True),
                NumericField("$.review_count", as_name="review_count", sortable=True),
                NumericField("$.citation_count", as_name="citation_count", sortable=True),
                NumericField("$.publishing_delay", as_name="publishing_delay", sortable=True),
                NumericField("$.word_count", as_name="word_count", sortable=True),
                NumericField("$.timestamp", as_name="timestamp", sortable=True),
                NumericField("$.reading_time_minutes", as_name="reading_time_minutes", sortable=True),
                NumericField("$.global_sales", as_name="global_sales", sortable=True),
                NumericField("$.translations_count", as_name="translations_count", sortable=True),
                NumericField("$.author_age_at_publication", as_name="author_age_at_publication", sortable=True),
                NumericField("$.weight_grams", as_name="weight_grams", sortable=True),
                NumericField("$.dimensions.width_cm", as_name="width_cm", sortable=True),
                NumericField("$.dimensions.height_cm", as_name="height_cm", sortable=True),
                NumericField("$.dimensions.depth_cm", as_name="depth_cm", sortable=True),
            ],
            definition=IndexDefinition(
                index_type=IndexType.JSON,
                prefix=[f"{REDIS_KEY_BASE}:"]
            )
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


def normalize_json_search_value(value):
    if value is None:
        return None

    if isinstance(value, list):
        if not value:
            return None
        if len(value) == 1:
            return normalize_json_search_value(value[0])
        return value

    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                if not parsed:
                    return None
                if len(parsed) == 1:
                    return normalize_json_search_value(parsed[0])
                return parsed
            return parsed
        except (json.JSONDecodeError, TypeError):
            return value

    return value


def print_live_status(stop_event):
    while not stop_event.is_set():
        counters = get_counters_snapshot()
        print(
            f"\rCurrent Status, Successful Verification: {counters['data_verification_successful']}, "
            f"Error Verification: {counters['data_verification_error']}, "
            f"Successful Writes: {counters['successful_write']}, "
            f"Unsuccessful Writes: {counters['unsuccessful_write']}",
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
        r.json().set(make_key(0), Path.root_path(), book_data)
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to write data verification. Error: {str(e)}")


def read_data_verification(connection_pool, stop_event, verify_sleep=0.05):
    try:
        r = redis.Redis(connection_pool=connection_pool)
        query = Query('@author:"Alon Shmuely"').return_field("$.title").paging(0, 1)

        while not stop_event.is_set():
            try:
                docs = r.ft(INDEX_NAME).search(query).docs
                if docs:
                    raw_title = docs[0].__dict__.get("$.title")
                    title = normalize_json_search_value(raw_title)

                    if title == "QA architect":
                        increment_counter("data_verification_successful")
                    else:
                        increment_counter("data_verification_error")
                else:
                    increment_counter("data_verification_error")
            except (IndexError, redis.exceptions.ResponseError, redis.exceptions.ConnectionError) as e:
                print(f"\nData verification failed. Error: {str(e)}")
                increment_counter("data_verification_error")

            time.sleep(verify_sleep)

    except redis.exceptions.ConnectionError as e:
        print(f"\nFailed to start data verification. Error: {str(e)}")


def generating_books(connection_pool, max_books, max_random):
    try:
        r = redis.Redis(connection_pool=connection_pool)
        for _ in range(1, max_books + 1):
            book_id = random.randint(1, max_random)
            book_data = generate_random_book(book_id)
            r.json().set(make_key(book_id), Path.root_path(), book_data)
            increment_counter("successful_write")
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to generate books. Error: {str(e)}")
        increment_counter("unsuccessful_write")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Running the book store application v3.1")
    arg_parser.add_argument("--redis", default="redis://localhost:6379", dest="redis_url", help="Redis URL to connect to.")
    arg_parser.add_argument("--max-connections", default=10, type=int, dest="max_connections", help="Maximum number of Redis connections.")
    arg_parser.add_argument("--max-books", default=3000, type=int, dest="max_books", help="Maximum number of books")
    arg_parser.add_argument("--max-random", default=3000, type=int, dest="max_random", help="Maximum random number of books")
    arg_parser.add_argument("--flush", action="store_true", help="Flush the Redis database on startup")
    arg_parser.add_argument("--verify-sleep", default=0.05, type=float, dest="verify_sleep", help="Sleep time in seconds between verification queries")
    args = arg_parser.parse_args()

    try:
        print(f"Connecting to Redis at {args.redis_url} with a max of {args.max_connections} connections")
        redis_pool = create_redis_connection_pool(args.redis_url, args.max_connections)

        if args.flush:
            print("Flushing Redis database...")
            r = redis.Redis(connection_pool=redis_pool)
            r.flushall()

        create_search_index(redis_pool)
        write_data_verification(redis_pool)

        stop_event = threading.Event()
        status_stop_event = threading.Event()

        verification_thread = threading.Thread(
            target=read_data_verification,
            args=(redis_pool, stop_event, args.verify_sleep)
        )
        write_thread = threading.Thread(
            target=generating_books,
            args=(redis_pool, args.max_books, args.max_random)
        )
        status_thread = threading.Thread(
            target=print_live_status,
            args=(status_stop_event,)
        )

        status_thread.start()
        verification_thread.start()
        write_thread.start()

        write_thread.join()
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

    except redis.exceptions.ConnectionError as e:
        print(f"Failed to connect to Redis. Error: {str(e)}")
