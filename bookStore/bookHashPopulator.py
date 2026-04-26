import argparse
import random
import threading
import time

import redis
from faker import Faker
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
                TextField("author", sortable=True),
                TagField("id", sortable=True),
                TextField("description"),
                TagField("editions", separator="|", sortable=True),
                TagField("genres", separator="|", sortable=True),
                NumericField("pages", sortable=True),
                TextField("title", sortable=True),
                NumericField("year_published", sortable=True),
                NumericField("rating_votes", sortable=True),
                NumericField("score", sortable=True),
                TagField("status", separator="|", sortable=True),
                TagField("stock_id", separator="|", sortable=True),
                TagField("format", sortable=True),
                TagField("is_available", sortable=True),
                NumericField("price", sortable=True),
                TagField("isbn", sortable=True),
                GeoField("geo"),
                TextField("publisher", sortable=True),
                TextField("book_series", sortable=True),
                TextField("main_character", sortable=True),
                TextField("location", sortable=True),
                TextField("address"),
                NumericField("edition_number", sortable=True),
                NumericField("chapter_count", sortable=True),
                NumericField("review_count", sortable=True),
                NumericField("citation_count", sortable=True),
                NumericField("publishing_delay", sortable=True),
                NumericField("word_count", sortable=True),
                NumericField("timestamp", sortable=True),
                NumericField("reading_time_minutes", sortable=True),
                NumericField("global_sales", sortable=True),
                NumericField("translations_count", sortable=True),
                NumericField("author_age_at_publication", sortable=True),
                NumericField("weight_grams", sortable=True),
                NumericField("width_cm", sortable=True),
                NumericField("height_cm", sortable=True),
                NumericField("depth_cm", sortable=True),
            ],
            definition=IndexDefinition(
                index_type=IndexType.HASH,
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
        r.hset(make_key(0), mapping=flatten_book_for_hash(book_data))
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to write data verification. Error: {str(e)}")


def read_data_verification(connection_pool, stop_event, verify_sleep=0.05):
    try:
        r = redis.Redis(connection_pool=connection_pool)
        query = Query("Shmuely").return_fields("title").paging(0, 1)

        while not stop_event.is_set():
            try:
                docs = r.ft(INDEX_NAME).search(query).docs
                if docs and getattr(docs[0], "title", None) == "QA architect":
                    increment_counter("data_verification_successful")
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
            r.hset(make_key(book_id), mapping=flatten_book_for_hash(book_data))
            increment_counter("successful_write")
    except redis.exceptions.ConnectionError as e:
        print(f"Failed to generate books. Error: {str(e)}")
        increment_counter("unsuccessful_write")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Running the book store application v2.1")
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
