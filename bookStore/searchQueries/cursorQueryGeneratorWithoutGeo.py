"""
queryGeneratorNoGeo.py
----------------------

Same high-throughput Redis search query generator as ``queryGenerator.py``
for the ``idx:books`` index defined in ``bookSearch.py`` -- but with all
queries that touch the ``@geo`` field removed.

Why a separate file? On the cluster used during development
(``redis-10000.aws-alon-7300.env0.qa.redislabs.com:10000``), every query
that filtered on ``@geo:[lon lat radius unit]`` reproducibly closed the
shard connection and drove the cluster into ``LOADING`` for several
minutes. This variant skips those queries entirely so a load run can
proceed without poisoning the cluster.

Compared to ``queryGenerator.py`` the following factories are removed:
  * q_multi_tag_geo_search          (FT.SEARCH with @geo radius)
  * q_geo_radius_sorted             (FT.SEARCH with @geo radius)
  * q_agg_geo_distance_buckets      (FT.AGGREGATE using geodistance(@geo, ...))

Everything else (CLI, --test-mode, throughput design) is identical.

Usage examples
--------------

    # Run for 60 seconds against localhost with 100 connections:
    python queryGenerator.py --host 127.0.0.1 --port 6379 \
                             --connections 100 --duration 60

    # Run exactly 1,000,000 queries with 200 connections:
    python queryGenerator.py --host redis.example.com --port 6379 \
                             --connections 200 --total-queries 1000000

    # Stop on whichever limit hits first:
    python queryGenerator.py --connections 50 --duration 30 \
                             --total-queries 500000

    # Test mode: run each query once and print results:
    python queryGenerator.py --host 127.0.0.1 --port 6379 --test-mode

The script aims for maximum throughput:
  * one shared :class:`redis.ConnectionPool` sized to ``--connections``
  * a :class:`ThreadPoolExecutor` with ``--connections`` workers, each
    holding its own :class:`redis.Redis` client (all backed by the pool)
  * pre-built query factories; no per-iteration object churn beyond the
    randomised inputs
  * lock-free per-worker counters, merged at the end
  * a light background thread that prints live QPS without blocking
    workers
"""

from __future__ import annotations

import argparse
import random
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

import redis
from faker import Faker

import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.search.aggregation import AggregateRequest
from redis.commands.search.query import NumericFilter, Query


INDEX_NAME = "idx:books"

fake = Faker()


# ---------------------------------------------------------------------------
# Query factories
# ---------------------------------------------------------------------------
#
# Each factory returns a callable that, given a :class:`redis.Redis` client,
# executes one query against Redis and returns whatever Redis returns.
# Factories are used instead of pre-built :class:`Query` objects so every
# call can use fresh random inputs without the workers sharing mutable
# state.
# ---------------------------------------------------------------------------


def q_faceted_fuzzy_search() -> Callable[[redis.Redis], object]:
    """Highly-rated fantasy/sci-fi with fuzzy+phrase text matching."""

    def run(r: redis.Redis):
        word = fake.word()
        q = (
            Query(
                f"((@title|description:(%{word}% | \"ancient kingdom\"=>"
                f"{{$slop:2; $inorder:true}})) => {{$weight:2.0}}) "
                "(@genres:{fantasy|science\\ fiction}) "
                "(@format:{hardcover|ebook}) "
                "(@is_available:{True}) "
                "(@year_published:[(1990 +inf]) "
                "(@score:[4 +inf]) "
                "(@price:[-inf (50]) "
                "-@author:\"Alon Shmuely\""
            )
            .return_fields(
                "title", "author", "price",
                "year_published", "genres", "description",
            )
            # Alias the @score index field to "book_score" so it does not
            # collide with the document score produced by WITHSCORES
            # (redis-py's Document ctor raises TypeError on that clash).
            .return_field("score", as_field="book_score")
            .summarize(fields=["description"], context_len=15,
                       num_frags=2, sep=" ... ")
            .highlight(fields=["title", "description"], tags=("<b>", "</b>"))
            .scorer("BM25")
            .with_scores()
            .sort_by("score", asc=False)
            .paging(0, 25)
            .dialect(2)
        )
        q.add_filter(NumericFilter("rating_votes", 200, NumericFilter.INF))
        return r.ft(INDEX_NAME).search(q).docs

    return run


def q_multi_tag_intersection_search() -> Callable[[redis.Redis], object]:
    """Multi-tag intersection + numeric grid + multi-field sort (no geo)."""

    def run(r: redis.Redis):
        q = (
            Query(
                "(@genres:{mystery}) (@genres:{thriller}) "
                "(@editions:{english}) (@editions:{french}) "
                "(@status:{for_sale}) "
                "(@word_count:[80000 150000]) "
                "(@chapter_count:[15 40]) "
                "(@weight_grams:[(0 +inf]) "
                "(@year_published:[1950 2023])"
            )
            .return_fields("title", "author", "score",
                           "year_published", "format")
            .sort_by("score", asc=False)
            .paging(0, 30)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).search(q).docs

    return run


def q_optional_boost_search() -> Callable[[redis.Redis], object]:
    """Optional clauses that only boost ranking."""

    def run(r: redis.Redis):
        q = (
            Query(
                "(@format:{paperback}) (@is_available:{True}) "
                "(@price:[-inf 30]) "
                "~(@description:love | @main_character:Emma) "
                "~@year_published:[2015 +inf]"
            )
            .return_fields("title", "main_character",
                           "year_published", "price")
            .scorer("TFIDF.DOCNORM")
            .with_scores()
            .paging(0, 50)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).search(q).docs

    return run


def q_score_band_sorted() -> Callable[[redis.Redis], object]:
    """Score-band paginated search, sorted by score (replaces geo-radius variant)."""

    score_bands = [
        (3.0, 3.5),
        (3.5, 4.0),
        (4.0, 4.5),
        (4.5, 5.0),
    ]

    def run(r: redis.Redis):
        lo, hi = random.choice(score_bands)
        q = (
            Query(
                f"(@is_available:{{True}}) (@score:[{lo} {hi}]) "
                f"(@rating_votes:[10 +inf]) "
                f"(@year_published:[1950 2023])"
            )
            .return_fields("title", "author", "price", "score")
            .sort_by("score", asc=False)
            .paging(0, 100)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).search(q).docs

    return run


def q_agg_author_productivity() -> Callable[[redis.Redis], object]:
    """Per-author productivity with QUANTILE / STDDEV reducers."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("*")
            .load("@author")
            .apply(sales_per_page="@global_sales / @pages")
            .group_by(
                ["@author"],
                reducers.count().alias("book_count"),
                reducers.avg("@score").alias("avg_score"),
                reducers.quantile("@price", 0.5).alias("median_price"),
                reducers.quantile("@global_sales", 0.95).alias("p95_sales"),
                reducers.stddev("@word_count").alias("wc_stddev"),
                reducers.avg("@sales_per_page").alias("avg_sales_per_page"),
            )
            .filter("@book_count >= 3 && @avg_score >= 3.5")
            .sort_by(aggregations.Desc("@p95_sales"))
            .limit(0, 100)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).aggregate(req).rows

    return run


def q_agg_decade_format_buckets() -> Callable[[redis.Redis], object]:
    """Decade x format bucketed aggregate with TOLIST / FIRST_VALUE (no geo)."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest(
                "(@genres:{fantasy}) (@year_published:[2000 +inf])"
            )
            .load("@title", "@format", "@global_sales", "@year_published")
            .apply(decade="floor(@year_published/10)*10")
            .apply(fmt="upper(@format)")
            .group_by(
                ["@decade", "@fmt"],
                reducers.count().alias("books"),
                reducers.tolist("@title").alias("sample_titles"),
                reducers.first_value("@title",
                                     aggregations.Desc("@global_sales"))
                        .alias("bestseller"),
                reducers.max("@global_sales").alias("top_sales"),
            )
            .sort_by(
                aggregations.Asc("@decade"),
                aggregations.Desc("@books"),
            )
            .limit(0, 40)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).aggregate(req).rows

    return run


def q_agg_publisher_leaderboard() -> Callable[[redis.Redis], object]:
    """Two-stage GROUPBY with top-publisher share per decade."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("(@is_available:{True})")
            .load("@publisher", "@global_sales", "@year_published")
            .apply(decade="floor(@year_published/10)*10")
            .group_by(
                ["@decade", "@publisher"],
                reducers.sum("@global_sales").alias("pub_sales"),
                reducers.count().alias("pub_books"),
            )
            .group_by(
                ["@decade"],
                reducers.sum("@pub_sales").alias("decade_sales"),
                reducers.first_value("@publisher",
                                     aggregations.Desc("@pub_sales"))
                        .alias("top_publisher"),
                reducers.max("@pub_sales").alias("top_publisher_sales"),
            )
            .apply(top_share="@top_publisher_sales / @decade_sales")
            .filter("@top_share > 0.01")
            .sort_by(aggregations.Asc("@decade"))
            .dialect(2)
        )
        return r.ft(INDEX_NAME).aggregate(req).rows

    return run


def q_agg_reading_efficiency() -> Callable[[redis.Redis], object]:
    """Reading-efficiency buckets with QUANTILE + RANDOM_SAMPLE."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest(
                "(@reading_time_minutes:[(0 +inf]) "
                "(@publishing_delay:[0 +inf])"
            )
            .load("@title", "@pages", "@chapter_count",
                  "@price", "@word_count")
            .apply(wpm="@word_count / @reading_time_minutes")
            .apply(wpm_bucket="floor( log(@wpm + 1) )")
            .group_by(
                ["@wpm_bucket"],
                reducers.count().alias("books"),
                reducers.avg("@pages").alias("avg_pages"),
                reducers.avg("@chapter_count").alias("avg_chapters"),
                reducers.quantile("@price", 0.5).alias("median_price"),
                reducers.quantile("@wpm", 0.9).alias("p90_wpm"),
                reducers.random_sample("@title", 5).alias("sample_titles"),
            )
            .sort_by(aggregations.Asc("@wpm_bucket"))
            .dialect(2)
        )
        return r.ft(INDEX_NAME).aggregate(req).rows

    return run


def q_parameterized_price_window() -> Callable[[redis.Redis], object]:
    """Parameterized FT.SEARCH with $params binding and DIALECT 2."""

    def run(r: redis.Redis):
        lo = random.randint(1900, 1990)
        hi = lo + random.randint(5, 40)
        min_score = round(random.uniform(2.5, 4.5), 2)
        max_price = random.choice([20, 35, 50, 75, 100])
        q = (
            Query(
                "(@year_published:[$lo $hi]) "
                "(@score:[$min_score +inf]) "
                "(@price:[-inf ($max_price]) "
                "(@is_available:{True})"
            )
            .return_fields("title", "author", "year_published",
                           "score", "price")
            .sort_by("year_published", asc=True)
            .paging(0, 50)
            .dialect(2)
        )
        params = {
            "lo": lo,
            "hi": hi,
            "min_score": min_score,
            "max_price": max_price,
        }
        return r.ft(INDEX_NAME).search(q, query_params=params).docs

    return run


def q_prefix_suffix_infix_text() -> Callable[[redis.Redis], object]:
    """Prefix / suffix / infix wildcard TEXT matching with VERBATIM."""

    def run(r: redis.Redis):
        prefix = fake.lexify(text="???").lower()
        infix = fake.lexify(text="??").lower()
        suffix = fake.lexify(text="??").lower()
        q = (
            Query(
                f"(@title:{prefix}*) "
                f"(@description:*{infix}*) "
                f"(@author:*{suffix}) "
                f"(@is_available:{{True}})"
            )
            .verbatim()
            .return_fields("title", "author", "description")
            .paging(0, 20)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).search(q).docs

    return run


def q_negative_heavy_search() -> Callable[[redis.Redis], object]:
    """Negation-heavy query: exclude genres, statuses, formats, keywords."""

    def run(r: redis.Redis):
        q = (
            Query(
                "(@is_available:{True}) "
                "-(@genres:{horror|true\\ crime}) "
                "-(@status:{maintenance|on_loan}) "
                "-(@format:{ebook}) "
                "-(@description:(violence|gore)) "
                "-@author:\"Alon Shmuely\" "
                "(@rating_votes:[50 +inf]) "
                "(@score:[3 +inf])"
            )
            .return_fields("title", "author", "genres",
                           "format", "score", "rating_votes")
            .sort_by("rating_votes", asc=False)
            .paging(0, 40)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).search(q).docs

    return run


def q_agg_author_distinct_genres() -> Callable[[redis.Redis], object]:
    """Author versatility: distinct genres/editions + date-formatted activity."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("(@is_available:{True})")
            .load("@author", "@timestamp")
            .apply(last_active="timefmt(@timestamp, \"%Y-%m\")")
            .group_by(
                ["@author"],
                reducers.count().alias("books"),
                reducers.count_distinct("@genres").alias("distinct_genres"),
                reducers.count_distinctish("@editions")
                        .alias("distinct_editions_approx"),
                reducers.max("@timestamp").alias("last_ts"),
                reducers.tolist("@last_active").alias("active_months"),
                reducers.avg("@score").alias("avg_score"),
            )
            .apply(
                last_active_str="timefmt(@last_ts, \"%Y-%m-%d\")"
            )
            .filter("@books >= 2 && @distinct_genres >= 2")
            .sort_by(aggregations.Desc("@distinct_genres"),
                     aggregations.Desc("@books"))
            .limit(0, 50)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).aggregate(req).rows

    return run


def q_agg_price_per_page_leaders() -> Callable[[redis.Redis], object]:
    """Value-for-money leaderboard: price/page, citation/review, size vs weight."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest(
                "(@is_available:{True}) (@pages:[(0 +inf]) "
                "(@review_count:[(0 +inf])"
            )
            .load("@title", "@author", "@price", "@pages",
                  "@citation_count", "@review_count", "@weight_grams",
                  "@width_cm", "@height_cm", "@depth_cm")
            .apply(price_per_page="@price / @pages")
            .apply(citation_ratio="@citation_count / @review_count")
            .apply(volume_cm3="@width_cm * @height_cm * @depth_cm")
            .apply(density_g_cm3="@weight_grams / @volume_cm3")
            .filter("@price_per_page > 0 && @volume_cm3 > 0")
            .group_by(
                ["@author"],
                reducers.count().alias("books"),
                reducers.avg("@price_per_page").alias("avg_ppp"),
                reducers.quantile("@price_per_page", 0.5)
                        .alias("median_ppp"),
                reducers.avg("@citation_ratio").alias("avg_citation_ratio"),
                reducers.avg("@density_g_cm3").alias("avg_density"),
                reducers.first_value(
                    "@title", aggregations.Asc("@price_per_page")
                ).alias("cheapest_title"),
            )
            .filter("@books >= 2")
            .sort_by(aggregations.Asc("@median_ppp"))
            .limit(0, 25)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).aggregate(req).rows

    return run


def q_agg_cursor_catalog_dashboard() -> Callable[[redis.Redis], object]:
    """Very complex WITHCURSOR aggregate, drained end-to-end.

    Builds a multi-stage FT.AGGREGATE (many APPLYs, fine-grained GROUPBY,
    double-filter, multi-key SORTBY) that produces enough rows for the
    cursor to actually page. Then drains the cursor via FT.CURSOR READ
    until it is exhausted or a hard page cap fires; on early exit it
    explicitly runs FT.CURSOR DEL so no cursor is leaked on the server.

    Returns a dict:
        {"pages": <cursor pages consumed>,
         "cursor_id": <0 on full drain, else the id that was DEL'd>,
         "rows": <all rows merged across pages>}
    """

    PAGE_COUNT = 50          # rows per cursor page (FT.CURSOR READ COUNT)
    MAX_IDLE_SEC = 5          # server-side idle TTL before reaping (seconds)
    MAX_PAGES = 40           # hard cap so one call can't monopolize a worker

    def run(r: redis.Redis):
        lo_year = random.randint(1900, 1990)
        hi_year = min(lo_year + random.randint(10, 60), 2023)
        min_votes = random.choice([10, 50, 100, 200])
        max_delay = random.choice([200, 500, 1000])

        req = (
            AggregateRequest(
                f"(@is_available:{{True}}) "
                f"(@year_published:[{lo_year} {hi_year}]) "
                f"(@rating_votes:[{min_votes} +inf]) "
                f"(@pages:[(0 +inf]) "
                f"(@weight_grams:[(0 +inf]) "
                f"(@publishing_delay:[-inf {max_delay}])"
            )
            .load(
                "@author", "@title", "@publisher", "@main_character",
                "@year_published", "@score", "@rating_votes",
                "@pages", "@price", "@global_sales", "@word_count",
                "@chapter_count", "@weight_grams",
                "@width_cm", "@height_cm", "@depth_cm",
                "@translations_count", "@format",
            )
            .apply(decade="floor(@year_published/10)*10")
            .apply(price_per_page="@price / @pages")
            .apply(
                value_score="(@score * log(@rating_votes + 1)) "
                            "/ (@price_per_page + 0.01)"
            )
            .apply(text_density="@word_count / @chapter_count")
            .apply(volume_cm3="@width_cm * @height_cm * @depth_cm")
            .apply(density_g_cm3="@weight_grams / @volume_cm3")
            .apply(fmt_up="upper(@format)")
            .filter("@volume_cm3 > 0 && @price_per_page > 0")
            # Fine-grained bucket so the cursor has pages to drain.
            .group_by(
                ["@author", "@decade", "@fmt_up"],
                reducers.count().alias("books"),
                reducers.sum("@global_sales").alias("bucket_sales"),
                reducers.avg("@score").alias("avg_score"),
                reducers.avg("@translations_count").alias("avg_tx"),
                reducers.quantile("@value_score", 0.5)
                        .alias("median_value_score"),
                reducers.quantile("@value_score", 0.9)
                        .alias("p90_value_score"),
                reducers.stddev("@text_density").alias("stddev_density"),
                reducers.max("@global_sales").alias("top_sales"),
                reducers.min("@price").alias("min_price"),
                reducers.count_distinct("@publisher")
                        .alias("distinct_publishers"),
                reducers.first_value(
                    "@title", aggregations.Desc("@value_score")
                ).alias("top_value_title"),
                reducers.first_value(
                    "@publisher", aggregations.Desc("@global_sales")
                ).alias("top_publisher"),
                reducers.random_sample("@main_character", 2)
                        .alias("sample_chars"),
                reducers.tolist("@publisher").alias("publishers"),
            )
            .apply(sales_per_book="@bucket_sales / @books")
            .apply(
                bucket_summary="format("
                               "\"%s/%d/%s: %d books, top=%s, $%.2f/book\", "
                               "@author, @decade, @fmt_up, @books, "
                               "@top_value_title, @sales_per_book)"
            )
            .filter("@books >= 1 && @median_value_score > 0")
            .sort_by(
                aggregations.Asc("@decade"),
                aggregations.Desc("@bucket_sales"),
                aggregations.Desc("@p90_value_score"),
            )
            .cursor(count=PAGE_COUNT, max_idle=MAX_IDLE_SEC)
            .dialect(2)
        )

        pages = 0
        total_rows: List = []
        ft = r.ft(INDEX_NAME)

        result = ft.aggregate(req)
        total_rows.extend(result.rows)
        pages += 1
        cursor = result.cursor
        final_cid = cursor.cid if cursor is not None else 0

        try:
            while (
                cursor is not None
                and cursor.cid != 0
                and pages < MAX_PAGES
            ):
                cursor.count = PAGE_COUNT
                next_result = ft.aggregate(cursor)
                total_rows.extend(next_result.rows)
                pages += 1
                cursor = next_result.cursor
                final_cid = cursor.cid if cursor is not None else 0
        finally:
            # If we stopped early (page cap / exception), release the
            # cursor so it doesn't sit on the server until MAXIDLE.
            if (
                cursor is not None
                and cursor.cid != 0
                and pages >= MAX_PAGES
            ):
                try:
                    r.execute_command(
                        "FT.CURSOR", "DEL", INDEX_NAME, cursor.cid
                    )
                except redis.exceptions.ResponseError:
                    pass

        return {
            "pages": pages,
            "cursor_id": final_cid,
            "rows_total": len(total_rows),
            "rows": total_rows,
        }

    return run


def q_agg_inventory_status_funnel() -> Callable[[redis.Redis], object]:
    """Inventory funnel per format: counts per status with string formatting."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("*")
            .load("@format", "@status", "@price", "@title")
            .apply(fmt_up="upper(@format)")
            .apply(status_lc="lower(@status)")
            .group_by(
                ["@fmt_up", "@status_lc"],
                reducers.count().alias("count"),
                reducers.avg("@price").alias("avg_price"),
                reducers.min("@price").alias("min_price"),
                reducers.max("@price").alias("max_price"),
                reducers.random_sample("@title", 3).alias("samples"),
            )
            .apply(
                summary="format("
                        "\"%s/%s: %d books, avg $%.2f\", "
                        "@fmt_up, @status_lc, @count, @avg_price)"
            )
            .sort_by(aggregations.Asc("@fmt_up"),
                     aggregations.Desc("@count"))
            .limit(0, 40)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).aggregate(req).rows

    return run


PAGE_COUNT_DEFAULT = 50
MAX_IDLE_DEFAULT = 10         # seconds; redis-py multiplies by 1000 -> ms
MAX_PAGES_DEFAULT = 40        # hard cap so one call can't monopolize a worker


def _drain_cursor(
    ft,
    r: redis.Redis,
    req: AggregateRequest,
    page_count: int = PAGE_COUNT_DEFAULT,
    max_pages: int = MAX_PAGES_DEFAULT,
) -> Dict[str, object]:
    """Execute an aggregate with WITHCURSOR and drain the cursor.

    On early exit (hard page cap or exception), explicitly issues
    FT.CURSOR DEL so the cursor doesn't linger on the server until
    MAXIDLE.

    Returns ``{"pages", "cursor_id", "rows_total", "rows"}`` so
    --test-mode can report drain metadata.
    """

    pages = 0
    total_rows: List = []

    result = ft.aggregate(req)
    total_rows.extend(result.rows)
    pages += 1
    cursor = result.cursor
    final_cid = cursor.cid if cursor is not None else 0

    try:
        while (
            cursor is not None
            and cursor.cid != 0
            and pages < max_pages
        ):
            cursor.count = page_count
            next_result = ft.aggregate(cursor)
            total_rows.extend(next_result.rows)
            pages += 1
            cursor = next_result.cursor
            final_cid = cursor.cid if cursor is not None else 0
    finally:
        if (
            cursor is not None
            and cursor.cid != 0
            and pages >= max_pages
        ):
            try:
                r.execute_command(
                    "FT.CURSOR", "DEL", INDEX_NAME, cursor.cid
                )
            except redis.exceptions.ResponseError:
                pass

    return {
        "pages": pages,
        "cursor_id": final_cid,
        "rows_total": len(total_rows),
        "rows": total_rows,
    }


def q_cur_decade_format_matrix() -> Callable[[redis.Redis], object]:
    """Per (decade, format) with QUANTILE/STDDEV/COUNT_DISTINCT; cursor-drained."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("(@is_available:{True})")
            .load("@year_published", "@format", "@price", "@pages")
            .apply(decade="floor(@year_published/10)*10")
            .apply(fmt_up="upper(@format)")
            .group_by(
                ["@decade", "@fmt_up"],
                reducers.count().alias("books"),
                reducers.avg("@score").alias("avg_score"),
                reducers.quantile("@price", 0.5).alias("median_price"),
                reducers.stddev("@pages").alias("stddev_pages"),
                reducers.count_distinct("@publisher").alias("distinct_pubs"),
                reducers.min("@global_sales").alias("min_sales"),
                reducers.max("@global_sales").alias("max_sales"),
            )
            .sort_by(aggregations.Asc("@decade"), aggregations.Asc("@fmt_up"))
            .cursor(count=PAGE_COUNT_DEFAULT, max_idle=MAX_IDLE_DEFAULT)
            .dialect(2)
        )
        return _drain_cursor(r.ft(INDEX_NAME), r, req)

    return run


def q_cur_author_deep_stats() -> Callable[[redis.Redis], object]:
    """Per-author deep stats: distinct genres/editions, score quantiles, year extremes; cursor-drained."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("*")
            .load("@author", "@word_count", "@year_published")
            .group_by(
                ["@author"],
                reducers.count().alias("books"),
                reducers.count_distinct("@genres").alias("distinct_genres"),
                reducers.count_distinctish("@editions")
                        .alias("approx_editions"),
                reducers.quantile("@score", 0.5).alias("median_score"),
                reducers.quantile("@score", 0.9).alias("p90_score"),
                reducers.stddev("@word_count").alias("wc_stddev"),
                reducers.min("@year_published").alias("first_year"),
                reducers.max("@year_published").alias("last_year"),
                reducers.random_sample("@title", 3).alias("sample_titles"),
            )
            .filter("@books >= 2")
            .sort_by(aggregations.Desc("@distinct_genres"))
            .cursor(count=PAGE_COUNT_DEFAULT, max_idle=MAX_IDLE_DEFAULT)
            .dialect(2)
        )
        return _drain_cursor(r.ft(INDEX_NAME), r, req)

    return run


def q_cur_publisher_market_share() -> Callable[[redis.Redis], object]:
    """Per (decade, publisher): sales, books, distinct authors, top title; cursor-drained."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("(@is_available:{True})")
            .load("@publisher", "@author", "@global_sales", "@year_published")
            .apply(decade="floor(@year_published/10)*10")
            .group_by(
                ["@decade", "@publisher"],
                reducers.count().alias("books"),
                reducers.sum("@global_sales").alias("pub_sales"),
                reducers.count_distinct("@author").alias("distinct_authors"),
                reducers.first_value(
                    "@title", aggregations.Desc("@global_sales")
                ).alias("top_title"),
            )
            .filter("@books >= 1")
            .sort_by(aggregations.Asc("@decade"),
                     aggregations.Desc("@pub_sales"))
            .cursor(count=40, max_idle=MAX_IDLE_DEFAULT)
            .dialect(2)
        )
        return _drain_cursor(r.ft(INDEX_NAME), r, req, page_count=40)

    return run


def q_cur_price_band_profile() -> Callable[[redis.Redis], object]:
    """Price-band profile: distinct formats/genres, avg score, p75 votes; cursor-drained."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("(@price:[(0 +inf])")
            .load("@price", "@title")
            .apply(price_band="floor(@price/10)*10")
            .group_by(
                ["@price_band"],
                reducers.count().alias("books"),
                reducers.count_distinct("@format").alias("distinct_formats"),
                reducers.count_distinct("@genres").alias("distinct_genres"),
                reducers.avg("@score").alias("avg_score"),
                reducers.quantile("@rating_votes", 0.75).alias("p75_votes"),
                reducers.random_sample("@title", 3).alias("samples"),
            )
            .sort_by(aggregations.Asc("@price_band"))
            .cursor(count=30, max_idle=MAX_IDLE_DEFAULT)
            .dialect(2)
        )
        return _drain_cursor(r.ft(INDEX_NAME), r, req, page_count=30)

    return run


def q_cur_rating_tier_stats() -> Callable[[redis.Redis], object]:
    """Rating-votes tiers (floor/100*100): score, price, authors, year extremes; cursor-drained."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("*")
            .load("@rating_votes", "@title")
            .apply(vote_tier="floor(@rating_votes/100)*100")
            .group_by(
                ["@vote_tier"],
                reducers.count().alias("books"),
                reducers.avg("@score").alias("avg_score"),
                reducers.quantile("@price", 0.5).alias("median_price"),
                reducers.count_distinct("@author").alias("distinct_authors"),
                reducers.min("@year_published").alias("min_year"),
                reducers.max("@year_published").alias("max_year"),
                reducers.first_value(
                    "@title", aggregations.Desc("@score")
                ).alias("top_scored_title"),
            )
            .sort_by(aggregations.Asc("@vote_tier"))
            .cursor(count=30, max_idle=MAX_IDLE_DEFAULT)
            .dialect(2)
        )
        return _drain_cursor(r.ft(INDEX_NAME), r, req, page_count=30)

    return run


def q_cur_score_buckets() -> Callable[[redis.Redis], object]:
    """Score bucket floor: pages, sales, translations, formats, top voted; cursor-drained."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("*")
            .load("@score", "@title", "@global_sales")
            .apply(score_bucket="floor(@score)")
            .group_by(
                ["@score_bucket"],
                reducers.count().alias("books"),
                reducers.avg("@pages").alias("avg_pages"),
                reducers.quantile("@global_sales", 0.9).alias("p90_sales"),
                reducers.sum("@translations_count").alias("total_tx"),
                reducers.count_distinct("@format").alias("distinct_formats"),
                reducers.first_value(
                    "@title", aggregations.Desc("@rating_votes")
                ).alias("top_voted"),
            )
            .sort_by(aggregations.Asc("@score_bucket"))
            .cursor(count=30, max_idle=MAX_IDLE_DEFAULT)
            .dialect(2)
        )
        return _drain_cursor(r.ft(INDEX_NAME), r, req, page_count=30)

    return run


def q_cur_reading_hours() -> Callable[[redis.Redis], object]:
    """Reading-time hour buckets: word_count, pages, sales, authors, chars; cursor-drained."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("(@reading_time_minutes:[(0 +inf])")
            .load("@reading_time_minutes", "@word_count", "@title")
            .apply(hours="floor(@reading_time_minutes/60)")
            .group_by(
                ["@hours"],
                reducers.count().alias("books"),
                reducers.avg("@word_count").alias("avg_wc"),
                reducers.quantile("@pages", 0.5).alias("median_pages"),
                reducers.quantile("@global_sales", 0.95).alias("p95_sales"),
                reducers.count_distinct("@author").alias("distinct_authors"),
                reducers.random_sample("@main_character", 3)
                        .alias("sample_chars"),
            )
            .sort_by(aggregations.Asc("@hours"))
            .cursor(count=40, max_idle=MAX_IDLE_DEFAULT)
            .dialect(2)
        )
        return _drain_cursor(r.ft(INDEX_NAME), r, req, page_count=40)

    return run


def q_cur_volume_density() -> Callable[[redis.Redis], object]:
    """Density bands (weight/volume * 10 floored/10): pages, price, formats; cursor-drained."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("(@weight_grams:[(0 +inf])")
            .load("@width_cm", "@height_cm", "@depth_cm", "@weight_grams")
            .apply(volume="@width_cm * @height_cm * @depth_cm")
            .apply(density="@weight_grams / @volume")
            .apply(density_band="floor(@density*10)/10")
            .filter("@volume > 0 && @density > 0")
            .group_by(
                ["@density_band"],
                reducers.count().alias("books"),
                reducers.avg("@pages").alias("avg_pages"),
                reducers.quantile("@price", 0.5).alias("median_price"),
                reducers.count_distinct("@format").alias("distinct_formats"),
                reducers.first_value(
                    "@title", aggregations.Desc("@translations_count")
                ).alias("most_translated"),
            )
            .sort_by(aggregations.Asc("@density_band"))
            .cursor(count=30, max_idle=MAX_IDLE_DEFAULT)
            .dialect(2)
        )
        return _drain_cursor(r.ft(INDEX_NAME), r, req, page_count=30)

    return run


def q_cur_chapter_efficiency() -> Callable[[redis.Redis], object]:
    """Log-bucket of words-per-chapter: score, price, genres, samples; cursor-drained."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("(@chapter_count:[(0 +inf])")
            .load("@word_count", "@chapter_count", "@title")
            .apply(chap_density="@word_count / @chapter_count")
            .apply(density_bucket="floor(log(@chap_density + 1))")
            .filter("@chap_density > 0")
            .group_by(
                ["@density_bucket"],
                reducers.count().alias("books"),
                reducers.avg("@score").alias("avg_score"),
                reducers.quantile("@price", 0.75).alias("p75_price"),
                reducers.count_distinct("@genres").alias("distinct_genres"),
                reducers.random_sample("@title", 3).alias("samples"),
            )
            .sort_by(aggregations.Asc("@density_bucket"))
            .cursor(count=30, max_idle=MAX_IDLE_DEFAULT)
            .dialect(2)
        )
        return _drain_cursor(r.ft(INDEX_NAME), r, req, page_count=30)

    return run


def q_cur_citation_leaders() -> Callable[[redis.Redis], object]:
    """Per-author citation leaders: citation/review ratio, distinct publishers; cursor-drained."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("(@review_count:[(0 +inf])")
            .load("@author", "@citation_count", "@review_count", "@title")
            .apply(cite_ratio="@citation_count / @review_count")
            .group_by(
                ["@author"],
                reducers.count().alias("books"),
                reducers.avg("@cite_ratio").alias("avg_cite_ratio"),
                reducers.quantile("@citation_count", 0.5).alias("median_cites"),
                reducers.sum("@review_count").alias("total_reviews"),
                reducers.count_distinct("@publisher").alias("distinct_pubs"),
                reducers.first_value(
                    "@title", aggregations.Desc("@citation_count")
                ).alias("top_cited"),
            )
            .filter("@books >= 2")
            .sort_by(aggregations.Desc("@avg_cite_ratio"))
            .cursor(count=40, max_idle=MAX_IDLE_DEFAULT)
            .dialect(2)
        )
        return _drain_cursor(r.ft(INDEX_NAME), r, req, page_count=40)

    return run


def q_cur_translation_reach() -> Callable[[redis.Redis], object]:
    """Per (publisher, decade) translations: author age, sales, distinct chars; cursor-drained."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("(@translations_count:[(0 +inf])")
            .load("@publisher", "@translations_count",
                  "@author_age_at_publication", "@year_published",
                  "@global_sales")
            .apply(decade="floor(@year_published/10)*10")
            .group_by(
                ["@publisher", "@decade"],
                reducers.count().alias("books"),
                reducers.sum("@translations_count").alias("total_tx"),
                reducers.avg("@author_age_at_publication").alias("avg_age"),
                reducers.quantile("@global_sales", 0.9).alias("p90_sales"),
                reducers.count_distinct("@main_character")
                        .alias("distinct_chars"),
                reducers.random_sample("@genres", 3).alias("sample_genres"),
            )
            .filter("@books >= 1")
            .sort_by(aggregations.Asc("@decade"),
                     aggregations.Desc("@total_tx"))
            .cursor(count=40, max_idle=MAX_IDLE_DEFAULT)
            .dialect(2)
        )
        return _drain_cursor(r.ft(INDEX_NAME), r, req, page_count=40)

    return run


def q_cur_edition_number_stats() -> Callable[[redis.Redis], object]:
    """Per edition_number: price, word_count, distinct authors, bestseller; cursor-drained."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("*")
            .load("@edition_number", "@price", "@word_count")
            .group_by(
                ["@edition_number"],
                reducers.count().alias("books"),
                reducers.avg("@price").alias("avg_price"),
                reducers.quantile("@word_count", 0.5).alias("median_wc"),
                reducers.count_distinct("@author").alias("distinct_authors"),
                reducers.first_value(
                    "@title", aggregations.Desc("@global_sales")
                ).alias("bestseller"),
                reducers.max("@global_sales").alias("top_sales"),
            )
            .sort_by(aggregations.Asc("@edition_number"))
            .cursor(count=20, max_idle=MAX_IDLE_DEFAULT)
            .dialect(2)
        )
        return _drain_cursor(r.ft(INDEX_NAME), r, req, page_count=20)

    return run


def q_cur_author_age_bands() -> Callable[[redis.Redis], object]:
    """Per (author_age_band, decade): score, votes, genres, top title; cursor-drained."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("(@author_age_at_publication:[(0 +inf])")
            .load("@author_age_at_publication", "@year_published")
            .apply(age_band="floor(@author_age_at_publication/10)*10")
            .apply(decade="floor(@year_published/10)*10")
            .group_by(
                ["@age_band", "@decade"],
                reducers.count().alias("books"),
                reducers.avg("@score").alias("avg_score"),
                reducers.quantile("@rating_votes", 0.5).alias("median_votes"),
                reducers.count_distinct("@genres").alias("distinct_genres"),
                reducers.first_value(
                    "@title", aggregations.Desc("@score")
                ).alias("top_title"),
            )
            .sort_by(aggregations.Asc("@age_band"),
                     aggregations.Asc("@decade"))
            .cursor(count=40, max_idle=MAX_IDLE_DEFAULT)
            .dialect(2)
        )
        return _drain_cursor(r.ft(INDEX_NAME), r, req, page_count=40)

    return run


def q_cur_format_availability() -> Callable[[redis.Redis], object]:
    """Per (format, is_available): stddev price, distinct authors, sample publishers; cursor-drained."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("*")
            .load("@format", "@is_available", "@price", "@publisher")
            .apply(fmt_up="upper(@format)")
            .group_by(
                ["@fmt_up", "@is_available"],
                reducers.count().alias("books"),
                reducers.avg("@pages").alias("avg_pages"),
                reducers.quantile("@word_count", 0.5).alias("median_wc"),
                reducers.stddev("@price").alias("stddev_price"),
                reducers.count_distinct("@author").alias("distinct_authors"),
                reducers.random_sample("@publisher", 3).alias("sample_pubs"),
            )
            .sort_by(aggregations.Asc("@fmt_up"))
            .cursor(count=20, max_idle=MAX_IDLE_DEFAULT)
            .dialect(2)
        )
        return _drain_cursor(r.ft(INDEX_NAME), r, req, page_count=20)

    return run


def q_cur_timestamp_monthly() -> Callable[[redis.Redis], object]:
    """Monthly time series via timefmt: sales, votes, authors, samples; cursor-drained."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("(@timestamp:[(0 +inf])")
            .load("@timestamp", "@title", "@global_sales")
            .apply(ym="timefmt(@timestamp,\"%Y-%m\")")
            .group_by(
                ["@ym"],
                reducers.count().alias("books"),
                reducers.avg("@global_sales").alias("avg_sales"),
                reducers.quantile("@rating_votes", 0.9).alias("p90_votes"),
                reducers.count_distinct("@author").alias("distinct_authors"),
                reducers.random_sample("@title", 3).alias("samples"),
            )
            .sort_by(aggregations.Asc("@ym"))
            .cursor(count=40, max_idle=MAX_IDLE_DEFAULT)
            .dialect(2)
        )
        return _drain_cursor(r.ft(INDEX_NAME), r, req, page_count=40)

    return run


# The full pool of query factories the generator will cycle through.
ALL_QUERIES: List[Callable[[], Callable[[redis.Redis], object]]] = [
    q_faceted_fuzzy_search,
    q_multi_tag_intersection_search,
    q_optional_boost_search,
    q_score_band_sorted,
    q_parameterized_price_window,
    q_prefix_suffix_infix_text,
    q_negative_heavy_search,
    q_agg_author_productivity,
    q_agg_decade_format_buckets,
    q_agg_publisher_leaderboard,
    q_agg_reading_efficiency,
    q_agg_author_distinct_genres,
    q_agg_price_per_page_leaders,
    q_agg_inventory_status_funnel,
    q_agg_cursor_catalog_dashboard,
    q_cur_decade_format_matrix,
    q_cur_author_deep_stats,
    q_cur_publisher_market_share,
    q_cur_price_band_profile,
    q_cur_rating_tier_stats,
    q_cur_score_buckets,
    q_cur_reading_hours,
    q_cur_volume_density,
    q_cur_chapter_efficiency,
    q_cur_citation_leaders,
    q_cur_translation_reach,
    q_cur_edition_number_stats,
    q_cur_author_age_bands,
    q_cur_format_availability,
    q_cur_timestamp_monthly,
]


# ---------------------------------------------------------------------------
# Worker / stats plumbing
# ---------------------------------------------------------------------------


@dataclass
class WorkerStats:
    """Per-worker counters. Kept lock-free; merged at the end."""

    queries: int = 0
    errors: int = 0
    by_query: Dict[str, int] = field(default_factory=dict)
    errors_by_type: Dict[str, int] = field(default_factory=dict)


class GlobalState:
    """Shared coordination state used by all workers and the reporter."""

    def __init__(self, total_queries: Optional[int]) -> None:
        self.total_queries = total_queries
        self.stop = threading.Event()
        self._counter_lock = threading.Lock()
        self.global_queries = 0
        self.global_errors = 0

    def record(self, queries: int, errors: int) -> None:
        with self._counter_lock:
            self.global_queries += queries
            self.global_errors += errors

    def should_stop(self) -> bool:
        if self.stop.is_set():
            return True
        if self.total_queries is not None:
            with self._counter_lock:
                if self.global_queries >= self.total_queries:
                    self.stop.set()
                    return True
        return False


def worker_loop(
    pool: redis.ConnectionPool,
    state: GlobalState,
    queries: List[Callable[[], Callable[[redis.Redis], object]]],
    flush_every: int = 64,
) -> WorkerStats:
    """Run queries in a tight loop until ``state`` says to stop.

    Each worker flushes its local counts into the global counters every
    ``flush_every`` queries so the live reporter stays reasonably fresh
    without paying a lock cost on every single call.
    """

    r = redis.Redis(connection_pool=pool)
    stats = WorkerStats()

    local_q = 0
    local_e = 0

    # Pre-resolve the name list once.
    names = [f.__name__ for f in queries]
    factories = queries

    while not state.should_stop():
        idx = random.randrange(len(factories))
        name = names[idx]
        op = factories[idx]()
        try:
            op(r)
            stats.queries += 1
            stats.by_query[name] = stats.by_query.get(name, 0) + 1
            local_q += 1
        except Exception as e:  # noqa: BLE001 - we want to keep going
            stats.errors += 1
            err_key = type(e).__name__
            stats.errors_by_type[err_key] = (
                stats.errors_by_type.get(err_key, 0) + 1
            )
            local_e += 1

        if local_q + local_e >= flush_every:
            state.record(local_q, local_e)
            local_q = 0
            local_e = 0

    if local_q or local_e:
        state.record(local_q, local_e)

    return stats


def live_reporter(state: GlobalState, start_time: float,
                  interval: float = 1.0) -> None:
    """Background thread that prints a one-line live throughput report."""

    last_q = 0
    last_t = start_time

    while not state.stop.is_set():
        time.sleep(interval)
        now = time.time()
        with state._counter_lock:
            q = state.global_queries
            e = state.global_errors
        elapsed = now - start_time
        dq = q - last_q
        dt = now - last_t
        inst_qps = dq / dt if dt > 0 else 0.0
        avg_qps = q / elapsed if elapsed > 0 else 0.0
        last_q = q
        last_t = now
        sys.stdout.write(
            f"\r[t+{elapsed:6.1f}s] queries={q:>10d} errors={e:>7d} "
            f"| inst={inst_qps:>9.1f} q/s avg={avg_qps:>9.1f} q/s"
        )
        sys.stdout.flush()


def merge_stats(per_worker: List[WorkerStats]) -> WorkerStats:
    total = WorkerStats()
    for s in per_worker:
        total.queries += s.queries
        total.errors += s.errors
        for k, v in s.by_query.items():
            total.by_query[k] = total.by_query.get(k, 0) + v
        for k, v in s.errors_by_type.items():
            total.errors_by_type[k] = total.errors_by_type.get(k, 0) + v
    return total


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="High-throughput Redis search query generator for "
                    f"the '{INDEX_NAME}' index."
    )
    conn = p.add_argument_group("Connection")
    conn.add_argument("--host", default="localhost",
                      help="Redis host (default: localhost)")
    conn.add_argument("--port", type=int, default=6379,
                      help="Redis port (default: 6379)")
    conn.add_argument("--password", default=None,
                      help="Redis password, if any.")
    conn.add_argument("--db", type=int, default=0,
                      help="Redis DB index (default: 0)")
    conn.add_argument("--redis-url", default=None,
                      help="Full redis:// URL. Overrides --host/--port/"
                           "--password/--db if provided.")
    conn.add_argument("--connections", "-c", type=int, default=50,
                      help="Number of concurrent connections / workers "
                           "(default: 50). The script uses one worker "
                           "thread per connection for maximum throughput.")

    load = p.add_argument_group("Load profile (at least one required)")
    load.add_argument("--total-queries", "-n", type=int, default=None,
                      help="Total number of queries to execute across all "
                           "workers. Stops when reached.")
    load.add_argument("--duration", "-d", type=float, default=None,
                      help="Wall-clock test duration in seconds. Stops "
                           "when reached.")
    load.add_argument("--report-interval", type=float, default=1.0,
                      help="Live status refresh interval in seconds "
                           "(default: 1.0). Use 0 to disable.")

    test = p.add_argument_group("Test mode")
    test.add_argument("--test-mode", action="store_true",
                      help="Run each query exactly once, sequentially, "
                           "and print its results. Ignores --connections "
                           "/ --total-queries / --duration.")
    test.add_argument("--test-max-rows", type=int, default=5,
                      help="In --test-mode, max rows/docs to print per "
                           "query (default: 5). Use 0 for unlimited.")

    return p.parse_args(argv)


def build_pool(args: argparse.Namespace) -> redis.ConnectionPool:
    if args.redis_url:
        return redis.ConnectionPool.from_url(
            args.redis_url, max_connections=args.connections
        )
    return redis.ConnectionPool(
        host=args.host,
        port=args.port,
        db=args.db,
        password=args.password,
        max_connections=args.connections,
    )


def print_summary(total: WorkerStats, elapsed: float,
                  connections: int) -> None:
    qps = total.queries / elapsed if elapsed > 0 else 0.0
    print("\n\n=== Run summary ===")
    print(f"Elapsed time       : {elapsed:.2f} s")
    print(f"Connections/workers: {connections}")
    print(f"Total queries      : {total.queries}")
    print(f"Total errors       : {total.errors}")
    print(f"Throughput         : {qps:.1f} q/s")

    if total.by_query:
        print("\nPer-query counts:")
        width = max(len(k) for k in total.by_query)
        for name in sorted(total.by_query):
            count = total.by_query[name]
            share = count / total.queries * 100 if total.queries else 0
            print(f"  {name:<{width}}  {count:>10d}  ({share:5.1f}%)")

    if total.errors_by_type:
        print("\nErrors by type:")
        for name, count in sorted(
            total.errors_by_type.items(), key=lambda kv: -kv[1]
        ):
            print(f"  {name:<30s} {count:>10d}")


def _fmt_value(v, max_len: int = 200) -> str:
    """Pretty-format an individual cell value for test-mode output."""

    if isinstance(v, bytes):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            v = repr(v)
    s = str(v)
    if len(s) > max_len:
        s = s[: max_len - 3] + "..."
    return s


def _print_test_result(name: str, doc_help: Optional[str],
                       result, max_rows: int) -> None:
    """Render the result of a single query for --test-mode."""

    header = f"### {name}"
    if doc_help:
        header += f" -- {doc_help.strip().splitlines()[0]}"
    print("\n" + "=" * 78)
    print(header)
    print("=" * 78)

    if result is None:
        print("(no result)")
        return

    # Some queries (e.g. the cursor-driven aggregate) return a dict that
    # carries both metadata and the actual rows. Unwrap it so the rest of
    # the printer can treat it uniformly.
    if isinstance(result, dict) and "rows" in result and isinstance(
        result.get("rows"), list
    ):
        for k in sorted(k for k in result if k != "rows"):
            print(f"{k:<14s} = {_fmt_value(result[k])}")
        result = result["rows"]

    # ``result`` is either a list of Document objects (FT.SEARCH .docs) or
    # a list of aggregate rows (list/dict/tuple).
    if not isinstance(result, list):
        print(_fmt_value(result, 400))
        return

    print(f"rows returned : {len(result)}")
    if max_rows and len(result) > max_rows:
        print(f"showing first : {max_rows}")
        items = result[:max_rows]
    else:
        items = result

    for i, row in enumerate(items, 1):
        print(f"\n[{i}]")
        if hasattr(row, "__dict__"):
            payload = {
                k: v for k, v in row.__dict__.items()
                if not k.startswith("_")
            }
            for k in sorted(payload):
                print(f"  {k:<24s} = {_fmt_value(payload[k])}")
        elif isinstance(row, dict):
            for k in sorted(row):
                print(f"  {str(k):<24s} = {_fmt_value(row[k])}")
        elif isinstance(row, (list, tuple)):
            # Aggregate rows from redis-py come back as flat [k, v, k, v, ...]
            # lists; show them as key/value pairs when that shape fits.
            if len(row) % 2 == 0 and all(
                isinstance(x, (str, bytes)) for x in row[::2]
            ):
                for k, v in zip(row[::2], row[1::2]):
                    key = k.decode() if isinstance(k, bytes) else str(k)
                    print(f"  {key:<24s} = {_fmt_value(v)}")
            else:
                for j, v in enumerate(row):
                    print(f"  [{j}] {_fmt_value(v)}")
        else:
            print(f"  {_fmt_value(row)}")


def run_test_mode(pool: redis.ConnectionPool, max_rows: int) -> int:
    """Run every query factory exactly once and print the results."""

    r = redis.Redis(connection_pool=pool)
    print(f"Running {len(ALL_QUERIES)} queries once each against "
          f"index '{INDEX_NAME}' (test-mode)")

    failures = 0
    for factory in ALL_QUERIES:
        name = factory.__name__
        doc = factory.__doc__
        op = factory()
        t0 = time.perf_counter()
        try:
            result = op(r)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            _print_test_result(name, doc, result, max_rows)
            print(f"\n(elapsed: {elapsed_ms:.2f} ms)")
        except Exception as e:  # noqa: BLE001
            failures += 1
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            print("\n" + "=" * 78)
            print(f"### {name} -- FAILED after {elapsed_ms:.2f} ms")
            print("=" * 78)
            print(f"{type(e).__name__}: {e}")

    print("\n" + "=" * 78)
    print(f"test-mode done: {len(ALL_QUERIES) - failures} ok, "
          f"{failures} failed")
    print("=" * 78)
    return 0 if failures == 0 else 1


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if not args.test_mode and args.total_queries is None \
            and args.duration is None:
        print(
            "error: you must specify --total-queries and/or --duration "
            "(or use --test-mode)",
            file=sys.stderr,
        )
        return 2
    if args.connections <= 0:
        print("error: --connections must be > 0", file=sys.stderr)
        return 2

    pool = build_pool(args)

    # Sanity-check connectivity up front so we fail fast.
    try:
        probe = redis.Redis(connection_pool=pool)
        probe.ping()
    except redis.exceptions.RedisError as e:
        print(f"error: cannot connect to Redis: {e}", file=sys.stderr)
        return 1

    if args.test_mode:
        return run_test_mode(pool, args.test_max_rows)

    state = GlobalState(total_queries=args.total_queries)

    # Ctrl+C => graceful shutdown.
    def _sigint(_sig, _frm):
        sys.stdout.write("\n[interrupt] stopping workers...\n")
        sys.stdout.flush()
        state.stop.set()

    signal.signal(signal.SIGINT, _sigint)

    target_desc = []
    if args.total_queries is not None:
        target_desc.append(f"{args.total_queries} queries")
    if args.duration is not None:
        target_desc.append(f"{args.duration:g}s")
    print(
        f"Starting {args.connections} workers against "
        f"{args.redis_url or f'{args.host}:{args.port}'} "
        f"(stop on: {', '.join(target_desc)})"
    )

    start = time.time()

    reporter_thread: Optional[threading.Thread] = None
    if args.report_interval > 0:
        reporter_thread = threading.Thread(
            target=live_reporter,
            args=(state, start, args.report_interval),
            daemon=True,
        )
        reporter_thread.start()

    # Enforce the duration cap from the main thread so workers stay tight.
    duration_timer: Optional[threading.Timer] = None
    if args.duration is not None:
        duration_timer = threading.Timer(args.duration, state.stop.set)
        duration_timer.daemon = True
        duration_timer.start()

    per_worker: List[WorkerStats] = []
    try:
        with ThreadPoolExecutor(max_workers=args.connections) as ex:
            futures = [
                ex.submit(worker_loop, pool, state, ALL_QUERIES)
                for _ in range(args.connections)
            ]
            for fut in as_completed(futures):
                try:
                    per_worker.append(fut.result())
                except Exception as e:  # noqa: BLE001
                    print(f"\n[worker crashed] {type(e).__name__}: {e}",
                          file=sys.stderr)
    finally:
        state.stop.set()
        if duration_timer is not None:
            duration_timer.cancel()
        if reporter_thread is not None:
            reporter_thread.join(timeout=2 * args.report_interval + 1)

    elapsed = time.time() - start
    total = merge_stats(per_worker)
    print_summary(total, elapsed, args.connections)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
