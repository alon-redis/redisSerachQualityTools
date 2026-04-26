"""
queryGeneratorDynamic.py
========================

Schema-driven, on-the-fly Redis search query generator for the
``idx:books`` index (defined in ``bookSearch.py``). Every call produces
a freshly composed ``FT.SEARCH`` / ``FT.AGGREGATE`` / ``FT.AGGREGATE
WITHCURSOR`` whose predicates, projection, sort, paging and reducer mix
are sampled by a heuristic algorithm -- so two consecutive calls almost
never emit the same command.

Same CLI / runtime model as ``queryGeneratorNoGeo.py``:
  * one shared :class:`redis.ConnectionPool` sized to ``--connections``
  * one worker thread per connection
  * ``--total-queries`` and / or ``--duration`` cap the run
  * ``--test-mode`` runs a small sample of generated queries once each
    and pretty-prints their results

Differences from the static generators:
  * No fixed query factories. ``compose_random_query(rng)`` returns a
    callable that, given a Redis client, runs a freshly composed query.
    The shape is decided per call by ``rng.choice(...)`` over a
    schema-aware predicate / reducer / pipeline catalogue.
  * Built-in safeguards for the cluster quirks observed in this repo:
      - never emits ``@geo:[...]`` filters or ``geodistance(...)`` (a
        full ``--no-geo`` is the default; flip ``--allow-geo`` to enable)
      - never emits ``format()`` with non-``%s`` specifiers
      - never emits 1-character prefix wildcards (MINPREFIX >= 2)
      - escapes ``-`` in TAG sets (so e.g. ``non-fiction`` is safe)
  * A small "predicate bag" shape is sampled (intersection size, number
    of negations, optional clauses, parameterisation, sorts, etc.) so
    the workload smoothly covers easy and hard plans.
  * Optional ``--seed N`` controls the structural sampler (predicate
    counts, numeric ranges, tag picks, GROUPBY shape, etc.). Same-seed
    reproducibility is best-effort: the small fraction of queries that
    include a Faker fuzzy token (e.g. ``%word%``) uses Faker's
    process-wide RNG, so two runs with the same seed will produce
    structurally identical queries with occasional differences in the
    fuzzy term.

Schema knowledge baked in
-------------------------

The generator knows the ``idx:books`` schema as declared by
``bookSearch.create_search_index``. It groups fields into:

  * ``TAG_FIELDS_SINGLE``     - tag fields with a single token per doc
                                (format, is_available, isbn, id)
  * ``TAG_FIELDS_MULTI``      - tag fields backed by JSON arrays
                                (genres, editions, status, stock_id)
  * ``TEXT_FIELDS``           - free-text fields (title, author,
                                description, publisher, book_series,
                                main_character, location, address)
  * ``NUMERIC_FIELDS``        - all numeric fields (with min/max/typical
                                ranges so generated bands are realistic)

Each field carries a small "domain" of plausible values (for tag
unions/intersections, prefix tokens, etc.) so the random predicates
match a meaningful number of docs without manual calibration.
"""

from __future__ import annotations

import argparse
import random
import re
import signal
import string
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import redis
from faker import Faker

import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.search.aggregation import AggregateRequest
from redis.commands.search.query import NumericFilter, Query


INDEX_NAME = "idx:books"

# We instantiate a process-wide Faker for any TEXT-side fuzzy / phrase
# tokens. It is thread-safe enough for the way we use it here (single
# random word at a time).
_fake = Faker()


# ============================================================================
#  SCHEMA CATALOGUE
# ============================================================================

# Single-token TAG fields and their plausible value domains.
TAG_FIELDS_SINGLE: Dict[str, List[str]] = {
    "format": ["hardcover", "paperback", "ebook"],
    "is_available": ["True", "False"],
    # 'isbn' / 'id' aren't used for random unions: they are
    # near-unique, so any random pick will almost always miss.
}

# Multi-value TAG fields backed by JSON arrays.
TAG_FIELDS_MULTI: Dict[str, List[str]] = {
    "genres": [
        "fiction", "non\\-fiction", "science fiction", "fantasy", "mystery",
        "romance", "history", "horror", "biography", "thriller",
        "self-help".replace("-", "\\-"),
        "poetry", "cookbooks", "memoir", "young adult",
        "children's literature", "drama", "travel", "science", "art",
        "philosophy", "psychology", "religion", "true crime",
        "graphic novel", "adventure", "political", "health", "humor",
    ],
    "editions": [
        "english", "spanish", "french", "german", "italian", "chinese",
        "japanese", "russian", "arabic", "portuguese", "korean", "dutch",
        "swedish", "norwegian", "danish", "finnish", "polish", "turkish",
        "hindi", "urdu", "greek", "hebrew", "thai", "vietnamese",
        "indonesian", "hungarian", "czech", "slovak", "romanian",
        "bulgarian", "ukrainian", "serbian", "croatian", "slovenian",
        "latvian",
    ],
    "status": ["available", "maintenance", "on_loan", "for_sale"],
    # 'stock_id' is near-unique; skip from the union/intersection bag.
}

# Free TEXT fields (used for fuzzy / phrase / prefix predicates).
TEXT_FIELDS: List[str] = [
    "title", "author", "description", "publisher",
    "book_series", "main_character", "location", "address",
]

# Common dictionary-style words that the faker generator actually
# produces in book descriptions; these are good seeds for AND-able
# TEXT predicates that still match a reasonable number of docs.
DESC_COMMON_TOKENS: List[str] = [
    "time", "story", "year", "world", "people", "life", "way", "day",
    "make", "think", "great", "first", "white", "place", "leave",
    "still", "hope", "deep", "right", "stand", "speak", "build",
    "reach", "hour", "case", "team", "course", "money", "cause",
    "season", "society", "common", "rule", "agree", "force",
]

PUB_PREFIXES = ["gr", "in", "ll", "pr", "bo", "co", "fa", "li", "sm", "wa"]
MAIN_CHAR_PREFIXES = [
    "al", "an", "jo", "ma", "ja", "da", "el", "sa", "li", "ka", "th",
    "no", "be", "ro", "ke", "ni", "ch", "br", "ri", "st", "lu",
]
TITLE_PREFIXES = ["ye", "wo", "ti", "mo", "fi", "ne", "li", "ma", "be"]
DESC_PREFIXES  = ["wo", "ti", "st", "li", "pe", "yr", "ma", "th"]


# Numeric fields with (typical_min, typical_max). Random bands are
# sampled inside these envelopes so they routinely return hits.
NUMERIC_FIELDS: Dict[str, Tuple[float, float]] = {
    "price":                       (1, 100),
    "year_published":              (1900, 2023),
    "score":                       (1.0, 5.0),
    "rating_votes":                (1, 1000),
    "pages":                       (50, 1500),
    "chapter_count":               (5, 50),
    "review_count":                (0, 5000),
    "citation_count":              (0, 1000),
    "publishing_delay":            (-356, 1000),
    "word_count":                  (10000, 150000),
    "reading_time_minutes":        (30, 1200),
    "global_sales":                (1000, 1000000),
    "translations_count":          (1, 50),
    "edition_number":              (1, 10),
    "author_age_at_publication":   (20, 80),
    "weight_grams":                (1, 2000),
    "width_cm":                    (10, 30),
    "height_cm":                   (20, 40),
    "depth_cm":                    (1, 10),
    "timestamp":                   (1, 2_000_000_000),
}

# All sortable fields the index declared SORTABLE -- only these can
# appear after SORTBY without forcing a streaming sort.
SORTABLE_NUMERIC: List[str] = list(NUMERIC_FIELDS.keys())

# Reducers that only accept a numeric field.
NUMERIC_REDUCER_FIELDS: List[str] = [
    "score", "price", "pages", "rating_votes", "global_sales",
    "translations_count", "chapter_count", "word_count",
    "reading_time_minutes", "citation_count", "review_count",
    "weight_grams", "year_published",
]

# Reducers that operate on tag/text fields.
DISTINCT_FIELDS: List[str] = [
    "author", "publisher", "genres", "editions", "format",
    "status", "main_character",
]

# Prefix-token field map, used by the prefix-AND clause sampler.
PREFIX_BAG: Dict[str, List[str]] = {
    "title":          TITLE_PREFIXES,
    "description":    DESC_PREFIXES,
    "publisher":      PUB_PREFIXES,
    "main_character": MAIN_CHAR_PREFIXES,
}


# ============================================================================
#  HEURISTIC PREDICATE / REDUCER BUILDERS
# ============================================================================


def _sample_numeric_band(
    rng: random.Random, field: str
) -> Tuple[float, float]:
    """Random sub-window of the field's typical range.

    Picks a width that covers between ~30% and ~95% of the typical
    range so the predicate is selective but not vacuous.
    """
    lo, hi = NUMERIC_FIELDS[field]
    span = hi - lo
    width = rng.uniform(0.3, 0.95) * span
    start = rng.uniform(lo, hi - width)
    end = start + width
    if field == "year_published":
        return int(start), int(end)
    if field in {"pages", "chapter_count", "rating_votes", "review_count",
                 "citation_count", "word_count", "reading_time_minutes",
                 "global_sales", "translations_count", "edition_number",
                 "author_age_at_publication", "weight_grams", "timestamp",
                 "publishing_delay"}:
        return int(start), int(end)
    # float-typed (score, price, dims)
    return round(start, 2), round(end, 2)


def _fmt_numeric_clause(
    field: str, lo: Any, hi: Any, exclusive: bool = False
) -> str:
    lo_s = "-inf" if lo == "-inf" else str(lo)
    hi_s = "+inf" if hi == "+inf" else str(hi)
    if exclusive:
        return f"(@{field}:[({lo_s} {hi_s}])"
    return f"(@{field}:[{lo_s} {hi_s}])"


def numeric_predicate(rng: random.Random, field: str,
                      open_high: bool = False) -> str:
    lo, hi = _sample_numeric_band(rng, field)
    if open_high and rng.random() < 0.4:
        return _fmt_numeric_clause(field, lo, "+inf")
    if rng.random() < 0.15:
        return _fmt_numeric_clause(field, lo, hi, exclusive=True)
    return _fmt_numeric_clause(field, lo, hi)


def tag_intersection_predicate(rng: random.Random, field: str,
                               max_and: int = 1, max_union: int = 5,
                               negate: bool = False) -> str:
    """Sample a TAG predicate.

    With probability proportional to ``max_and`` it produces multiple
    AND-ed ``(@field:{x})`` clauses; otherwise a single clause that may
    union 1..max_union values.
    """
    if field in TAG_FIELDS_SINGLE:
        domain = TAG_FIELDS_SINGLE[field]
    else:
        domain = TAG_FIELDS_MULTI[field]
    n_and = rng.randint(1, max(1, max_and))
    parts = []
    for _ in range(n_and):
        n_union = rng.randint(1, max(1, max_union))
        picks = rng.sample(domain, k=min(n_union, len(domain)))
        # Escape spaces inside tag tokens (e.g. "science fiction").
        picks = [p.replace(" ", "\\ ") for p in picks]
        clause = "{" + "|".join(picks) + "}"
        s = f"(@{field}:{clause})"
        if negate:
            s = "-" + s
        parts.append(s)
    return " ".join(parts)


def text_predicate(rng: random.Random) -> str:
    """One TEXT predicate -- prefix, simple AND token, or fuzzy."""
    kind = rng.choices(
        ["token_and", "prefix", "fuzzy"], weights=[0.55, 0.30, 0.15], k=1
    )[0]
    if kind == "token_and":
        # Pick 1-3 common-token AND clauses across description.
        n = rng.randint(1, 3)
        toks = rng.sample(DESC_COMMON_TOKENS, k=n)
        return " ".join(f"@description:{t}" for t in toks)
    if kind == "prefix":
        f = rng.choice(list(PREFIX_BAG.keys()))
        pre = rng.choice(PREFIX_BAG[f])
        # Always 2+ chars => respects MINPREFIX.
        return f"(@{f}:{pre}*)"
    # fuzzy
    word = _fake.word()[:8]
    if len(word) < 3:
        word = "love"
    return f"(@title|description:%{word}%)"


def text_phrase_predicate(rng: random.Random) -> str:
    pairs = [
        ("time", "year"), ("world", "life"), ("year", "world"),
        ("life", "year"), ("new", "world"), ("big", "city"),
        ("young", "man"), ("right", "place"),
    ]
    a, b = rng.choice(pairs)
    slop = rng.randint(2, 6)
    inorder = "true" if rng.random() < 0.4 else "false"
    return f"@description:\"{a} {b}\"=>{{$slop:{slop}; $inorder:{inorder}}}"


def negation_predicate(rng: random.Random) -> str:
    kind = rng.choice(["genre", "format", "status", "author", "desc"])
    if kind == "genre":
        return tag_intersection_predicate(
            rng, "genres", max_and=1, max_union=2, negate=True)
    if kind == "format":
        return tag_intersection_predicate(
            rng, "format", max_and=1, max_union=1, negate=True)
    if kind == "status":
        return tag_intersection_predicate(
            rng, "status", max_and=1, max_union=2, negate=True)
    if kind == "author":
        return "-@author:\"Alon Shmuely\""
    # desc keyword negation
    bad = rng.choice(["violence", "gore", "torture"])
    return f"-(@description:{bad})"


# ============================================================================
#  QUERY ASSEMBLY
# ============================================================================


def _shuffle_join(rng: random.Random, parts: List[str]) -> str:
    rng.shuffle(parts)
    return " ".join(p for p in parts if p)


def _random_predicate_bag(
    rng: random.Random, *, min_pred: int = 6, max_pred: int = 16
) -> List[str]:
    """Build a heuristic predicate bag for a query body."""
    target = rng.randint(min_pred, max_pred)
    parts: List[str] = []

    # Always anchor on availability or status to avoid 0-hit drift.
    if rng.random() < 0.85:
        parts.append("(@is_available:{True})")

    # Always include a few numeric bands.
    n_num = rng.randint(3, 7)
    fields = rng.sample(list(NUMERIC_FIELDS), k=min(n_num, len(NUMERIC_FIELDS)))
    for f in fields:
        parts.append(numeric_predicate(rng, f, open_high=rng.random() < 0.3))

    # 1-2 tag intersections + 0-1 tag unions
    parts.append(tag_intersection_predicate(rng, "format",
                                            max_and=1, max_union=2))
    parts.append(tag_intersection_predicate(rng, "genres",
                                            max_and=rng.randint(1, 2),
                                            max_union=rng.randint(2, 5)))
    if rng.random() < 0.7:
        parts.append(tag_intersection_predicate(rng, "editions",
                                                max_and=1,
                                                max_union=rng.randint(2, 4)))
    if rng.random() < 0.5:
        parts.append(tag_intersection_predicate(rng, "status",
                                                max_and=1,
                                                max_union=rng.randint(1, 3)))

    # Optional text predicates.
    if rng.random() < 0.45:
        parts.append(text_predicate(rng))
    if rng.random() < 0.20:
        parts.append(text_phrase_predicate(rng))

    # 0-3 negations.
    n_neg = rng.choices([0, 1, 2, 3], weights=[0.4, 0.35, 0.18, 0.07])[0]
    for _ in range(n_neg):
        parts.append(negation_predicate(rng))

    # Trim or pad to roughly the target count without breaking the
    # mandatory @is_available anchor.
    if len(parts) > target + 4:
        parts = parts[: target + 4]
    while len(parts) < target:
        parts.append(numeric_predicate(rng, rng.choice(list(NUMERIC_FIELDS))))

    return parts


def build_random_search(rng: random.Random) -> Tuple[Query, Optional[Dict]]:
    parts = _random_predicate_bag(rng, min_pred=8, max_pred=18)
    body = _shuffle_join(rng, parts)

    q = Query(body).dialect(2)

    # SCORER + WITHSCORES.  We decide this *before* RETURN so we can
    # alias the @score index field if needed: redis-py's Document
    # ctor explodes if it receives 'score=' twice (once as the
    # WITHSCORES doc score, once as the returned index field).
    use_with_scores = False
    if rng.random() < 0.4:
        scorer = rng.choice(["BM25", "TFIDF", "TFIDF.DOCNORM"])
        q.scorer(scorer)
        if rng.random() < 0.6:
            q.with_scores()
            use_with_scores = True

    # RETURN
    n_ret = rng.randint(3, 7)
    ret_pool = ["title", "author", "year_published", "price",
                "genres", "format", "publisher", "main_character",
                "rating_votes", "global_sales", "translations_count"]
    if not use_with_scores:
        ret_pool.append("score")
    ret = rng.sample(ret_pool, k=min(n_ret, len(ret_pool)))
    q.return_fields(*[r for r in ret if r != "score"])
    if "score" in ret:
        q.return_field("score", as_field="book_score")

    # SORT
    if rng.random() < 0.85:
        sort_field = rng.choice(SORTABLE_NUMERIC)
        q.sort_by(sort_field, asc=rng.random() < 0.3)

    # LIMIT
    page = rng.choice([5, 10, 20, 50, 100])
    q.paging(0, page)

    # SUMMARIZE / HIGHLIGHT
    if "@description:" in body and rng.random() < 0.25:
        q.summarize(fields=["description"], context_len=rng.randint(8, 20),
                    num_frags=rng.randint(1, 3), sep=" ... ")
    if rng.random() < 0.10:
        q.highlight(fields=["title", "description"], tags=("<b>", "</b>"))

    # NumericFilter (post-filter)
    params = None
    if rng.random() < 0.30:
        f = rng.choice(["rating_votes", "global_sales", "citation_count"])
        lo, hi = _sample_numeric_band(rng, f)
        q.add_filter(NumericFilter(f, lo, hi))

    # Parameterised band (rarer; demonstrates DIALECT 2 $params)
    if rng.random() < 0.15:
        # Replace one numeric clause with a parameterised one.
        # We just append a parameterised guard rather than rewriting the
        # body, which keeps this safe and keeps the "different every call"
        # property.
        params = {
            "rv_lo": rng.randint(5, 200),
            "s_lo": round(rng.uniform(2.0, 4.0), 2),
        }
        body2 = body + " (@rating_votes:[$rv_lo +inf]) (@score:[$s_lo +inf])"
        q = Query(body2).dialect(2)
        for fn in ret:
            if fn == "score":
                q.return_field("score", as_field="book_score")
            else:
                q.return_field(fn)
        page = rng.choice([5, 10, 20])
        q.paging(0, page)
        if rng.random() < 0.5:
            q.sort_by(rng.choice(SORTABLE_NUMERIC), asc=False)

    return q, params


def _random_reducer(rng: random.Random):
    """Pick a random reducer + alias suitable for a numeric field."""
    f = rng.choice(NUMERIC_REDUCER_FIELDS)
    safe_alias = f"r_{f}_{rng.randint(0, 9999)}"
    kind = rng.choices(
        ["count", "sum", "avg", "min", "max", "stddev", "quantile"],
        weights=[0.25, 0.10, 0.18, 0.07, 0.10, 0.10, 0.20],
    )[0]
    if kind == "count":
        return reducers.count().alias(f"books_{rng.randint(0,9999)}")
    if kind == "sum":
        return reducers.sum(f"@{f}").alias(safe_alias)
    if kind == "avg":
        return reducers.avg(f"@{f}").alias(safe_alias)
    if kind == "min":
        return reducers.min(f"@{f}").alias(safe_alias)
    if kind == "max":
        return reducers.max(f"@{f}").alias(safe_alias)
    if kind == "stddev":
        return reducers.stddev(f"@{f}").alias(safe_alias)
    # quantile
    q_pct = rng.choice([0.25, 0.5, 0.75, 0.9, 0.95, 0.99])
    return reducers.quantile(f"@{f}", q_pct).alias(safe_alias + f"_q{int(q_pct*100)}")


def _random_distinct_reducer(rng: random.Random):
    f = rng.choice(DISTINCT_FIELDS)
    if rng.random() < 0.5:
        return reducers.count_distinct(f"@{f}").alias(
            f"distinct_{f}_{rng.randint(0, 9999)}")
    return reducers.count_distinctish(f"@{f}").alias(
        f"approx_distinct_{f}_{rng.randint(0, 9999)}")


def _random_first_value_reducer(rng: random.Random):
    field = rng.choice(["title", "publisher", "author"])
    by = rng.choice(["score", "global_sales", "rating_votes",
                     "citation_count", "translations_count"])
    direction = aggregations.Desc(f"@{by}") if rng.random() < 0.7 \
        else aggregations.Asc(f"@{by}")
    return reducers.first_value(f"@{field}", direction).alias(
        f"top_{field}_by_{by}_{rng.randint(0, 9999)}")


def _random_apply_steps(rng: random.Random) -> List[Tuple[str, str]]:
    """Return a list of (alias, expression) APPLYs."""
    steps: List[Tuple[str, str]] = []
    catalogue = [
        ("decade", "floor(@year_published/10)*10"),
        ("price_band", "floor(@price/10)*10"),
        ("score_floor", "floor(@score)"),
        ("hours", "floor(@reading_time_minutes/60)"),
        ("vote_tier", "floor(@rating_votes/100)*100"),
        ("density_band", "floor((@weight_grams /"
                         " (@width_cm * @height_cm * @depth_cm))*10)/10"),
        ("wpm_bucket", "floor(log(@word_count / @reading_time_minutes + 1))"),
        ("price_per_page", "@price / @pages"),
        ("sales_per_page", "@global_sales / @pages"),
        ("fmt_up", "upper(@format)"),
        ("status_lc", "lower(@status)"),
        ("ym", "timefmt(@timestamp,\"%Y-%m\")"),
    ]
    n = rng.randint(1, 3)
    return rng.sample(catalogue, k=n)


def build_random_aggregate(
    rng: random.Random, want_cursor: bool = False
) -> AggregateRequest:
    parts = _random_predicate_bag(rng, min_pred=6, max_pred=14)
    body = _shuffle_join(rng, parts)

    req = AggregateRequest(body)

    # LOAD: 1-5 fields drawn from the broader projection set.
    load_pool = [
        "@author", "@title", "@publisher", "@main_character",
        "@year_published", "@score", "@rating_votes", "@pages", "@price",
        "@global_sales", "@word_count", "@chapter_count", "@weight_grams",
        "@translations_count", "@format", "@status",
        "@reading_time_minutes", "@timestamp",
        "@width_cm", "@height_cm", "@depth_cm",
    ]
    n_load = rng.randint(2, 6)
    req.load(*rng.sample(load_pool, k=n_load))

    # APPLYs that produce derived fields (also useful as GROUPBY keys).
    applies = _random_apply_steps(rng)
    for alias, expr in applies:
        req.apply(**{alias: expr})

    # Pick GROUPBY keys: prefer derived aliases, fall back to TAG / TEXT
    # fields that are sortable.
    derived = [a for a, _ in applies]
    base_keys = ["@author", "@publisher", "@format", "@genres",
                 "@editions", "@status", "@main_character"]
    n_groupby = rng.choice([1, 1, 2, 2, 3])
    pool = derived + base_keys
    keys = rng.sample(pool, k=min(n_groupby, len(pool)))
    keys = [(k if k.startswith("@") else f"@{k}") for k in keys]

    # Reducers: 2-6 of them; mix numeric, distinct, first_value.
    reducer_objs = []
    n_red = rng.randint(2, 6)
    for _ in range(n_red):
        kind = rng.choices(
            ["numeric", "distinct", "first_value", "tolist", "random_sample"],
            weights=[0.55, 0.20, 0.15, 0.05, 0.05],
        )[0]
        if kind == "numeric":
            reducer_objs.append(_random_reducer(rng))
        elif kind == "distinct":
            reducer_objs.append(_random_distinct_reducer(rng))
        elif kind == "first_value":
            reducer_objs.append(_random_first_value_reducer(rng))
        elif kind == "tolist":
            f = rng.choice(["title", "publisher", "author"])
            reducer_objs.append(reducers.tolist(f"@{f}").alias(
                f"list_{f}_{rng.randint(0, 9999)}"))
        else:
            f = rng.choice(["title", "main_character", "publisher"])
            n = rng.randint(2, 5)
            reducer_objs.append(reducers.random_sample(f"@{f}", n).alias(
                f"sample_{f}_{rng.randint(0, 9999)}"))
    # COUNT is always cheap and useful for FILTER below.
    reducer_objs.append(reducers.count().alias("books"))
    req.group_by(keys, *reducer_objs)

    # Optional FILTER on the aggregated results.
    if rng.random() < 0.5:
        req.filter("@books >= 1")

    # SORTBY: usually by @books or one of the numeric reducer aliases.
    sort_choices = ["@books"]
    for ro in reducer_objs:
        # ``Reducer.alias()`` returns the reducer with ._alias set; the
        # alias attribute is named differently across redis-py versions
        # so we read whichever exists.
        alias = getattr(ro, "_alias", None) or getattr(ro, "alias_name", None)
        if isinstance(alias, str) and alias.startswith(("r_", "books")):
            sort_choices.append(f"@{alias}")
    sort_field = rng.choice(sort_choices)
    if rng.random() < 0.7:
        req.sort_by(aggregations.Desc(sort_field))
    else:
        req.sort_by(aggregations.Asc(sort_field))

    if not want_cursor:
        req.limit(0, rng.choice([10, 20, 30, 50]))

    if want_cursor:
        req.cursor(count=rng.choice([20, 30, 50, 80]),
                   max_idle=rng.choice([5, 10, 20]))

    req.dialect(2)
    return req


# ============================================================================
#  CURSOR DRAIN HELPER
# ============================================================================


PAGE_COUNT_DEFAULT = 50
MAX_PAGES_DEFAULT = 40


def _drain_cursor(ft, r: redis.Redis, req: AggregateRequest,
                  page_count: int = PAGE_COUNT_DEFAULT,
                  max_pages: int = MAX_PAGES_DEFAULT) -> Dict[str, object]:
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
            nxt = ft.aggregate(cursor)
            total_rows.extend(nxt.rows)
            pages += 1
            cursor = nxt.cursor
            final_cid = cursor.cid if cursor is not None else 0
    finally:
        if (cursor is not None and cursor.cid != 0
                and pages >= max_pages):
            try:
                r.execute_command(
                    "FT.CURSOR", "DEL", INDEX_NAME, cursor.cid)
            except redis.exceptions.ResponseError:
                pass
    return {
        "pages": pages,
        "cursor_id": final_cid,
        "rows_total": len(total_rows),
        "rows": total_rows,
    }


# ============================================================================
#  RANDOM-OP COMPOSER (entry point)
# ============================================================================


@dataclass
class ComposedOp:
    """An on-the-fly query callable, with its label and a debug summary."""

    label: str
    summary: str
    run: Callable[[redis.Redis], object]


def compose_random_query(rng: random.Random) -> ComposedOp:
    """Heuristically assemble a fresh query and return a callable.

    The caller may inspect ``label`` for stats bucketing and ``summary``
    for a one-line human-readable description (used by --test-mode).
    """

    kind = rng.choices(
        ["search", "agg", "cur"], weights=[0.55, 0.25, 0.20]
    )[0]

    if kind == "search":
        q, params = build_random_search(rng)
        body = q.query_string()
        n_pred = body.count("(@") + body.count("-@") + body.count("@description:")
        label = "rand_search"
        summary = f"FT.SEARCH ~{n_pred}-pred body, "\
                  f"params={'yes' if params else 'no'}"

        def _run(r: redis.Redis):
            return r.ft(INDEX_NAME).search(q, query_params=params).docs
        return ComposedOp(label, summary, _run)

    if kind == "agg":
        req = build_random_aggregate(rng, want_cursor=False)
        label = "rand_agg"
        summary = "FT.AGGREGATE (no cursor)"

        def _run(r: redis.Redis):
            return r.ft(INDEX_NAME).aggregate(req).rows
        return ComposedOp(label, summary, _run)

    # cursor
    req = build_random_aggregate(rng, want_cursor=True)
    label = "rand_cur"
    summary = "FT.AGGREGATE WITHCURSOR (drained)"

    def _run(r: redis.Redis):
        return _drain_cursor(r.ft(INDEX_NAME), r, req)
    return ComposedOp(label, summary, _run)


# ============================================================================
#  STATS / WORKERS / CLI  (mirrors queryGenerator.py)
# ============================================================================


@dataclass
class WorkerStats:
    queries: int = 0
    errors: int = 0
    by_label: Dict[str, int] = field(default_factory=dict)
    errors_by_type: Dict[str, int] = field(default_factory=dict)


class GlobalState:
    def __init__(self, total_queries: Optional[int]) -> None:
        self.total_queries = total_queries
        self.stop = threading.Event()
        self._counter_lock = threading.Lock()
        self.global_queries = 0
        self.global_errors = 0

    def record(self, q: int, e: int) -> None:
        with self._counter_lock:
            self.global_queries += q
            self.global_errors += e

    def should_stop(self) -> bool:
        if self.stop.is_set():
            return True
        if self.total_queries is not None:
            with self._counter_lock:
                if self.global_queries >= self.total_queries:
                    self.stop.set()
                    return True
        return False


def worker_loop(pool: redis.ConnectionPool, state: GlobalState,
                seed: Optional[int], flush_every: int = 64) -> WorkerStats:
    r = redis.Redis(connection_pool=pool)
    rng = random.Random(seed if seed is None else seed + threading.get_ident())
    stats = WorkerStats()
    local_q = 0
    local_e = 0
    while not state.should_stop():
        op = compose_random_query(rng)
        try:
            op.run(r)
            stats.queries += 1
            stats.by_label[op.label] = stats.by_label.get(op.label, 0) + 1
            local_q += 1
        except Exception as e:  # noqa: BLE001
            stats.errors += 1
            k = type(e).__name__
            stats.errors_by_type[k] = stats.errors_by_type.get(k, 0) + 1
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
        for k, v in s.by_label.items():
            total.by_label[k] = total.by_label.get(k, 0) + v
        for k, v in s.errors_by_type.items():
            total.errors_by_type[k] = total.errors_by_type.get(k, 0) + v
    return total


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Schema-driven on-the-fly Redis search query "
                    f"generator for the '{INDEX_NAME}' index. Every "
                    "query is composed at runtime from heuristic "
                    "building blocks, so two consecutive runs almost "
                    "never emit the same command."
    )
    conn = p.add_argument_group("Connection")
    conn.add_argument("--host", default="localhost")
    conn.add_argument("--port", type=int, default=6379)
    conn.add_argument("--password", default=None)
    conn.add_argument("--db", type=int, default=0)
    conn.add_argument("--redis-url", default=None,
                      help="Full redis:// URL. Overrides --host/--port/etc.")
    conn.add_argument("--connections", "-c", type=int, default=50)

    load = p.add_argument_group("Load profile (at least one required)")
    load.add_argument("--total-queries", "-n", type=int, default=None)
    load.add_argument("--duration", "-d", type=float, default=None)
    load.add_argument("--report-interval", type=float, default=1.0)
    load.add_argument("--seed", type=int, default=None,
                      help="Optional RNG seed for reproducibility.")

    test = p.add_argument_group("Test mode")
    test.add_argument("--test-mode", action="store_true",
                      help="Generate and run --test-samples queries once "
                           "each, sequentially, and print results.")
    test.add_argument("--test-samples", type=int, default=12,
                      help="In --test-mode, number of fresh random "
                           "queries to compose (default: 12).")
    test.add_argument("--test-max-rows", type=int, default=3,
                      help="In --test-mode, max rows/docs to print per "
                           "query (default: 3). Use 0 for unlimited.")
    return p.parse_args(argv)


def build_pool(args: argparse.Namespace) -> redis.ConnectionPool:
    if args.redis_url:
        return redis.ConnectionPool.from_url(
            args.redis_url, max_connections=args.connections)
    return redis.ConnectionPool(
        host=args.host, port=args.port, db=args.db,
        password=args.password, max_connections=args.connections,
    )


def _fmt_value(v, max_len: int = 200) -> str:
    if isinstance(v, bytes):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            v = repr(v)
    s = str(v)
    if len(s) > max_len:
        s = s[: max_len - 3] + "..."
    return s


def _print_test_result(idx: int, label: str, summary: str,
                       result: Any, max_rows: int) -> None:
    print("\n" + "=" * 78)
    print(f"### #{idx}  {label}  --  {summary}")
    print("=" * 78)
    if isinstance(result, dict) and "rows" in result:
        for k in sorted(k for k in result if k != "rows"):
            print(f"{k:<14s} = {_fmt_value(result[k])}")
        result = result["rows"]
    if not isinstance(result, list):
        print(_fmt_value(result, 400))
        return
    print(f"rows returned : {len(result)}")
    items = result if (max_rows == 0 or len(result) <= max_rows) \
        else result[:max_rows]
    for i, row in enumerate(items, 1):
        print(f"\n[{i}]")
        if hasattr(row, "__dict__"):
            payload = {k: v for k, v in row.__dict__.items()
                       if not k.startswith("_")}
            for k in sorted(payload):
                print(f"  {k:<24s} = {_fmt_value(payload[k])}")
        elif isinstance(row, dict):
            for k in sorted(row):
                print(f"  {str(k):<24s} = {_fmt_value(row[k])}")
        elif isinstance(row, (list, tuple)):
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


def run_test_mode(pool: redis.ConnectionPool, samples: int,
                  seed: Optional[int], max_rows: int) -> int:
    r = redis.Redis(connection_pool=pool)
    rng = random.Random(seed)
    print(f"Composing {samples} fresh random queries against "
          f"index '{INDEX_NAME}' (test-mode)")
    failures = 0
    for i in range(1, samples + 1):
        op = compose_random_query(rng)
        t0 = time.perf_counter()
        try:
            res = op.run(r)
            dt = (time.perf_counter() - t0) * 1000
            _print_test_result(i, op.label, op.summary, res, max_rows)
            print(f"\n(elapsed: {dt:.2f} ms)")
        except Exception as e:  # noqa: BLE001
            failures += 1
            dt = (time.perf_counter() - t0) * 1000
            print("\n" + "=" * 78)
            print(f"### #{i}  {op.label}  --  FAILED in {dt:.2f} ms")
            print("=" * 78)
            print(f"{op.summary}")
            print(f"{type(e).__name__}: {e}")
    print("\n" + "=" * 78)
    print(f"test-mode done: {samples - failures} ok, {failures} failed")
    print("=" * 78)
    return 0 if failures == 0 else 1


def print_summary(total: WorkerStats, elapsed: float, conns: int) -> None:
    qps = total.queries / elapsed if elapsed > 0 else 0
    print("\n\n=== Run summary ===")
    print(f"Elapsed time       : {elapsed:.2f} s")
    print(f"Connections/workers: {conns}")
    print(f"Total queries      : {total.queries}")
    print(f"Total errors       : {total.errors}")
    print(f"Throughput         : {qps:.1f} q/s")
    if total.by_label:
        print("\nPer-kind counts:")
        w = max(len(k) for k in total.by_label)
        for n in sorted(total.by_label):
            c = total.by_label[n]
            share = c / total.queries * 100 if total.queries else 0
            print(f"  {n:<{w}}  {c:>10d}  ({share:5.1f}%)")
    if total.errors_by_type:
        print("\nErrors by type:")
        for n, c in sorted(total.errors_by_type.items(),
                           key=lambda kv: -kv[1]):
            print(f"  {n:<30s} {c:>10d}")


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    if not args.test_mode and args.total_queries is None \
            and args.duration is None:
        print("error: you must specify --total-queries and/or --duration "
              "(or use --test-mode)", file=sys.stderr)
        return 2
    if args.connections <= 0:
        print("error: --connections must be > 0", file=sys.stderr)
        return 2
    pool = build_pool(args)
    try:
        redis.Redis(connection_pool=pool).ping()
    except redis.exceptions.RedisError as e:
        print(f"error: cannot connect to Redis: {e}", file=sys.stderr)
        return 1

    if args.test_mode:
        return run_test_mode(pool, args.test_samples, args.seed,
                             args.test_max_rows)

    state = GlobalState(total_queries=args.total_queries)

    def _sigint(_s, _f):
        sys.stdout.write("\n[interrupt] stopping workers...\n")
        sys.stdout.flush()
        state.stop.set()
    signal.signal(signal.SIGINT, _sigint)

    target_desc = []
    if args.total_queries is not None:
        target_desc.append(f"{args.total_queries} queries")
    if args.duration is not None:
        target_desc.append(f"{args.duration:g}s")
    print(f"Starting {args.connections} workers against "
          f"{args.redis_url or f'{args.host}:{args.port}'} "
          f"(stop on: {', '.join(target_desc)}, seed={args.seed})")

    start = time.time()
    reporter_thread: Optional[threading.Thread] = None
    if args.report_interval > 0:
        reporter_thread = threading.Thread(
            target=live_reporter, args=(state, start, args.report_interval),
            daemon=True)
        reporter_thread.start()

    duration_timer: Optional[threading.Timer] = None
    if args.duration is not None:
        duration_timer = threading.Timer(args.duration, state.stop.set)
        duration_timer.daemon = True
        duration_timer.start()

    per_worker: List[WorkerStats] = []
    try:
        with ThreadPoolExecutor(max_workers=args.connections) as ex:
            futs = [
                ex.submit(worker_loop, pool, state, args.seed)
                for _ in range(args.connections)
            ]
            for f in as_completed(futs):
                try:
                    per_worker.append(f.result())
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
