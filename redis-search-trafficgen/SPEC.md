# Redis Search P0 Traffic Generator — Build Specification

> **Status:** Implementation-ready spec. Use this file as the canonical context when generating or reviewing code for the traffic generator.
> **Client library:** [`github.com/redis/go-redis/v9`](https://github.com/redis/go-redis) (chosen by user).
> **Target Redis:** Redis Open Source / CE **8.4+** (required for `FT.HYBRID`). Backward-compatible scenarios run on 8.0+.
> **Language/toolchain:** Go **1.22+** (uses `math/rand/v2` PCG generators and `slices`/`maps` stdlib).

---

## 1. Goals & Non-Goals

### 1.1 Goals (in priority order)
1. **Realistic workload simulation** of an e-commerce catalog with hybrid semantic + keyword + geo search and aggregated event analytics.
2. **Feature-coverage tracking** for every P0 Redis Search feature listed in `redis-search-p0-context.md`. A run must report which features were exercised and assert ≥ N coverage.
3. **Correctness assertions** (sampled) for BM25 ordering, KNN recall, FT.HYBRID fusion math, prefix/wildcard membership, aggregation totals.
4. **Light load testing**: configurable QPS, ramp-up profiles, latency histograms (p50/p95/p99/p99.9), throughput, error classification. Not a replacement for `memtier_benchmark`.

### 1.2 Non-Goals
- Real embedding models, real datasets, or external feature stores. All data and vectors are synthetic and deterministic.
- Redis Cluster–specific tuning (cross-slot, resharding races). Stand-alone and Sentinel only in v1.
- Redis Enterprise–exclusive features (auto-tiering, Active-Active, search-on-flash).

---

## 2. Project Layout

```
redis-search-trafficgen/
├── go.mod                      # module github.com/<org>/redis-search-trafficgen
├── go.sum
├── cmd/
│   └── trafficgen/
│       └── main.go             # CLI entry point (cobra)
├── internal/
│   ├── config/                 # YAML scenario loading + validation
│   │   ├── config.go
│   │   └── config_test.go
│   ├── client/                 # go-redis wrapper, dial options
│   │   ├── client.go
│   │   └── version.go          # Detect Redis version, gate FT.HYBRID etc.
│   ├── schema/                 # FT.CREATE for the two indexes
│   │   ├── product.go
│   │   └── event.go
│   ├── datagen/                # Deterministic synthetic data
│   │   ├── seed.go             # PCG seed splitting
│   │   ├── product.go          # Product struct + generator
│   │   ├── event.go            # Event struct + generator
│   │   ├── vector.go           # FP32/FP16 vectors, clustered
│   │   ├── geo.go              # Metro clustering + WKT polygons
│   │   └── corpus.go           # Pre-generated query corpus
│   ├── ops/                    # One file per operation family
│   │   ├── op.go               # Op interface + registry
│   │   ├── search_text.go
│   │   ├── search_prefix.go
│   │   ├── search_wildcard.go
│   │   ├── search_fielded.go
│   │   ├── search_boolean.go
│   │   ├── search_knn.go
│   │   ├── search_knn_filter.go
│   │   ├── search_geo.go
│   │   ├── aggregate_facet.go
│   │   ├── aggregate_analytics.go
│   │   ├── hybrid_rrf.go
│   │   ├── hybrid_linear.go
│   │   ├── write_hset.go
│   │   ├── write_jsonset.go
│   │   ├── config_sweep.go
│   │   └── info_poll.go
│   ├── runner/                 # Worker pool + rate limiter + phase scheduler
│   │   ├── runner.go
│   │   ├── worker.go
│   │   ├── ratelimit.go
│   │   └── phase.go
│   ├── coverage/               # P0 feature tracker
│   │   ├── feature.go          # Enum of all P0 features
│   │   └── tracker.go
│   ├── metrics/                # HdrHistogram + Prometheus
│   │   ├── histogram.go
│   │   ├── counters.go
│   │   └── exporter.go
│   ├── assert/                 # Sampled correctness checks
│   │   ├── bm25.go
│   │   ├── recall.go
│   │   ├── rrf.go
│   │   └── result.go
│   └── report/                 # End-of-run report writers
│       ├── text.go
│       ├── json.go
│       └── hgrm.go
├── scenarios/                  # YAML scenario files
│   ├── smoke.yaml
│   ├── ecommerce_p0_full.yaml
│   └── feature_coverage_ci.yaml
├── deploy/
│   ├── docker-compose.yaml     # redis-stack:8.4 + the generator
│   └── prometheus.yaml
├── Makefile
└── README.md
```

---

## 3. Dependencies (pinned in `go.mod`)

```go
require (
    github.com/redis/go-redis/v9            v9.7.0+   // Redis client. Use Protocol: 3 (RESP3).
    github.com/HdrHistogram/hdrhistogram-go v1.1.2
    github.com/prometheus/client_golang     v1.20.0+
    github.com/spf13/cobra                  v1.8.0+
    github.com/spf13/viper                  v1.19.0+   // YAML + env binding
    github.com/brianvoe/gofakeit/v7         v7.0.0+    // Synthetic data
    github.com/x448/float16                 v0.8.4     // FP16 conversion
    golang.org/x/time                       latest     // rate.Limiter
    go.uber.org/zap                         v1.27.0+   // Structured logging
)
```

**Do not pull in `rueidis`.** Single-client implementation against go-redis only.

---

## 4. Configuration (YAML Schema)

### 4.1 Top-level schema

```yaml
name: string                    # required; identifier for the run
seed: uint64                    # required; master deterministic seed
redis:
  addrs: [host:port, ...]       # required; one or more endpoints
  username: string              # optional
  password: string              # optional (or env REDIS_PASSWORD)
  db: int                       # default 0
  protocol: 2 | 3               # default 3 (RESP3)
  pool_size: int                # default 4 * GOMAXPROCS
  min_idle_conns: int           # default 0
  read_timeout: duration        # default 3s
  write_timeout: duration       # default 3s
  dial_timeout: duration        # default 5s
  tls:
    enabled: bool               # default false
    insecure_skip_verify: bool  # default false
    ca_file: string
    cert_file: string
    key_file: string

dataset:
  products: int                 # default 100_000
  events:   int                 # default 500_000
  preload:  bool                # default true; drop + reindex + write before phases run
  drop_indexes: bool            # default true on preload
  flush_db: bool                # default false (DANGEROUS in shared env)

indexes:
  product:
    name: idx:product           # default
    prefix: "product:"          # default
  event:
    name: idx:event
    prefix: "event:"

vectors:
  desc_dim:   384               # HNSW COSINE FP32
  img_dim:    512               # SVS-VAMANA IP FP16
  feat_dim:   8                 # FLAT L2 FP32
  clusters:   50                # # of centroids for clustered generation

phases:                         # ordered list
  - name: string                # required
    duration: duration          # required (e.g. 60s, 10m)
    target_qps: int             # 0 = closed loop (use concurrency instead)
    concurrency: int            # workers active in this phase
    mix_overrides:              # optional weight overrides
      <op_name>: int

mix:                            # global default mix (weights, normalized internally)
  ft_search_text:           20
  ft_search_fielded_bool:   15
  ft_search_prefix:          8
  ft_search_wildcard:        2
  ft_search_geo:             6
  ft_search_knn:            15
  ft_search_knn_prefilter:  10
  ft_aggregate_facet:        8
  ft_aggregate_analytics:    5
  ft_hybrid_rrf:             8
  ft_hybrid_linear:          4
  hset_update:               3
  json_set_update:           2

config_sweeps:                  # optional; ran by `config_sweep` operation
  timeout_ms:           [50, 200, 1000]
  default_dialect:      [2, 3]
  worker_threads:       [0, 4, 8]
  maxprefixexpansions:  [200, 1024, 4096]
  minprefix:            [1, 2]

assertions:                     # sampled checks; rate is the fraction sampled
  bm25_descending:
    enabled: true
    sample_rate: 0.01
    severity: error
  knn_recall_at_10:
    enabled: true
    min_recall: 0.85
    sample_rate: 0.005
    severity: warn
  hybrid_top1_in_either_leg:
    enabled: true
    sample_rate: 0.02
    severity: warn
  prefix_membership:
    enabled: true
    sample_rate: 0.01
    severity: error

coverage:
  min_features_exercised: 55    # of ~60 enumerated; below = exit code 2

metrics:
  prometheus_listen: ":9100"    # empty = disabled
  histogram_significant_digits: 3
  histogram_max_value_ms: 60000
  out_dir: "./out"              # final JSON + .hgrm files

logging:
  level: info | debug | warn | error
  format: console | json
```

### 4.2 Validation rules (enforced in `internal/config`)
- `seed` must be non-zero.
- Sum of weights in `mix` > 0.
- Every operation in `mix_overrides` must exist in the op registry.
- `phases` non-empty.
- `dataset.products >= vectors.clusters * 2`.
- If any `ft_hybrid_*` weight > 0 → require Redis 8.4+ at startup (probe `INFO server`).

---

## 5. Index Schemas

### 5.1 `idx:product` — JSON

Construct with go-redis `FTCreate(ctx, "idx:product", opts, fields...)`. Fields use `FieldSchema` with the per-type attribute structs.

```text
FT.CREATE idx:product
  ON JSON PREFIX 1 product:
  SCHEMA
    $.sku             AS sku           TAG  CASESENSITIVE SORTABLE
    $.brand           AS brand         TAG  SORTABLE
    $.categories[*]   AS categories    TAG  SEPARATOR "|"  WITHSUFFIXTRIE
    $.title           AS title         TEXT WEIGHT 5.0 SORTABLE NOSTEM
    $.description     AS description   TEXT WEIGHT 1.0 PHONETIC dm:en WITHSUFFIXTRIE
    $.internal_notes  AS notes         TEXT NOINDEX
    $.price           AS price         NUMERIC SORTABLE
    $.rating          AS rating        NUMERIC SORTABLE
    $.in_stock        AS in_stock      TAG
    $.created_ts      AS created_ts    NUMERIC SORTABLE
    $.store_location  AS store_loc     GEO SORTABLE
    $.pickup_zone     AS pickup_zone   GEOSHAPE SPHERICAL
    $.desc_embedding  AS desc_vec      VECTOR HNSW       10 TYPE FLOAT32 DIM 384 DISTANCE_METRIC COSINE  M 16 EF_CONSTRUCTION 200
    $.img_embedding   AS img_vec       VECTOR SVS-VAMANA 12 TYPE FLOAT16 DIM 512 DISTANCE_METRIC IP      COMPRESSION LVQ8 GRAPH_MAX_DEGREE 64 CONSTRUCTION_WINDOW_SIZE 200
    $.feat_embedding  AS feat_vec      VECTOR FLAT        6 TYPE FLOAT32 DIM 8   DISTANCE_METRIC L2
```

> **Fallback:** if `FT.CREATE` rejects `SVS-VAMANA` (older Redis, non-Intel build), log a warning, retry with `VECTOR HNSW … TYPE FLOAT16` for `img_vec`, and disable the `vec_svs_vamana` coverage flag. Detection: call `FT.CREATE` once at startup against a probe index; on `Unknown index type SVS-VAMANA` error, set capability flag.

### 5.2 `idx:event` — Hash

```text
FT.CREATE idx:event
  ON HASH PREFIX 1 event:
  SCHEMA
    user_id      TAG
    session_id   TAG
    product_sku  TAG CASESENSITIVE
    event_type   TAG                  -- view | add_to_cart | purchase | search
    query_text   TEXT NOSTEM
    ts           NUMERIC SORTABLE
    dwell_ms     NUMERIC SORTABLE
    country      TAG SORTABLE
    device       TAG
```

### 5.3 go-redis construction (reference)

```go
func CreateProductIndex(ctx context.Context, rdb *redis.Client, name string) error {
    opts := &redis.FTCreateOptions{
        OnJSON: true,
        Prefix: []any{"product:"},
    }
    schema := []*redis.FieldSchema{
        {FieldName: "$.sku",            As: "sku",         FieldType: redis.SearchFieldTypeTag,
         TagOptions: &redis.TagFieldOptions{CaseSensitive: true, Sortable: true}},
        {FieldName: "$.brand",          As: "brand",       FieldType: redis.SearchFieldTypeTag,
         TagOptions: &redis.TagFieldOptions{Sortable: true}},
        {FieldName: "$.categories[*]",  As: "categories",  FieldType: redis.SearchFieldTypeTag,
         TagOptions: &redis.TagFieldOptions{Separator: "|", WithSuffixtrie: true}},
        {FieldName: "$.title",          As: "title",       FieldType: redis.SearchFieldTypeText,
         TextOptions: &redis.TextFieldOptions{Weight: 5.0, Sortable: true, NoStem: true}},
        {FieldName: "$.description",    As: "description", FieldType: redis.SearchFieldTypeText,
         TextOptions: &redis.TextFieldOptions{Weight: 1.0, Phonetic: "dm:en", WithSuffixtrie: true}},
        {FieldName: "$.internal_notes", As: "notes",       FieldType: redis.SearchFieldTypeText,
         TextOptions: &redis.TextFieldOptions{NoIndex: true}},
        {FieldName: "$.price",          As: "price",       FieldType: redis.SearchFieldTypeNumeric,
         NumericOptions: &redis.NumericFieldOptions{Sortable: true}},
        // … rating, in_stock, created_ts, store_loc, pickup_zone …
        {FieldName: "$.desc_embedding", As: "desc_vec",    FieldType: redis.SearchFieldTypeVector,
         VectorArgs: &redis.FTVectorArgs{HNSWOptions: &redis.FTHNSWOptions{
             Type: "FLOAT32", Dim: 384, DistanceMetric: "COSINE", M: 16, EFConstruction: 200,
         }}},
        // img_vec via FTVectorArgs (SVS-VAMANA branch; see fallback above)
        // feat_vec via FTVectorArgs.FlatOptions
    }
    return rdb.FTCreate(ctx, name, opts, schema...).Err()
}
```

> If the installed go-redis version's `FTVectorArgs` doesn't expose `SVSVamana` directly, fall back to `rdb.Do(ctx, "FT.CREATE", ...)` with raw args for that field only; keep other fields typed.

---

## 6. Deterministic Synthetic Data

### 6.1 Seed splitting

Master seed from YAML. Per-stream seeds derived deterministically:

```go
const (
    StreamProducts uint64 = 1
    StreamEvents          = 2
    StreamQueries         = 3
    StreamVectorsDesc     = 4
    StreamVectorsImg      = 5
    StreamVectorsFeat     = 6
    StreamGeo             = 7
    StreamCentroids       = 8
)

func RNG(master, stream uint64) *rand.Rand {
    return rand.New(rand.NewPCG(master, stream)) // math/rand/v2
}
```

Workers receive a *child* RNG seeded by `(master, stream<<32 | workerID)` to keep cross-worker draws independent and reproducible.

### 6.2 Product (`product:<id>`)

```go
type Product struct {
    SKU            string    `json:"sku"`            // e.g. "A1B2-X9F3" (case-sensitive TAG)
    Brand          string    `json:"brand"`          // one of 50 fixed brands
    Categories     []string  `json:"categories"`     // 1–3 from a 50-leaf taxonomy
    Title          string    `json:"title"`          // gofakeit ProductName
    Description    string    `json:"description"`    // 1–3 sentences, includes brand
    InternalNotes  string    `json:"internal_notes"` // NOINDEX field, must still round-trip
    Price          float64   `json:"price"`          // lognormal μ=5 σ=0.6 → ~$50–$2000
    Rating         float64   `json:"rating"`         // beta-distributed in [1,5]
    InStock        string    `json:"in_stock"`       // "true" | "false"
    CreatedTS      int64     `json:"created_ts"`     // unix seconds, last 2 years
    StoreLocation  string    `json:"store_location"` // "lon,lat" (GEO format)
    PickupZone     string    `json:"pickup_zone"`    // WKT POLYGON near store
    DescEmbedding  []float32 `json:"desc_embedding"` // 384-D, L2-normalized
    ImgEmbedding   []float32 `json:"img_embedding"`  // 512-D, L2-normalized; stored as FP16-quantized base64? see §7.4
    FeatEmbedding  []float32 `json:"feat_embedding"` // 8-D, NOT normalized
}
```

- **Stable key**: `product:<lowerhex(sha1(seed||"product"||idx)[:6])>`.
- **Brand**: pick from `brands[idx % 50]` for predictable BM25/PHONETIC tests; remaining draws use RNG.
- **Categories**: 1–3 picks from a fixed 50-leaf taxonomy with a Zipf prior (top categories more common).
- **Title / description**: include the brand verbatim; description includes 1 misspelling token from a fixed set (drives PHONETIC).
- **`internal_notes`**: include a unique marker like `NOINDEX-<idx>` so we can verify it's returned via `RETURN` but unreachable via `@notes:` queries.

### 6.3 Event (`event:<id>`)

```go
type Event struct {
    UserID     string `redis:"user_id"`
    SessionID  string `redis:"session_id"`
    ProductSKU string `redis:"product_sku"`  // CASESENSITIVE TAG
    EventType  string `redis:"event_type"`   // weighted: view 0.7, add 0.15, purchase 0.05, search 0.10
    QueryText  string `redis:"query_text"`   // empty unless EventType=="search"
    TS         int64  `redis:"ts"`
    DwellMS    int64  `redis:"dwell_ms"`     // lognormal
    Country    string `redis:"country"`      // 1 of 20 ISO codes, weighted
    Device     string `redis:"device"`       // mobile|desktop|tablet
}
```

### 6.4 Geo

20 fixed metro centers (lon, lat). Each product picks a metro by Zipf, then jitters by Gaussian σ=0.03° (~3 km). `PickupZone` is a square WKT polygon ±0.05° around the store. This guarantees:
- Radius queries (`@store_loc:[lon lat r mi]`) hit non-empty results.
- `GEOSHAPE` `WITHIN`/`CONTAINS` queries (DIALECT 3) have hits.

---

## 7. Synthetic Vectors

### 7.1 Centroids (clustered embedding space)

At preload, generate `clusters` (default 50) random unit vectors per vector field. Persist them in `internal/datagen` so workers can re-derive ground truth.

```go
func GenCentroids(rng *rand.Rand, dim, n int) [][]float32 {
    cs := make([][]float32, n)
    for i := range cs {
        v := make([]float32, dim)
        for j := range v { v[j] = float32(rng.NormFloat64()) }
        l2normalize(v)
        cs[i] = v
    }
    return cs
}
```

### 7.2 Per-product vectors

```go
func MakeVec(rng *rand.Rand, centroid []float32, sigma float32, normalize bool) []float32 {
    v := make([]float32, len(centroid))
    for i := range v {
        v[i] = centroid[i] + sigma*float32(rng.NormFloat64())
    }
    if normalize { l2normalize(v) }
    return v
}
```

- `desc_vec` (384-D, COSINE): cluster = `hash(category0) % clusters`, σ=0.15, **normalize**.
- `img_vec` (512-D, IP): cluster = `hash(brand) % clusters`, σ=0.10, **normalize** (so IP ≈ COSINE).
- `feat_vec` (8-D, L2): cluster = `hash(category0) % clusters`, σ=0.30, **do not normalize** (L2 needs magnitude variation).

### 7.3 Query vectors (for KNN / FT.HYBRID)

A query vector is either:
- **Centroid-targeted**: pick a centroid + jitter (mimics a real semantic query close to a cluster). Used 90% of the time.
- **Out-of-distribution**: uniform random unit vector. Used 10% to stress recall.

This gives recall assertions a meaningful baseline.

### 7.4 Encoding for the wire

go-redis (and Redis) expect vector bytes as **little-endian packed floats** in the `PARAMS` payload (and in the JSON document for indexing).

```go
func F32ToBytesLE(v []float32) []byte {
    b := make([]byte, 4*len(v))
    for i, x := range v {
        binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(x))
    }
    return b
}

func F16ToBytesLE(v []float32) []byte {
    b := make([]byte, 2*len(v))
    for i, x := range v {
        binary.LittleEndian.PutUint16(b[2*i:], float16.Fromfloat32(x).Bits())
    }
    return b
}
```

For **JSON-stored vectors** (the index reads from `$.desc_embedding`), Redis 8 supports storing them as JSON arrays of numbers; emit them as `[]float32` in the JSON. For `img_vec` (FP16), JSON storage still uses a regular number array — the engine converts; we keep our in-memory copies FP32 and let the engine quantize at index time.

For **query-time `$qv` PARAMS**, always send raw FP32 little-endian bytes for `desc_vec`/`feat_vec` and raw FP16 little-endian bytes for `img_vec`. With go-redis:

```go
res, err := rdb.FTSearchWithArgs(ctx, "idx:product",
    "(@categories:{road})=>[KNN 10 @desc_vec $qv AS score]",
    &redis.FTSearchOptions{
        DialectVersion: 2,
        Params: map[string]any{"qv": F32ToBytesLE(queryVec)},
        Return: []redis.FTSearchReturn{{FieldName: "sku"}, {FieldName: "score"}},
        SortBy: []redis.FTSearchSortBy{{FieldName: "score", Asc: true}},
        LimitOffset: 0, Limit: 10,
    }).Result()
```

---

## 8. Query Corpus

Pre-generated **before** phases start so latency numbers are not polluted by query construction.

| Corpus | Size (default) | Used by |
|---|---|---|
| `terms_common` | 2,000 | text/fielded/boolean queries (Zipf) |
| `terms_rare`   | 200    | prefix + MAXEXPANSIONS stress |
| `brands`       | 50     | exact-match TAG, BM25 ordering |
| `misspellings` | 200    | PHONETIC matching |
| `categories`   | 50     | TAG faceting, KNN pre-filter |
| `query_vecs_desc` | 10,000 | KNN / FT.HYBRID over `desc_vec` |
| `query_vecs_img`  | 1,000  | KNN over `img_vec` |
| `geo_points`   | 200    | radius / polygon queries |

Workers pick from these by hashing `(workerID, opCounter)` for deterministic replay. The corpus itself is regenerated only when `seed` or `dataset.*` change.

---

## 9. Operations Catalog

Every op implements:

```go
type Op interface {
    Name() string
    Features() []coverage.Feature        // P0 features this op exercises
    Execute(ctx context.Context, w *Worker) (ExecResult, error)
}

type ExecResult struct {
    Latency      time.Duration
    BytesIn      int
    BytesOut     int
    ResultCount  int
    Warnings     []string
    AssertResult assert.Result            // empty if no assertion sampled
}
```

### 9.1 Op catalog (one row per op, all required)

| Op name | Cmd | Notes | P0 features exercised |
|---|---|---|---|
| `ft_search_text` | `FT.SEARCH` | random text from `terms_common`, no field | `text`, `bm25`, `limit`, `return` |
| `ft_search_fielded_bool` | `FT.SEARCH` | `@brand:{X} (@categories:{A}|@categories:{B}) -@in_stock:{false} ~comfortable` | `text`, `tag`, `fielded`, `bool_and_or_not_optional`, `bm25`, `infields` (1/3 of queries use `INFIELDS`) |
| `ft_search_prefix` | `FT.SEARCH` | `@title:hel*`, sweep prefix length 1-4 | `prefix`, `minprefix`, `maxexpansions`, `with_suffixtrie` (also `@categories:{*foo*}` contains) |
| `ft_search_wildcard` | `FT.SEARCH` | `*` | `wildcard`, `limit` (capped to 2% of mix) |
| `ft_search_geo` | `FT.SEARCH` | `@store_loc:[lon lat 30 mi]` and (with DIALECT 3) `@pickup_zone:[WITHIN $poly]` | `geo`, `geoshape`, `dialect_3` |
| `ft_search_knn` | `FT.SEARCH` | `*=>[KNN 10 @desc_vec $qv AS score]` SORTBY score | `knn`, `vec_hnsw`, `cosine`, `dialect_2`, `sortby`, `return` |
| `ft_search_knn_prefilter` | `FT.SEARCH` | `(@categories:{road} @price:[100 800])=>[KNN 10 @desc_vec $qv]` | `knn_prefilter`, `vec_hnsw`, `tag`, `numeric_range` |
| `ft_search_knn_flat_l2` | `FT.SEARCH` | KNN on `feat_vec`; used as recall oracle | `knn`, `vec_flat`, `l2` |
| `ft_search_knn_svs_ip` | `FT.SEARCH` | KNN on `img_vec` (FP16 SVS-VAMANA, IP) | `knn`, `vec_svs_vamana`, `ip`, `fp16` |
| `ft_aggregate_facet` | `FT.AGGREGATE` | `GROUPBY @categories REDUCE COUNT 0 AS n SORTBY 2 @n DESC LIMIT 0 20` | `aggregate`, `groupby`, `reduce_count`, `sortby`, `limit` |
| `ft_aggregate_analytics` | `FT.AGGREGATE` | per-brand: `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `QUANTILE 0.95`, `TOLIST`, `FIRST_VALUE`, `RANDOM_SAMPLE`, `APPLY year(@created_ts) AS y`, `FILTER` | `aggregate`, `reduce_sum`, `reduce_avg`, `reduce_min`, `reduce_max`, `reduce_stddev`, `reduce_quantile`, `reduce_tolist`, `reduce_first_value`, `reduce_random_sample`, `apply`, `filter`, `load` |
| `ft_hybrid_rrf` | `FT.HYBRID` | SEARCH leg + VSIM leg, `COMBINE RRF 2 CONSTANT 60 WINDOW 20` | `hybrid_rrf`, `bm25`, `vec_hnsw`, `cosine` |
| `ft_hybrid_linear` | `FT.HYBRID` | `COMBINE LINEAR 4 ALPHA 0.7 BETA 0.3 WINDOW 20` | `hybrid_linear`, `bm25`, `vec_hnsw` |
| `hset_update` | `HSET` | update random event field; triggers background reindex of `idx:event` | `hash_doc`, `background_indexing` |
| `json_set_update` | `JSON.SET` | mutate `$.price` or `$.in_stock` of a random product | `json_doc`, `background_indexing` |
| `config_sweep` | `FT.CONFIG SET` | rotate through `config_sweeps` values | `config_set_timeout`, `config_set_default_dialect`, `config_set_worker_threads`, `config_set_maxprefixexpansions`, `config_set_minprefix` |
| `info_poll` | `FT.INFO` / `INFO search` | every 5s on a single dedicated worker | `ft_info`, `info_search`, `gc_stats` |
| `timeout_probe` | `FT.SEARCH` | wildcard with explicit `TIMEOUT 1` (ms) to force warning | `timeout` |

### 9.2 Example op — `ft_hybrid_rrf` (canonical go-redis pattern)

go-redis as of v9.7 has no typed `FTHybrid` helper. Use `rdb.Do`:

```go
func (o *HybridRRFOp) Execute(ctx context.Context, w *Worker) (ExecResult, error) {
    qv := w.Corpus.PickQueryVecDesc(w.RNG)
    term := w.Corpus.PickCommonTerm(w.RNG)
    cat  := w.Corpus.PickCategory(w.RNG)

    args := []any{
        "FT.HYBRID", w.Cfg.Indexes.Product.Name,
        "SEARCH", fmt.Sprintf("@description:%s @categories:{%s}", term, cat),
        "SCORER", "BM25", "YIELD_SCORE_AS", "text_score",
        "VSIM", "@desc_vec", "$qv",
        "KNN", "20", "K", "20",
        "YIELD_SCORE_AS", "vec_score",
        "COMBINE", "RRF", "2", "CONSTANT", "60", "WINDOW", "20",
        "YIELD_SCORE_AS", "fused",
        "LIMIT", "0", "10",
        "PARAMS", "2", "qv", F32ToBytesLE(qv),
        "DIALECT", "2",
    }
    start := time.Now()
    res, err := w.Rdb.Do(ctx, args...).Result()
    lat := time.Since(start)
    if err != nil { return ExecResult{Latency: lat}, err }

    // Parse generic RESP3 map/array result; count rows.
    n := parseHybridRowCount(res)
    ar := assert.MaybeRRF(w.RNG, w.Cfg.Assertions.HybridTop1InEitherLeg, res)
    return ExecResult{Latency: lat, ResultCount: n, AssertResult: ar}, nil
}
```

### 9.3 RESP3 result parsing

With `Protocol: 3`, `FT.SEARCH` returns a typed map (`{total_results, results, ...}`). Use go-redis's typed `FTSearchWithArgs(...).Result()` for SEARCH/AGGREGATE. For `FT.HYBRID` (no typed wrapper yet), `rdb.Do(...)` returns `any` whose concrete type is `map[string]any` on RESP3 and a flat `[]any` on RESP2 — implement both branches in `parseHybridRowCount`.

---

## 10. Worker Pool & Scheduling

### 10.1 Worker

```go
type Worker struct {
    ID        int
    Rdb       redis.UniversalClient
    RNG       *rand.Rand
    Cfg       *config.Config
    Corpus    *datagen.Corpus
    Coverage  *coverage.Tracker
    Hist      *metrics.HistogramSet
    Counters  *metrics.Counters
    Logger    *zap.Logger
}
```

### 10.2 Phase scheduler

- Phases run sequentially. For each phase:
  - Spin `concurrency` goroutines.
  - If `target_qps > 0`: shared `golang.org/x/time/rate.Limiter` (token bucket, burst = ceil(qps/10)).
  - If `target_qps == 0`: closed loop — each worker runs ops back-to-back.
- Each worker loop:
  1. `limiter.Wait(ctx)` (if rate-limited).
  2. Pick op by weighted draw from the merged mix (`mix` + `mix_overrides` for this phase).
  3. `ctx, cancel := context.WithTimeout(ctx, perOpTimeout)`.
  4. `Execute`. Record latency, bytes, errors.
  5. For each declared feature → `Coverage.Mark(feature)`.
  6. If `AssertResult` non-empty → log/count by severity; if `error` and configured to fail → cancel root context.

### 10.3 Coordinated omission

Open-loop mode uses `hdr.RecordValueWithExpectedInterval(latNs, expectedIntervalNs)` where `expectedIntervalNs = 1e9 / target_qps`. Closed-loop mode uses `hdr.RecordValue`. Mode is auto-selected per phase from `target_qps`.

### 10.4 Error classification

```go
const (
    ErrClassClientTimeout    = "client_timeout"    // ctx.DeadlineExceeded
    ErrClassServerTimeout    = "server_timeout"    // Redis "Timeout" warning in FT.SEARCH result
    ErrClassDial             = "dial"
    ErrClassConn             = "conn"              // EOF, broken pipe
    ErrClassQuerySyntax      = "query_syntax"      // bad query (treated as BUG, fail run)
    ErrClassFeatureUnsupp    = "feature_unsupported" // e.g. SVS-VAMANA missing
    ErrClassOther            = "other"
)
```

`query_syntax` errors fail the run (exit code 3) — they indicate generator bugs, not Redis issues.

---

## 11. Coverage Tracker

```go
type Feature string

const (
    FeatHashDoc                Feature = "hash_doc"
    FeatJSONDoc                Feature = "json_doc"
    FeatTagSeparator           Feature = "tag_separator"
    FeatTagCaseSensitive       Feature = "tag_casesensitive"
    FeatTagSortable            Feature = "tag_sortable"
    FeatTagSuffixtrie          Feature = "tag_with_suffixtrie"
    FeatTextWeight             Feature = "text_weight"
    FeatTextSortable           Feature = "text_sortable"
    FeatTextNoIndex            Feature = "text_noindex"
    FeatTextSuffixtrie         Feature = "text_with_suffixtrie"
    FeatTextPhonetic           Feature = "text_phonetic"
    FeatTextNoStem             Feature = "text_nostem"
    FeatVecFlat                Feature = "vec_flat"
    FeatVecHNSW                Feature = "vec_hnsw"
    FeatVecSVSVamana           Feature = "vec_svs_vamana"
    FeatVecFP32                Feature = "vec_fp32"
    FeatVecFP16                Feature = "vec_fp16"
    FeatDistL2                 Feature = "dist_l2"
    FeatDistIP                 Feature = "dist_ip"
    FeatDistCosine             Feature = "dist_cosine"
    FeatHNSWParamsRuntime      Feature = "hnsw_ef_runtime"
    FeatSearchExact            Feature = "search_exact"
    FeatSearchPrefix           Feature = "search_prefix"
    FeatSearchWildcard         Feature = "search_wildcard"
    FeatSearchFielded          Feature = "search_fielded"
    FeatSearchInFields         Feature = "search_infields"
    FeatBoolAnd                Feature = "bool_and"
    FeatBoolOr                 Feature = "bool_or"
    FeatBoolNot                Feature = "bool_not"
    FeatBoolOptional           Feature = "bool_optional"
    FeatBM25                   Feature = "bm25"
    FeatHybridRRF              Feature = "hybrid_rrf"
    FeatHybridLinear           Feature = "hybrid_linear"
    FeatKNN                    Feature = "knn"
    FeatKNNPrefilter           Feature = "knn_prefilter"
    FeatSortBy                 Feature = "sortby"
    FeatLimit                  Feature = "limit"
    FeatReturn                 Feature = "return"
    FeatLoad                   Feature = "load"
    FeatApply                  Feature = "apply"
    FeatFilter                 Feature = "filter"
    FeatGroupBy                Feature = "groupby"
    FeatReduceCount            Feature = "reduce_count"
    FeatReduceSum              Feature = "reduce_sum"
    FeatReduceAvg              Feature = "reduce_avg"
    FeatReduceMin              Feature = "reduce_min"
    FeatReduceMax              Feature = "reduce_max"
    FeatReduceStddev           Feature = "reduce_stddev"
    FeatReduceQuantile         Feature = "reduce_quantile"
    FeatReduceToList           Feature = "reduce_tolist"
    FeatReduceFirstValue       Feature = "reduce_first_value"
    FeatReduceRandomSample     Feature = "reduce_random_sample"
    FeatWorkerThreads          Feature = "worker_threads"
    FeatTimeout                Feature = "timeout"
    FeatBackgroundIndexing     Feature = "background_indexing"
    FeatGC                     Feature = "gc"
    FeatFTInfo                 Feature = "ft_info"
    FeatInfoSearch             Feature = "info_search"
    FeatConfigSetTimeout       Feature = "config_set_timeout"
    FeatConfigSetMinPrefix     Feature = "config_set_minprefix"
    FeatConfigSetMaxExpansions Feature = "config_set_maxexpansions"
    FeatConfigSetDefDialect    Feature = "config_set_default_dialect"
    FeatConfigSetWorkerThreads Feature = "config_set_worker_threads"
    FeatDialect2               Feature = "dialect_2"
    FeatDialect3               Feature = "dialect_3"
)
```

`Tracker` is a sync-safe map of `Feature → counter`. End-of-run report emits a table + a JSON object with zero-count features highlighted. CI scenarios set `coverage.min_features_exercised`; below threshold → exit code 2.

---

## 12. Metrics

### 12.1 HdrHistogram set

One histogram per op name, plus per-error-class. Config:
- `lowestDiscernibleValue = 1` (µs)
- `highestTrackableValue = max_value_ms * 1000` (µs)
- `significantFigures = 3`

End-of-run: emit `<out_dir>/<run>.<op>.hgrm` files (HistogramLogProcessor format) and a `<run>.summary.json`.

### 12.2 Prometheus

- `trafficgen_ops_total{op, status}` counter
- `trafficgen_op_latency_seconds{op}` histogram (native histogram if go-redis Protocol=3 — but use a Prometheus native histogram here independent of Redis protocol)
- `trafficgen_features_exercised{feature}` counter
- `trafficgen_redis_info{stat}` gauge (poll `INFO search` and select fields: `total_indexing_time`, `gc_total_docs`, `active_io_threads`, etc.)

### 12.3 Reporter

End-of-run report (text + JSON):
- Per-op: count, error count, p50/p95/p99/p99.9/max.
- Coverage table (feature → count, missing features list).
- Top 10 slowest queries (sampled).
- Server INFO deltas across the run.
- Exit code:
  - 0 — success, all assertions pass, coverage met.
  - 1 — generator error.
  - 2 — coverage shortfall.
  - 3 — query-syntax bug (internal).
  - 4 — assertion `error` severity violated.

---

## 13. Correctness Assertions (sampled)

| Assertion | Method | Sample rate |
|---|---|---|
| `bm25_descending` | When `WithScores=true`, verify scores are non-increasing across returned docs. | 1% |
| `knn_recall_at_10` | Periodically run the same query against `feat_vec` (FLAT, ground truth) and `desc_vec` (HNSW). Compute recall@10 against the FLAT result set's projection onto the same docs (only valid when query vectors are drawn from a common cluster — use a special "recall-probe" query that targets both fields). Warn if < `min_recall`. | 0.5% |
| `hybrid_top1_in_either_leg` | After running `FT.HYBRID`, re-run each leg separately and verify the top hybrid doc appears in at least one leg's top-`WINDOW`. | 2% |
| `prefix_membership` | After `@title:hel*`, parse returned titles and verify each starts with "hel" (case-insensitive). | 1% |
| `noindex_unsearchable` | Once per run: `@notes:"NOINDEX-1"` must return zero docs; `RETURN $.internal_notes` must return the field. | 1× |
| `nostem_distinguishes` | Search `trek` vs `trekking` against title (NOSTEM) and description (stemmed); verify different result counts. | 1× |
| `phonetic_matches` | Submit known misspelling, verify ≥1 result whose description contains the correctly-spelled token. | 1× |

Assertion results feed both per-op metrics and the exit-code calculation.

---

## 14. CLI

```
trafficgen [--config PATH] <command>

Commands:
  preload      Create indexes + write dataset (idempotent if --resume)
  run          Execute phases against an already-loaded dataset
  full         preload + run in one shot
  validate     Parse and validate config; no Redis traffic
  drop         FT.DROPINDEX + DEL the prefix (DANGEROUS, requires --yes)
  capabilities Probe the connected Redis: version, modules, supported vector types
  version      Print trafficgen version + go-redis version

Global flags:
  --config PATH        YAML scenario (required for preload/run/full)
  --redis-addr HOST:PORT  Override redis.addrs[0]
  --seed UINT64        Override config seed
  --out-dir PATH       Override metrics.out_dir
  --log-level LEVEL
```

`full` is the most-used path: `trafficgen full --config scenarios/ecommerce_p0_full.yaml`.

---

## 15. Preload Procedure

1. Connect, run `capabilities` probe:
   - `INFO server` → version.
   - `MODULE LIST` → confirm `search` and `ReJSON` are loaded; record versions.
   - Create+drop a probe SVS-VAMANA index → set `caps.svs_vamana` accordingly.
2. If `dataset.drop_indexes`: `FT.DROPINDEX idx:product DD` (DD also deletes docs) and same for `idx:event`. Ignore "Unknown Index name" errors.
3. Generate centroids (deterministic) → in-memory.
4. Pre-generate query corpus → in-memory + dump JSON to `out_dir/corpus.json` for the report.
5. Create indexes (with SVS-VAMANA fallback if unsupported).
6. Write products in pipelined batches of 500 across `pool_size` goroutines. Use `JSON.SET` for products, `HSET` for events.
7. Wait for indexing: poll `FT.INFO` until `indexing == 0` (or timeout 10 min). Log throughput.
8. Persist a `out_dir/preload.manifest.json` (seed, dataset size, capabilities, index DDL) for reproducibility.

**Idempotency:** preload writes `meta:trafficgen:manifest` with the manifest hash; if `--resume` and the hash matches, skip preload entirely.

---

## 16. Testing Strategy

- **Unit tests** for `datagen` (deterministic outputs, vector normalization invariants, geo bounds, taxonomy coverage).
- **Unit tests** for `coverage.Tracker` (concurrency-safe, missing-feature reporting).
- **Integration tests** under `//go:build integration` that spin `redis-stack:8.4` via Testcontainers-go:
  - Index creation succeeds (with both SVS-VAMANA paths).
  - Each op executes against a 1k-doc dataset.
  - Coverage tracker hits ≥ `len(features) - 2` after running every op once.
  - Assertions fire on intentionally-broken queries (negative tests).
- **Smoke scenario** `scenarios/smoke.yaml`: 1k products, 5k events, 30s steady phase, all ops weighted ≥ 1. CI runs this on every PR.

---

## 17. Reproducibility Contract

A run is reproducible iff:
1. Same Redis version + module versions (recorded in manifest).
2. Same `seed` and `dataset.*`.
3. Same scenario YAML hash.
4. Same Go toolchain major.minor version.

The reporter emits a `repro_key` = `sha256(seed || yaml || redis_ver || go_ver)`. Two runs sharing a `repro_key` should produce identical FT.* command streams (verify in tests by replaying through a recording client).

---

## 18. Open Issues for Implementer to Resolve

1. **go-redis FT.HYBRID typed support**: confirm at implementation time whether v9.7+ has shipped a typed builder. If yes, prefer it over `rdb.Do`; the spec assumes `rdb.Do` as a safe baseline.
2. **go-redis SVS-VAMANA option struct**: verify whether `redis.FTVectorArgs` exposes an `SVSVamanaOptions` field. If not, use raw `rdb.Do("FT.CREATE", ...)` only for the `img_vec` field; keep the rest typed.
3. **JSON FP16 storage**: Redis 8 accepts FP16 vector fields backed by FP32 JSON arrays (engine quantizes). Verify against the connected Redis at preload (a single probe write+read) and skip FP16 storage if it errors.
4. **RESP3 vs RESP2 result shapes** for `FT.HYBRID`: implement both branches in `parseHybridRowCount`; pick by `rdb.Options().Protocol`.
5. **`FT.CONFIG SET WORKER_THREADS` runtime semantics**: the docs note this requires restart on some builds. Detect by reading `FT.CONFIG GET` after `SET`; if unchanged, mark feature as `attempted` instead of `exercised`.
6. **Sortable on TEXT + NOSTEM combination**: confirm Redis accepts both modifiers together on `title`. If not, drop SORTABLE on `title` and move sort tests to `brand`.
7. **`GEOSHAPE` requires DIALECT 3** — make geo-polygon queries explicitly request `DialectVersion: 3` and gate behind `caps.dialect_3` (always true on 8.0+).
8. **Closed-source SVS optimizations** (LeanVec/LVQ): assume open-source build → expect SQ8 fallback. `caps.svs_compression_supported` records which compressions were accepted.
9. **Background GC visibility**: `FT.INFO` exposes `gc_stats` only on some versions. Reporter should print "n/a" gracefully when absent.
10. **`FT.CONFIG GET DEFAULT_DIALECT`** key spelling has varied (`DEFAULT_DIALECT` vs `default-dialect`). Probe both at startup.

---

## 19. Minimal "Hello, P0" Scenario (`scenarios/smoke.yaml`)

```yaml
name: smoke
seed: 1
redis:
  addrs: ["127.0.0.1:6379"]
  protocol: 3
dataset:
  products: 1000
  events:   5000
  preload:  true
  drop_indexes: true
phases:
  - name: warmup
    duration: 10s
    target_qps: 200
    concurrency: 8
  - name: steady
    duration: 30s
    target_qps: 1000
    concurrency: 16
  - name: config_sweep_phase
    duration: 10s
    target_qps: 0
    concurrency: 2
    mix_overrides:
      ft_search_text: 0
      ft_search_knn:  0
      config_sweep:  100
mix:
  ft_search_text:           20
  ft_search_fielded_bool:   15
  ft_search_prefix:          8
  ft_search_wildcard:        2
  ft_search_geo:             6
  ft_search_knn:            15
  ft_search_knn_prefilter:  10
  ft_search_knn_flat_l2:     2
  ft_search_knn_svs_ip:      2
  ft_aggregate_facet:        8
  ft_aggregate_analytics:    5
  ft_hybrid_rrf:             8
  ft_hybrid_linear:          4
  hset_update:               3
  json_set_update:           2
  info_poll:                 1
  timeout_probe:             1
config_sweeps:
  timeout_ms:           [50, 200, 1000]
  default_dialect:      [2, 3]
  worker_threads:       [0, 4, 8]
  maxprefixexpansions:  [200, 1024]
  minprefix:            [1, 2]
assertions:
  bm25_descending: { enabled: true, sample_rate: 0.05, severity: error }
  knn_recall_at_10: { enabled: true, sample_rate: 0.05, min_recall: 0.80, severity: warn }
  hybrid_top1_in_either_leg: { enabled: true, sample_rate: 0.05, severity: warn }
  prefix_membership: { enabled: true, sample_rate: 0.05, severity: error }
coverage:
  min_features_exercised: 50
metrics:
  prometheus_listen: ":9100"
  out_dir: "./out/smoke"
logging:
  level: info
  format: console
```

---

## 20. Acceptance Criteria (DoD)

The implementation is considered complete when:
- [ ] `trafficgen full --config scenarios/smoke.yaml` against a fresh `redis-stack:8.4` exits 0 in < 90s.
- [ ] Coverage report shows ≥ 50 features with non-zero counts.
- [ ] All 4 sampled assertions execute at least once and pass.
- [ ] `out/smoke/<run>.summary.json` is well-formed and contains per-op p50/p95/p99 latencies.
- [ ] `out/smoke/<run>.<op>.hgrm` files are readable by `HistogramLogProcessor`.
- [ ] Prometheus endpoint at `:9100/metrics` exposes `trafficgen_*` and `trafficgen_features_exercised{feature=...}` metrics.
- [ ] Integration tests pass (`go test -tags=integration ./...`).
- [ ] Re-running with the same seed produces identical `repro_key` and identical preloaded document SHAs (verified by a side `keys-snapshot` script).
- [ ] README documents the smoke flow, the YAML schema, and the exit codes.

---

## 21. Implementation Findings (live Redis 8.6.2, Enterprise QA)

Empirical learnings from getting the MVP running end-to-end. Each item
overrides or supplements the spec above; cite this section when reviewing
future patches.

### 21.1 Capability probing

- **`MODULE LIST` is hidden** on Redis Enterprise managed clusters — it
  returns `[]` even when RediSearch and RedisJSON are loaded. The probe
  must fall back to **`FT._LIST`** (proves Search) and **`JSON.GET <missing>`**
  → `redis.Nil` (proves JSON). Treat *any* error other than “unknown
  command” as “module loaded but the command had other issues.”
- **`FT.DROPINDEX` error string is `SEARCH_INDEX_NOT_FOUND`**, not
  “Unknown index name”. The idempotent-drop matcher needs both.

### 21.2 go-redis v9.7 quirks (with empirical evidence)

- **Typed `FTSearch*` / `FTAggregate*` panic under RESP3** with
  `RESP3 responses for this command are disabled because they may still
  change. Please set the flag UnstableResp3.` → set
  `UniversalOptions.UnstableResp3 = true`. The flag is **not** exposed on
  `ClusterOptions` in v9.7.
- **Typed `FTAggregateWithArgs` renders `LIMIT <n>` without the required
  offset arg** under any protocol. Server replies
  `SEARCH_ARG_UNRECOGNIZED Unknown argument <n>`. Workaround: issue
  `FT.AGGREGATE` via raw `rdb.Do(ctx, ...)`.
- **Typed `FTSearchWithArgs` RESP3 parser returns `Total=0`** against
  Redis 8.6.x even when raw `FT.SEARCH` returns docs (verified: raw =
  19 docs, typed = 0). Workaround: pin scenarios to `protocol: 2`. Raw
  `rdb.Do` paths are unaffected either way.

### 21.3 FT.HYBRID on Redis 8.6.x

- **`DIALECT` is rejected** — matches §18 / painPoints. Probe at startup
  and store the result; future builds may flip this.
- **`WINDOW` is rejected** under `COMBINE RRF`. The spec text and
  painPoints reference `RRF 2 CONSTANT 60 WINDOW 20`; this server only
  accepts up to `RRF 2 CONSTANT 60`. Workaround: drop `WINDOW`. Default
  window is server-side.
- **Result rows use `__key`** (and `__score`) under both RESP2 flat
  arrays and RESP3 maps. The spec example assumed `id` — wrong on this
  build. Parsers must check both `__key` and `id`.

### 21.4 Redis Flex (Search-on-Disk) — a separate mode

This isn't a quirk; Flex is a distinct backend exposed through the same
FT.* commands but with a much narrower feature surface. The trafficgen
auto-detects Flex (try a JSON-backed `FT.CREATE`; catch
`SEARCH_FLEX_UNSUPPORTED_FT_CREATE_ARGUMENT Only HASH is supported as
index data type for Flex indexes`) and switches the schema and op set.
A YAML `redis.flex_mode: auto | force | disable` (plus `--flex` CLI)
overrides auto-detection.

**Schema restrictions on Flex (verified by probing 8.6.2 Flex):**

| Restriction | Resolution |
|---|---|
| `ON JSON` rejected | Use `ON HASH`. Store vectors as raw FP32 bytes via `HSET`. |
| `SKIPINITIALSCAN` mandatory on `FT.CREATE` | Always emit it. |
| `FLOAT16` rejected (`Disk index does not support FLOAT16 vector type`) | Use FP32 for every vector field, including `img_vec`. |
| `FLAT` rejected | Drop `feat_vec` from the index (still write the field to the hash so it round-trips). |
| `SVS-VAMANA` rejected | Same — Flex has no SVS variant. |
| HNSW requires `M`, `EF_CONSTRUCTION`, **`EF_RUNTIME`**, **`RERANK TRUE`** | Emit all four; `RERANK` only accepts the literal `TRUE`. |
| `NUMERIC`, `GEO`, `GEOSHAPE` rejected (`SEARCH_FLEX_UNSUPPORTED_FIELD`) | Drop `price`, `rating`, `created_ts`, `store_location`, `pickup_zone` from the schema (still write them to the hash). |
| `SORTABLE` accepted but pointless | Drop — SORTBY is blocked anyway. |
| `FT.DROPINDEX … DD` rejected (`DD is not supported in Redis Flex`) | Drop without `DD`. Docs survive; SKIPINITIALSCAN means stale docs aren't re-indexed by the new index. |

**Query restrictions on Flex:**

- **`FT.SEARCH` must use `NOCONTENT` or `RETURN 0`** — otherwise rejected.
- **`SORTBY`, `LOAD`, `SLOP`, `INORDER`, `HIGHLIGHT`, `SUMMARIZE` rejected.**
- **`FT.AGGREGATE`, `FT.HYBRID`, `FT.CURSOR`, `FT.ALTER` rejected** — drop these ops from the registry when `IsFlex`.
- **KNN still works** — `*=>[KNN k @desc_vec $qv]` returns top-K in score order without needing SORTBY.
- **Sampled assertions don't fire on Flex** because:
  - `prefix_membership` needs returned titles (blocked by NOCONTENT).
  - `knn_recall_at_10` needs a FLAT side index for ground truth.
  - `hybrid_top1_in_either_leg` needs `FT.HYBRID`.
- **Prefix queries returned 0 results** in a 1k-doc Flex smoke despite text
  queries against the same field working — the Flex prefix-expansion path
  has different semantics from in-memory and is **TBD** (worth its own
  investigation; current code just measures latency without asserting on
  result counts).

### 21.5 Go / toolchain

- **`math/rand/v2` lacks `NewZipf`** (and `Zipf`); v1 has it but the spec
  pins v2. Replace with hand-rolled biased sampler
  (`int(n * u^2)`, u ∈ Uniform(0,1)) — close enough to Zipfian for
  taxonomy / country / metro draws.
- **yaml.v3 does not natively decode `time.Duration`** from `"10s"`
  strings. Define a `Duration time.Duration` wrapper with
  `UnmarshalYAML`; expose `.D()` to get the underlying `time.Duration`.
- **`go vet` defer ordering**: when a function starts background
  goroutines that watch a context, register `defer wg.Wait()` *before*
  `defer cancel()` so LIFO ordering cancels the context first and then
  drains the goroutines (otherwise `wg.Wait()` deadlocks).

### 21.6 Throughput shape (Redis Enterprise QA, single shard)

Measured against `redis-11000.aws-alon-5160.env0.qa.redislabs.com:11000`
(Redis 8.6.2, non-Flex). All from the same generator; only concurrency
and physical location varied:

| Setup | Concurrency | Throughput | p50 |
|---|---|---|---|
| Local dev → AWS Tel Aviv (~75ms RTT) | 16 (smoke) | 176 ops/s | 75ms |
| Local dev → AWS Tel Aviv | 96-128 (load) | **260 ops/s** | 320ms |
| Local dev → AWS Tel Aviv | 192-256 (heavy) | 91 ops/s | 2080ms |
| Same AWS region (node1) | 16 (smoke) | **426 ops/s** | <1ms |
| Same AWS region (node1) | 192-256 (heavy) | 96 ops/s | 1990ms |

Two operating regimes worth being explicit about:

- **RTT-dominated** (low concurrency, cross-region): p50 = link RTT, so
  in-region moves smoke p50 by ~180× (75ms → <1ms).
- **Server-dominated** (high concurrency, anywhere): the database is the
  bottleneck. In-region only nudges heavy throughput +5%; concurrency
  past ~128 makes both throughput and latency worse on a one-shard QA
  cluster. The throughput-vs-latency knee is around 32-96 in-region
  workers for this cluster size.

If you need to push past the server-side knee, the lever is **multiple
trafficgen processes** against the same endpoint, not deeper concurrency
inside one client. Each adds a fresh connection pool until the cluster's
own CPU/network ceiling is hit.

### 21.7 Operational notes

- **Preload pipeline timeouts**: a 3s `read_timeout` was insufficient for
  the initial JSON.SET burst against a cross-region endpoint at heavy
  scale; bump to ≥10s. The Flex HSET path is leaner (no JSON parsing
  server-side) and tolerates the same dataset with a shorter timeout.
- **Background goroutines**: `info_poll` and `anchor_verify` run as
  dedicated tickers (every 5s) rather than mix entries. This keeps the
  cadence independent of the worker pool's rate budget.
- **Connection pool sizing**: auto-grow to `max(cfg.PoolSize,
  MaxPhaseConcurrency)` so deep-concurrency phases never starve workers
  on connection acquire.

### 21.8 Things still NOT in the MVP (explicitly)

These were intentionally deferred during the implementation interview
(see commit history). When picking them up, the §21 findings above are
prerequisites:

- Write ops during runtime (`hset_update`, `json_set_update`).
- Prometheus exporter + HdrHistogram `.hgrm` files.
- Testcontainers integration suite.
- `bm25_descending` assertion (cheap; trivial follow-up).
- `ft_search_fielded_bool` with `INFIELDS`, `ft_search_wildcard`,
  `ft_search_geo`, `ft_search_knn_prefilter`, `ft_hybrid_linear`,
  `ft_aggregate_analytics`, `config_sweep`, `timeout_probe`.
- The `ecommerce_p0_full.yaml` and `feature_coverage_ci.yaml` scenarios.
