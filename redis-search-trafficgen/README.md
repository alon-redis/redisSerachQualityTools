# redis-search-trafficgen

A Go traffic generator that exercises Redis Search P0 features on a synthetic
e-commerce corpus. Current op coverage: text / prefix / KNN / FT.HYBRID RRF /
FT.AGGREGATE facet, plus a live anchor-verification thread and an FT.INFO
poll. Auto-adapts to **Redis Flex** (Search-on-Disk) and **OSS Cluster**
endpoints; bounded request/response capture via `--debug-mode`.

Target: **Redis 8.6+** with **RediSearch** and **RedisJSON** modules loaded.

## Quickstart

```bash
sudo apt-get update -y
sudo apt-get install -y git make wget
GO_VER=1.22.6
wget -q https://go.dev/dl/go${GO_VER}.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go${GO_VER}.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
export PATH=$PATH:/usr/local/go/bin
go version    # → "go version go1.22.6 linux/amd64"
git clone https://github.com/alon-redis/redisSerachQualityTools.git

cd redisSerachQualityTools/redis-search-trafficgen
make build              # → ./bin/trafficgen

# Validate a scenario (no Redis traffic)
./bin/trafficgen validate --config scenarios/smoke.yaml

# Probe the connected Redis (version, modules, cluster, Flex, hybrid, etc.)
./bin/trafficgen capabilities --config scenarios/smoke.yaml \
    --redis-addr 127.0.0.1:6379

# Preload + run the smoke scenario
./bin/trafficgen full --config scenarios/smoke.yaml \
    --redis-addr 127.0.0.1:6379
```

Reports land in `<metrics.out_dir>/<scenario>/<scenario>-<timestamp>.{summary.json,txt}`.

## Subcommands

| Subcommand | What it does | Touches data? |
|---|---|---|
| `validate` | Parse + validate YAML; no Redis traffic. | No |
| `capabilities` | Probe Redis: version, modules, cluster, Flex, SVS-VAMANA, FT.HYBRID DIALECT support. | Read-only |
| `preload` | Create indexes + write the deterministic dataset. | Yes — writes (and drops, if `drop_indexes: true`) |
| `run` | Execute phases against an already-loaded dataset. Skips preload. | Read-only (current op mix) |
| `full` | `preload` + `run` in one shot. Most common first-time path. | Yes — writes |
| `drop` | `FT.DROPINDEX … DD` for the configured indexes (DESTRUCTIVE; requires `--yes`). | Yes — destroys |
| `version` | Print trafficgen version. | No |

Repeated runs against the same dataset → use `run`, not `full`, to avoid re-writing.

## CLI flags

| Flag | Default | Purpose |
|---|---|---|
| `--config PATH` | _required_ | Scenario YAML. |
| `--redis-addr HOST:PORT` | — | Overrides `redis.addrs[0]`. |
| `--seed UINT64` | — | Overrides scenario `seed`. |
| `--out-dir PATH` | — | Overrides `metrics.out_dir`. |
| `--log-level` | `info` | `debug` / `info` / `warn` / `error`. |
| `--flex` | `false` | Force Flex schema + op set (same as `redis.flex_mode: force`). |
| `--live-interval` | `0` (use YAML) | Override `metrics.live_interval` — `1s` is the YAML default. Positive values override. |
| `--debug-mode` | `false` | Capture the last 25 errored requests + the 25 slowest into `--debug-file`. |
| `--debug-file` | `/tmp/debug.txt` | Where `--debug-mode` writes the capture. |

## YAML schema (overview)

```yaml
name: <string>            # required; used as run-id prefix
seed: <uint64>            # required; deterministic command stream key

redis:
  addrs: [host:port, ...] # required; multiple addrs => cluster mode (auto)
  username: ""            # optional; also read from REDIS_USERNAME env
  password: ""            # optional; also read from REDIS_PASSWORD env
  db: 0
  protocol: 2             # RESP2 default; RESP3 typed-helpers are unstable in go-redis 9.7
  pool_size: 0            # 0 = max(4*GOMAXPROCS, max phase concurrency)
  min_idle_conns: 0
  read_timeout:  3s
  write_timeout: 3s
  dial_timeout:  5s
  tls:
    enabled: false
    insecure_skip_verify: false
    ca_file: "" ; cert_file: "" ; key_file: ""
  cluster: false          # force ClusterClient even for a single-addr endpoint
  flex_mode: auto         # auto (probe) | force | disable

dataset:
  products: 1000
  events:   5000
  preload:  true          # for `full`; ignored by `run`
  drop_indexes: true      # drop idx:product / idx:event before recreate
  flush_db: false         # FLUSHDB before preload (DANGEROUS in shared envs)

indexes:
  product: { name: idx:product, prefix: "product:" }
  event:   { name: idx:event,   prefix: "event:"   }

vectors:
  desc_dim: 384  img_dim: 512  feat_dim: 8  clusters: 50

phases:
  - name: warmup
    duration: 10s
    target_qps: 200       # 0 = closed loop (uses concurrency only)
    concurrency: 8
    op_timeout: 3s

mix:                      # op_name → weight (normalized internally)
  ft_search_text:     25
  ft_search_prefix:   10
  ft_search_knn:      20
  ft_hybrid_rrf:      15
  ft_aggregate_facet: 10

assertions:
  bm25_descending:           { enabled: false }
  prefix_membership:         { enabled: true, sample_rate: 0.05, severity: error }
  knn_recall_at_10:          { enabled: true, sample_rate: 0.02, min_recall: 0.05, severity: warn }
  hybrid_top1_in_either_leg: { enabled: true, sample_rate: 0.05, severity: warn }

metrics:
  out_dir: "./out"
  histogram_significant_digits: 3
  histogram_max_value_ms: 60000
  live_interval: 1s       # 0 disables live stats

logging:
  level: info             # console-style slog handler on stderr
```

Unknown YAML keys are rejected by the strict decoder (typos fail fast).

## Live stats (default on, 1 s cadence)

While `run` / `full` is in flight, the runner emits per-tick stats to **stderr**:

```
[live 12s] Total ops: 1430 (119.2/s)   Errors: 0   Empty: 412 (28.8%)   num_docs: 1000   Anchor fails: 0
op                        count     errs      p50      p95      p99    p99.9  zero_rate
ft_search_knn               430        0    77.95    83.71    86.59   107.90    0.000
ft_search_text              671        0    78.97    84.09    86.85   104.51    0.518
…
```

On a TTY, the table redraws in place each tick. On a non-TTY (piped to a log file), each tick is a single compact line for easy `tail`/`grep`.

Preload phase also gets its own progress line — important for heavy preloads (e.g. `heavy.yaml` writes 250 k products):

```
[preload 47s] products: 124000/250000 (49.6%)   events: 0/50000 (0.0%)
```

Knobs: `metrics.live_interval: 1s` in YAML; `--live-interval=Xs` overrides; set YAML to `0` to disable.

## Flex (Search-on-Disk) support

Three modes, configured via `redis.flex_mode` (YAML) or `--flex` (CLI shorthand for `force`):

| Mode | Behaviour |
|---|---|
| `auto` (default) | Probe `FT.CREATE ON JSON`. If rejected with `SEARCH_FLEX_UNSUPPORTED_FT_CREATE_ARGUMENT`, treat as Flex. |
| `force` | Use Flex schema + op set regardless of probe. Useful for testing against any HASH-only backend. |
| `disable` | Never use Flex even if the probe says yes. |

When `IsFlex` resolves true:
- `idx:product` switches to a **HASH-backed schema** with `SKIPINITIALSCAN`, HNSW `M / EF_CONSTRUCTION / EF_RUNTIME / RERANK TRUE`, FP32-only vectors. NUMERIC, GEO, GEOSHAPE, FLAT, SVS-VAMANA fields are dropped.
- `idx:event` drops NUMERIC `ts` / `dwell_ms` + SORTABLE.
- Preload writes products via `HSET` with vectors encoded as raw FP32 bytes.
- `FT.DROPINDEX` omits the `DD` keyword (Flex rejects it).
- Op registry drops `ft_hybrid_rrf`, `ft_aggregate_facet`, **and `ft_search_prefix`** (all hard-rejected by the Flex query path).
- All `FT.SEARCH` ops add `NOCONTENT`; KNN drops `SORTBY`.
- Sampled assertions are skipped (no field returns under NOCONTENT, no FLAT side index).

A reference scenario `scenarios/smoke_flex.yaml` ships with `flex_mode: force`.

## Cluster auto-detect

Single-addr endpoints that resolve to multi-shard Redis Enterprise clusters get auto-detected via a `CLUSTER INFO` probe; the trafficgen then uses go-redis's `ClusterClient`, which follows MOVED redirects. Detection accepts either the canonical `cluster_enabled:1` line or — for Redis Enterprise, which omits it — a non-zero `cluster_slots_assigned`. Explicit `redis.cluster: true` overrides the probe.

Known blind spot: some Redis Enterprise ACLs **block `CLUSTER INFO`** (`ERR command is not allowed`); set `redis.cluster: true` in YAML if your endpoint is one of those.

## `--debug-mode` — bounded request/response capture

```bash
./bin/trafficgen run --config scenarios/heavy.yaml \
    --redis-addr <host>:<port> --debug-mode
# … run finishes …
# debug capture written  path=/tmp/debug.txt  entries=N
```

Captures, with negligible overhead when disabled:

- **Last 25 errored requests** — ring buffer; persistent failures keep the tail, intermittent ones still keep the most recent.
- **Top 25 slowest requests** — min-heap of size 25 keyed by latency; O(log 25) per op + one mutex acquire.

Each entry shows timestamp, op name, latency, the exact FT.* request string (byte slices abbreviated as `<N bytes>`), and either the error or a top-IDs response summary. Vector PARAMS are not dumped verbatim. Default output path `/tmp/debug.txt`, override with `--debug-file`.

## Shipped scenarios

| Scenario | Dataset | Duration | Notes |
|---|---|---|---|
| `scenarios/smoke.yaml` | 1 k products / 5 k events | 40 s | The DoD smoke test. Five ops, all assertions enabled. |
| `scenarios/smoke_flex.yaml` | 1 k / 5 k | 40 s | `flex_mode: force`; mix limited to text + KNN. |
| `scenarios/load.yaml` | 10 k / 50 k | ~5 min | 4 phases, max concurrency 128. |
| `scenarios/heavy.yaml` | **250 k** / 50 k | **5 h** | Single closed-loop steady phase, 100 connections. |

## Exit codes

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | generator error (config invalid, preload failed, Redis unreachable, etc.) |
| 3 | a query produced a `syntax error` response — generator bug |
| 4 | a sampled assertion with `severity: error` failed at least once |
| 130 | interrupted by SIGINT/SIGTERM (drains gracefully, writes partial report + debug capture) |

## What's exercised

**Ops** (auto-gated by capabilities):

- `ft_search_text` — fielded boolean BM25 search.
- `ft_search_prefix` — `@title:hel*`, MINPREFIX floored at 2. Dropped on Flex.
- `ft_search_knn` — `*=>[KNN 10 @desc_vec $qv]` over HNSW COSINE.
- `ft_hybrid_rrf` — FT.HYBRID SEARCH + VSIM, RRF fusion. Probed at startup. Dropped on Flex.
- `ft_aggregate_facet` — GROUPBY @categories REDUCE COUNT. Dropped on Flex.

**Background**:

- `FT.INFO` poll every 5 s (drives `ft_info` / `info_search` coverage, surfaces `num_docs` / `indexing` in live ticker).
- Anchor doc (`product:0`, title `"Alon Shmuely QA architect"`) verified every 5 s via FT.SEARCH; failures land in the final report.
- Live stats ticker (see above).

**Sampled assertions** (auto-skipped on Flex):

- `prefix_membership` — every returned title starts with the queried prefix.
- `knn_recall_at_10` — side query against FLAT `feat_vec` as coarse ground-truth overlap.
- `hybrid_top1_in_either_leg` — top hybrid doc must appear in either leg's top-N.

## What's *not* yet in the MVP

- Writes during runtime (HSET / JSON.SET updates) — preload only.
- `ft_search_fielded_bool` with `INFIELDS`, `ft_search_wildcard`, `ft_search_geo`, `ft_search_knn_prefilter`, `ft_hybrid_linear`, `ft_aggregate_analytics`, `config_sweep`, `timeout_probe`.
- Prometheus exporter, HdrHistogram `.hgrm` files.
- Testcontainers / integration test scaffolding.
- `bm25_descending` assertion (cheap; can be added when needed).
- `INFO cluster` fallback for cluster auto-detect when `CLUSTER INFO` is ACL-blocked.

## Quality rules honored (from `../painPoints.txt`)

- `MINPREFIX` floored at 2 in `ft_search_prefix`.
- `-` is backslash-escaped in TAG values via `EscapeTagValue`.
- `DIALECT` is **never** sent on `FT.HYBRID` unless the startup probe says
  the connected Redis accepts it (Redis 8.6 still rejects it).
- `WINDOW` is dropped from `COMBINE RRF` (8.6.x rejects it).
- `YIELD_SCORE_AS` and `EXPLAINSCORE` are not used on `FT.HYBRID`.
- All queries pin `DIALECT 2`; `DIALECT 3` reserved for GEOSHAPE work later.
- Pure-negative queries are never generated (every op has a non-negative seed).

## go-redis v9.7 quirks worked around

- Typed FT.SEARCH / FT.AGGREGATE panic under RESP3 unless `UnstableResp3: true` is set on `UniversalOptions`. Set unconditionally.
- The typed RESP3 FT.SEARCH parser returns `Total=0` on Redis 8.6.x even when raw FT.SEARCH returns docs. Scenarios pin `protocol: 2`.
- The typed `FTAggregateWithArgs` renders `LIMIT N` without the required offset. We issue FT.AGGREGATE via raw `rdb.Do(...)` instead.
- FT.HYBRID has no typed wrapper in v9.7. Done via raw `rdb.Do(...)`.

See `SPEC.md` §21 for the full list of findings from running against live Redis 8.6.2.
