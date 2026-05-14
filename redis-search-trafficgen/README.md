# redis-search-trafficgen

A Go traffic generator that exercises Redis Search P0 features on a synthetic
e-commerce corpus. MVP scope: text / prefix / KNN / FT.HYBRID RRF /
FT.AGGREGATE facet, plus a live anchor-verification thread and an FT.INFO
poll.

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

# Probe the connected Redis (version, modules, SVS-VAMANA, HYBRID+DIALECT)
./bin/trafficgen capabilities --config scenarios/smoke.yaml \
    --redis-addr 127.0.0.1:6379

# Preload + run the smoke scenario
./bin/trafficgen full --config scenarios/smoke.yaml \
    --redis-addr 127.0.0.1:6379
```

Reports land in `<metrics.out_dir>/<scenario>/<scenario>-<timestamp>.{summary.json,txt}`.

## CLI

```
trafficgen [global flags] <command>

Commands:
  preload      Create indexes + write the dataset.
  run          Execute phases against an already-loaded dataset.
  full         preload + run in one shot. Most common.
  validate     Parse and validate the YAML; no Redis traffic.
  drop         FT.DROPINDEX + DEL the prefix (DESTRUCTIVE; requires --yes).
  capabilities Probe Redis: version, modules, SVS-VAMANA, FT.HYBRID DIALECT.
  version      Print the trafficgen version.

Global flags:
  --config PATH        YAML scenario (required for all except `version`)
  --redis-addr HOST:PORT  Override redis.addrs[0]
  --seed UINT64        Override config seed
  --out-dir PATH       Override metrics.out_dir
  --log-level LEVEL    debug|info|warn|error  (default info)
```

## YAML schema (overview)

```yaml
name: <string>            # required; used for run-id prefix
seed: <uint64>            # required; deterministic command stream key
redis:
  addrs: [host:port, ...] # required; multiple addrs => cluster mode
  cluster: false          # force cluster against a single addr (Enterprise)
  protocol: 3             # RESP3 default
  pool_size: 0            # 0 = max(4*GOMAXPROCS, max phase concurrency)
  tls: { enabled: false, insecure_skip_verify: false }
dataset:
  products: 1000
  events:   5000
  preload:  true
  drop_indexes: true
  flush_db: false         # DANGEROUS in shared environments
indexes:
  product: { name: "idx:product", prefix: "product:" }
  event:   { name: "idx:event",   prefix: "event:"   }
vectors:
  desc_dim: 384  img_dim: 512  feat_dim: 8  clusters: 50
phases:
  - name: warmup
    duration: 10s
    target_qps: 200       # 0 = closed loop (use concurrency)
    concurrency: 8
    op_timeout: 3s
mix:                      # op_name: weight
  ft_search_text:     25
  ft_search_prefix:   10
  ft_search_knn:      20
  ft_hybrid_rrf:      15
  ft_aggregate_facet: 10
assertions:
  prefix_membership:        { enabled: true, sample_rate: 0.05, severity: error }
  knn_recall_at_10:         { enabled: true, sample_rate: 0.02, min_recall: 0.05, severity: warn }
  hybrid_top1_in_either_leg:{ enabled: true, sample_rate: 0.05, severity: warn }
metrics:
  out_dir: "./out"
logging:
  level: info
```

Unknown YAML keys are rejected by the strict decoder (typos fail fast).

## Exit codes

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | generator error (config invalid, preload failed, Redis unreachable, etc.) |
| 3 | a query produced a `syntax error` response — generator bug |
| 4 | a sampled assertion with `severity: error` failed at least once |
| 130 | interrupted by SIGINT/SIGTERM |

## What's exercised (MVP)

Ops:
- `ft_search_text` — fielded boolean BM25 search.
- `ft_search_prefix` — `@title:hel*`, MINPREFIX floored at 2.
- `ft_search_knn` — `*=>[KNN 10 @desc_vec $qv]` over HNSW COSINE.
- `ft_hybrid_rrf` — FT.HYBRID SEARCH + VSIM, RRF fusion (DIALECT omitted
  unless the capability probe says the server accepts it).
- `ft_aggregate_facet` — GROUPBY @categories REDUCE COUNT.

Background:
- `FT.INFO` poll every 5 s (drives `ft_info` / `info_search` coverage).
- Anchor doc (`product:0`, title `"Alon Shmuely QA architect"`) verified
  every 5 s via FT.SEARCH. Failures land in the report.

Sampled assertions:
- `prefix_membership` — every returned title starts with the queried prefix.
- `knn_recall_at_10` — side query against FLAT `feat_vec` as a coarse
  ground-truth overlap signal.
- `hybrid_top1_in_either_leg` — top hybrid doc must appear in the top-N of
  either the SEARCH leg or the VSIM leg in isolation.

## What's *not* in the MVP

- Writes during runtime (HSET / JSON.SET updates) — preload only.
- Wildcard / fielded-boolean stress, geo, KNN prefilter, FT.HYBRID LINEAR,
  config_sweep, timeout_probe.
- Prometheus exporter, HdrHistogram `.hgrm` files.
- Testcontainers / integration test scaffolding.
- The `feature_coverage_ci.yaml` and `ecommerce_p0_full.yaml` scenarios.
- `bm25_descending` assertion (cheap; can be added when needed).

## Quality rules honored (from `../painPoints.txt`)

- `MINPREFIX` floored at 2 in `ft_search_prefix`.
- `-` is backslash-escaped in TAG values via `EscapeTagValue`.
- `DIALECT` is **never** sent on `FT.HYBRID` unless the startup probe says
  the connected Redis accepts it.
- `YIELD_SCORE_AS` and `EXPLAINSCORE` are not used on `FT.HYBRID`.
- All queries pin `DIALECT 2`; `DIALECT 3` reserved for GEOSHAPE work later.
- Pure-negative queries are never generated (every op has a non-negative seed).
