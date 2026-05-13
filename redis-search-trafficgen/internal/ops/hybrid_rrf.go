package ops

import (
	"context"
	"fmt"
	"time"

	"github.com/alon-redis/redis-search-trafficgen/internal/coverage"
	"github.com/alon-redis/redis-search-trafficgen/internal/datagen"
)

// HybridRRFOp runs FT.HYBRID with SEARCH + VSIM legs fused by RRF. No
// DIALECT is sent unless capability probe said the server accepts it (the
// source rejects it as of Redis 8.6, per painPoints). No YIELD_SCORE_AS,
// no EXPLAINSCORE — both flagged as parser-accept-then-reject / TODO.
type HybridRRFOp struct{}

func (HybridRRFOp) Name() string { return "ft_hybrid_rrf" }

func (HybridRRFOp) Features() []coverage.Feature {
	return []coverage.Feature{
		coverage.FeatHybridRRF, coverage.FeatBM25, coverage.FeatVecHNSW, coverage.FeatDistCosine,
		coverage.FeatLimit,
	}
}

const hybridK = 20
const hybridWindow = 20

func (HybridRRFOp) Execute(ctx context.Context, w *WorkerCtx) (ExecResult, error) {
	qv := w.Corpus.QueryVecDesc[w.RNG.IntN(len(w.Corpus.QueryVecDesc))]
	term := w.Corpus.CommonTerms[w.RNG.IntN(len(w.Corpus.CommonTerms))]
	cat := EscapeTagValue(w.Corpus.Categories[w.RNG.IntN(len(w.Corpus.Categories))])

	textLeg := fmt.Sprintf("@description:%s @categories:{%s}", term, cat)

	// painPoints lists WINDOW as part of RRF, but Redis 8.6.x rejects it
	// with "WINDOW: Unknown argument". Stick to the minimal canonical RRF
	// form (count + CONSTANT). Defaults handle the window internally.
	args := []interface{}{
		"FT.HYBRID", w.Cfg.Indexes.Product.Name,
		"SEARCH", textLeg,
		"VSIM", "@desc_vec", "$qv",
		"COMBINE", "RRF", "2", "CONSTANT", "60",
		"LIMIT", "0", "10",
		"PARAMS", "2", "qv", datagen.F32ToBytesLE(qv),
	}
	if w.Caps != nil && w.Caps.HybridAcceptsDialect {
		args = append(args, "DIALECT", "2")
	}

	start := time.Now()
	res, err := w.Rdb.Do(ctx, args...).Result()
	lat := time.Since(start)
	if err != nil {
		return ExecResult{Latency: lat}, err
	}

	n, ids := parseHybrid(res)
	return ExecResult{
		Latency:     lat,
		ResultCount: n,
		TopIDs:      ids,
		AssertHint: map[string]interface{}{
			"qv":       qv,
			"text_leg": textLeg,
			"top_ids":  ids,
		},
	}, nil
}

// parseHybrid handles both the RESP3 map shape and the RESP2 flat slice shape
// of FT.HYBRID. We only need the row count + ordered doc keys for assertions.
func parseHybrid(res interface{}) (int, []string) {
	switch v := res.(type) {
	case map[interface{}]interface{}:
		return parseHybridMap(toStringKeys(v))
	case map[string]interface{}:
		return parseHybridMap(v)
	case []interface{}:
		return parseHybridArray(v)
	default:
		return 0, nil
	}
}

func toStringKeys(m map[interface{}]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		if s, ok := k.(string); ok {
			out[s] = v
		}
	}
	return out
}

func parseHybridMap(m map[string]interface{}) (int, []string) {
	total := 0
	if t, ok := m["total_results"]; ok {
		total = asInt(t)
	}
	var ids []string
	if results, ok := m["results"]; ok {
		if arr, ok := results.([]interface{}); ok {
			for _, row := range arr {
				ids = append(ids, extractRowID(row))
			}
		}
	}
	if total == 0 {
		total = len(ids)
	}
	return total, ids
}

// extractRowID pulls the doc key out of an FT.HYBRID row map. Redis 8.6
// exposes the key as `__key`; older builds may use `id`. Returns "" if
// neither is present.
func extractRowID(row interface{}) string {
	switch m := row.(type) {
	case map[interface{}]interface{}:
		m2 := toStringKeys(m)
		if s, ok := m2["__key"].(string); ok {
			return s
		}
		if s, ok := m2["id"].(string); ok {
			return s
		}
	case map[string]interface{}:
		if s, ok := m["__key"].(string); ok {
			return s
		}
		if s, ok := m["id"].(string); ok {
			return s
		}
	}
	return ""
}

// parseHybridArray walks the RESP2 flat []interface{} shape:
//
//	[total_results, N, results, [[row1...], [row2...], ...], warnings, [], execution_time, F]
//
// Each row is itself a flat array `[__key, "product:xxx", __score, F, ...]`.
func parseHybridArray(arr []interface{}) (int, []string) {
	total := 0
	var ids []string
	for i := 0; i+1 < len(arr); i += 2 {
		key, ok := arr[i].(string)
		if !ok {
			continue
		}
		switch key {
		case "total_results":
			total = asInt(arr[i+1])
		case "results":
			rows, ok := arr[i+1].([]interface{})
			if !ok {
				continue
			}
			for _, row := range rows {
				if rarr, ok := row.([]interface{}); ok {
					ids = append(ids, extractRowKeyFromFlatPairs(rarr))
				} else {
					ids = append(ids, extractRowID(row))
				}
			}
		}
	}
	if total == 0 {
		total = len(ids)
	}
	return total, ids
}

// extractRowKeyFromFlatPairs pulls the `__key` value out of a flat
// [k1, v1, k2, v2, ...] RESP2 row. Returns "" if `__key` (or `id`) absent.
func extractRowKeyFromFlatPairs(pairs []interface{}) string {
	for i := 0; i+1 < len(pairs); i += 2 {
		k, ok := pairs[i].(string)
		if !ok {
			continue
		}
		if k == "__key" || k == "id" {
			if s, ok := pairs[i+1].(string); ok {
				return s
			}
		}
	}
	return ""
}

func asInt(v interface{}) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	case string:
		var n int
		_, _ = fmt.Sscan(x, &n)
		return n
	default:
		return 0
	}
}
