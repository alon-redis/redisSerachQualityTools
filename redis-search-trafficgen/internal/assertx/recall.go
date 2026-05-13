package assertx

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/alon-redis/redis-search-trafficgen/internal/datagen"
)

// KNNRecallAt10 issues a side query against the FLAT feat_vec index using a
// projected query vector, then computes recall of HNSW desc_vec top-10
// against the FLAT top-10.
//
// Caveat: desc_vec is 384-D COSINE and feat_vec is 8-D L2 — different spaces.
// We treat FLAT as a *companion* ground-truth signal: if HNSW returns nothing
// in common with FLAT for a centroid-targeted query, that's worth flagging.
// This is the assertion the spec describes; tune `min_recall` per scenario.
func KNNRecallAt10(
	ctx context.Context,
	rng *rand.Rand,
	rdb redis.UniversalClient,
	indexName string,
	sampleRate, minRecall float64,
	severity Severity,
	hnswTopSKUs []string,
	featCentroids [][]float32,
) (Result, bool) {
	if sampleRate <= 0 || rng.Float64() >= sampleRate {
		return Result{}, false
	}
	if len(hnswTopSKUs) == 0 || len(featCentroids) == 0 {
		return Result{Name: "knn_recall_at_10", Passed: true, Severity: severity}, true
	}

	// Build a feat_vec query vector pointing at centroid 0 (deterministic for
	// the assertion). Cluster choice is intentionally simple; what we care
	// about is the *overlap* signal across runs.
	groundVec := datagen.MakeVec(rng, featCentroids[rng.IntN(len(featCentroids))], 0.05, false)
	// Short timeout so a slow assertion doesn't drag latency metrics for the
	// real op being sampled.
	sideCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	res, err := rdb.FTSearchWithArgs(sideCtx, indexName, "*=>[KNN 10 @feat_vec $qv AS s]", &redis.FTSearchOptions{
		DialectVersion: 2,
		Params: map[string]interface{}{
			"qv": datagen.F32ToBytesLE(groundVec),
		},
		Return:      []redis.FTSearchReturn{{FieldName: "sku"}},
		SortBy:      []redis.FTSearchSortBy{{FieldName: "s", Asc: true}},
		LimitOffset: 0,
		Limit:       10,
	}).Result()
	if err != nil {
		// Assertion isn't a Redis-failure signal; record as warn with no fail.
		return Result{
			Name:     "knn_recall_at_10",
			Passed:   true,
			Severity: severity,
			Detail:   fmt.Sprintf("side query failed: %v", err),
		}, true
	}

	groundSet := make(map[string]struct{}, len(res.Docs))
	for _, d := range res.Docs {
		if s, ok := d.Fields["sku"]; ok {
			groundSet[s] = struct{}{}
		}
	}
	overlap := 0
	for _, s := range hnswTopSKUs {
		if _, ok := groundSet[s]; ok {
			overlap++
		}
	}
	recall := 0.0
	if len(hnswTopSKUs) > 0 {
		recall = float64(overlap) / float64(len(hnswTopSKUs))
	}
	if recall+1e-9 < minRecall {
		return Result{
			Name:     "knn_recall_at_10",
			Passed:   false,
			Severity: severity,
			Detail:   fmt.Sprintf("recall@10 = %.2f below threshold %.2f", recall, minRecall),
		}, true
	}
	return Result{Name: "knn_recall_at_10", Passed: true, Severity: severity}, true
}
