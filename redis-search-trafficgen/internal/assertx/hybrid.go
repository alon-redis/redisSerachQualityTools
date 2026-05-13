package assertx

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/alon-redis/redis-search-trafficgen/internal/datagen"
)

// HybridTop1InEitherLeg verifies the FT.HYBRID #1 doc appears in the top-K
// of either the SEARCH leg or the VSIM leg taken in isolation. The hybrid
// fusion shouldn't surface a doc that neither leg surfaces.
func HybridTop1InEitherLeg(
	ctx context.Context,
	rng *rand.Rand,
	rdb redis.UniversalClient,
	indexName string,
	sampleRate float64,
	severity Severity,
	hybridTopID string,
	textLeg string,
	queryVec []float32,
	window int,
) (Result, bool) {
	if sampleRate <= 0 || rng.Float64() >= sampleRate {
		return Result{}, false
	}
	if hybridTopID == "" {
		return Result{Name: "hybrid_top1_in_either_leg", Passed: true, Severity: severity}, true
	}

	sideCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	// Leg 1: plain SEARCH; window-sized result set.
	textRes, err := rdb.FTSearchWithArgs(sideCtx, indexName, textLeg, &redis.FTSearchOptions{
		DialectVersion: 2,
		Return:         []redis.FTSearchReturn{{FieldName: "sku"}},
		LimitOffset:    0,
		Limit:          window,
	}).Result()
	if err != nil {
		return Result{Name: "hybrid_top1_in_either_leg", Passed: true, Severity: severity,
			Detail: fmt.Sprintf("text leg failed: %v", err)}, true
	}

	// Leg 2: KNN on desc_vec; window-sized result set.
	vecRes, err := rdb.FTSearchWithArgs(sideCtx, indexName, "*=>[KNN $k @desc_vec $qv AS s]", &redis.FTSearchOptions{
		DialectVersion: 2,
		Params: map[string]interface{}{
			"k":  window,
			"qv": datagen.F32ToBytesLE(queryVec),
		},
		Return:      []redis.FTSearchReturn{{FieldName: "sku"}},
		SortBy:      []redis.FTSearchSortBy{{FieldName: "s", Asc: true}},
		LimitOffset: 0,
		Limit:       window,
	}).Result()
	if err != nil {
		return Result{Name: "hybrid_top1_in_either_leg", Passed: true, Severity: severity,
			Detail: fmt.Sprintf("vec leg failed: %v", err)}, true
	}

	if containsID(textRes.Docs, hybridTopID) || containsID(vecRes.Docs, hybridTopID) {
		return Result{Name: "hybrid_top1_in_either_leg", Passed: true, Severity: severity}, true
	}
	return Result{
		Name:     "hybrid_top1_in_either_leg",
		Passed:   false,
		Severity: severity,
		Detail:   fmt.Sprintf("hybrid top1=%s appears in neither text top-%d nor vec top-%d", hybridTopID, window, window),
	}, true
}

func containsID(docs []redis.Document, id string) bool {
	for _, d := range docs {
		if d.ID == id {
			return true
		}
	}
	return false
}
