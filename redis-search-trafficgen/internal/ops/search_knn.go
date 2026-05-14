package ops

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/alon-redis/redis-search-trafficgen/internal/coverage"
	"github.com/alon-redis/redis-search-trafficgen/internal/datagen"
)

// KNNOp runs `*=>[KNN 10 @desc_vec $qv AS score]` and sorts by score.
// Spec'd as the primary KNN driver; assertion sampler may piggyback on it for
// knn_recall_at_10 (computed against feat_vec FLAT as ground truth).
type KNNOp struct{}

func (KNNOp) Name() string { return "ft_search_knn" }

func (KNNOp) Features() []coverage.Feature {
	return []coverage.Feature{
		coverage.FeatKNN, coverage.FeatVecHNSW, coverage.FeatDistCosine,
		coverage.FeatSortBy, coverage.FeatLimit, coverage.FeatReturn,
		coverage.FeatVecFP32, coverage.FeatDialect2,
	}
}

const knnK = 10

func (KNNOp) Execute(ctx context.Context, w *WorkerCtx) (ExecResult, error) {
	qv := w.Corpus.QueryVecDesc[w.RNG.IntN(len(w.Corpus.QueryVecDesc))]
	q := "*=>[KNN 10 @desc_vec $qv AS score]"
	flex := IsFlex(w.Caps)
	qvBytes := datagen.F32ToBytesLE(qv)
	opts := &redis.FTSearchOptions{
		DialectVersion: 2,
		Params:         map[string]interface{}{"qv": qvBytes},
		LimitOffset:    0,
		Limit:          knnK,
	}
	if flex {
		// Flex blocks SORTBY entirely; KNN inherently returns results in
		// score order so no explicit sort is needed. NOCONTENT is also required.
		opts.NoContent = true
	} else {
		opts.Return = []redis.FTSearchReturn{{FieldName: "sku"}, {FieldName: "score"}}
		opts.SortBy = []redis.FTSearchSortBy{{FieldName: "score", Asc: true}}
	}
	var reqStr string
	if w.Debug {
		dbgArgs := []interface{}{"FT.SEARCH", w.Cfg.Indexes.Product.Name, q,
			"PARAMS", "2", "qv", qvBytes}
		if flex {
			dbgArgs = append(dbgArgs, "NOCONTENT")
		} else {
			dbgArgs = append(dbgArgs, "RETURN", "2", "sku", "score",
				"SORTBY", "score")
		}
		dbgArgs = append(dbgArgs, "LIMIT", "0", fmt.Sprintf("%d", knnK), "DIALECT", "2")
		reqStr = formatRequestArgs(dbgArgs)
	}
	start := time.Now()
	res, err := w.Rdb.FTSearchWithArgs(ctx, w.Cfg.Indexes.Product.Name, q, opts).Result()
	lat := time.Since(start)
	if err != nil {
		return ExecResult{Latency: lat, RequestString: reqStr}, err
	}
	skus := skus(res.Docs)
	out := ExecResult{
		Latency:     lat,
		ResultCount: res.Total,
		TopIDs:      skus,
		AssertHint: map[string]interface{}{
			"qv":   qv,
			"skus": skus,
		},
		RequestString: reqStr,
	}
	if w.Debug {
		out.ResponseSummary = formatResponseSummary(skus, res.Total)
	}
	return out, nil
}

func skus(docs []redis.Document) []string {
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		if s, ok := d.Fields["sku"]; ok {
			out = append(out, s)
		} else {
			out = append(out, d.ID)
		}
	}
	return out
}
