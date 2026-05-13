package ops

import (
	"context"
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
	start := time.Now()
	res, err := w.Rdb.FTSearchWithArgs(ctx, w.Cfg.Indexes.Product.Name, q, &redis.FTSearchOptions{
		DialectVersion: 2,
		Params: map[string]interface{}{
			"qv": datagen.F32ToBytesLE(qv),
		},
		Return: []redis.FTSearchReturn{
			{FieldName: "sku"},
			{FieldName: "score"},
		},
		SortBy:      []redis.FTSearchSortBy{{FieldName: "score", Asc: true}},
		LimitOffset: 0,
		Limit:       knnK,
	}).Result()
	lat := time.Since(start)
	if err != nil {
		return ExecResult{Latency: lat}, err
	}
	skus := skus(res.Docs)
	return ExecResult{
		Latency:     lat,
		ResultCount: res.Total,
		TopIDs:      skus,
		AssertHint: map[string]interface{}{
			"qv":   qv,
			"skus": skus,
		},
	}, nil
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
