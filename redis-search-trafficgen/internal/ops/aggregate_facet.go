package ops

import (
	"context"
	"time"

	"github.com/alon-redis/redis-search-trafficgen/internal/coverage"
)

// AggregateFacetOp runs an FT.AGGREGATE over the product index, grouping by
// category and counting. Top 20 by count. Uses rdb.Do because go-redis v9.7's
// FTAggregateWithArgs renders LIMIT without the required offset arg, which
// the server rejects with SEARCH_ARG_UNRECOGNIZED.
type AggregateFacetOp struct{}

func (AggregateFacetOp) Name() string { return "ft_aggregate_facet" }

func (AggregateFacetOp) Features() []coverage.Feature {
	return []coverage.Feature{
		coverage.FeatGroupBy, coverage.FeatReduceCount, coverage.FeatSortBy, coverage.FeatLimit,
		coverage.FeatDialect2,
	}
}

func (AggregateFacetOp) Execute(ctx context.Context, w *WorkerCtx) (ExecResult, error) {
	args := []interface{}{
		"FT.AGGREGATE", w.Cfg.Indexes.Product.Name, "@in_stock:{true}",
		"GROUPBY", "1", "@categories",
		"REDUCE", "COUNT", "0", "AS", "n",
		"SORTBY", "2", "@n", "DESC",
		"LIMIT", "0", "20",
		"DIALECT", "2",
	}
	start := time.Now()
	res, err := w.Rdb.Do(ctx, args...).Result()
	lat := time.Since(start)
	if err != nil {
		return ExecResult{Latency: lat}, err
	}
	return ExecResult{
		Latency:     lat,
		ResultCount: parseAggregateRowCount(res),
	}, nil
}

func parseAggregateRowCount(res interface{}) int {
	switch v := res.(type) {
	case map[interface{}]interface{}:
		for k, val := range v {
			if s, ok := k.(string); ok && s == "total_results" {
				return asInt(val)
			}
		}
	case map[string]interface{}:
		if t, ok := v["total_results"]; ok {
			return asInt(t)
		}
	case []interface{}:
		if len(v) > 0 {
			return asInt(v[0])
		}
	}
	return 0
}
