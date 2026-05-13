package ops

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/alon-redis/redis-search-trafficgen/internal/coverage"
)

// PrefixOp runs an @title:<prefix>* query. Honors painPoints MINPREFIX=2 floor
// and never emits 1-char prefixes that the server would reject.
type PrefixOp struct{}

func (PrefixOp) Name() string { return "ft_search_prefix" }

func (PrefixOp) Features() []coverage.Feature {
	return []coverage.Feature{
		coverage.FeatSearchPrefix,
		coverage.FeatTextSuffixtrie,
		coverage.FeatLimit, coverage.FeatReturn, coverage.FeatDialect2,
	}
}

func (PrefixOp) Execute(ctx context.Context, w *WorkerCtx) (ExecResult, error) {
	term := w.Corpus.CommonTerms[w.RNG.IntN(len(w.Corpus.CommonTerms))]
	// Floor at 2 chars; cap at 4 so we don't always collapse to the same exact match.
	plen := 2 + w.RNG.IntN(3)
	if plen > len(term) {
		plen = len(term)
	}
	if plen < 2 {
		plen = 2
	}
	prefix := strings.ToLower(term[:plen])
	q := fmt.Sprintf("@title:%s*", prefix)

	flex := IsFlex(w.Caps)
	opts := &redis.FTSearchOptions{
		DialectVersion: 2,
		LimitOffset:    0,
		Limit:          10,
	}
	if flex {
		opts.NoContent = true
	} else {
		opts.Return = []redis.FTSearchReturn{{FieldName: "title"}, {FieldName: "sku"}}
	}
	start := time.Now()
	res, err := w.Rdb.FTSearchWithArgs(ctx, w.Cfg.Indexes.Product.Name, q, opts).Result()
	lat := time.Since(start)
	if err != nil {
		return ExecResult{Latency: lat}, err
	}

	// Collect titles for the prefix_membership assertion.
	titles := make([]string, 0, len(res.Docs))
	for _, d := range res.Docs {
		if t, ok := d.Fields["title"]; ok {
			titles = append(titles, t)
		}
	}
	return ExecResult{
		Latency:     lat,
		ResultCount: res.Total,
		TopIDs:      docIDs(res.Docs),
		AssertHint: map[string]interface{}{
			"prefix": prefix,
			"titles": titles,
		},
	}, nil
}
