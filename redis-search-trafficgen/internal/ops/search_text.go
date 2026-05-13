package ops

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/alon-redis/redis-search-trafficgen/internal/coverage"
)

// TextOp runs a fielded boolean FT.SEARCH over @description and @brand.
// Painpoints honored: parens around grouped multi-word clauses; TAG values
// escaped; never starts with a negation; DIALECT 2 pinned.
type TextOp struct{}

func (TextOp) Name() string { return "ft_search_text" }

func (TextOp) Features() []coverage.Feature {
	return []coverage.Feature{
		coverage.FeatSearchFielded,
		coverage.FeatBoolAnd, coverage.FeatBoolOr, coverage.FeatBoolNot, coverage.FeatBoolOptional,
		coverage.FeatBM25, coverage.FeatLimit, coverage.FeatReturn, coverage.FeatDialect2,
	}
}

func (TextOp) Execute(ctx context.Context, w *WorkerCtx) (ExecResult, error) {
	term1 := w.Corpus.CommonTerms[w.RNG.IntN(len(w.Corpus.CommonTerms))]
	term2 := w.Corpus.CommonTerms[w.RNG.IntN(len(w.Corpus.CommonTerms))]
	cat := EscapeTagValue(w.Corpus.Categories[w.RNG.IntN(len(w.Corpus.Categories))])

	// Composition: (term1 OR term2) AND categories:{cat} AND in_stock:{true}.
	// Brand was dropped from the AND chain because brand×category×term3 over
	// 1k docs is too selective (97% zero-rate on the previous smoke). Brand
	// only joins the conjunction ~30% of the time now, as a stress variant.
	bool_extra := ""
	if w.RNG.Float64() < 0.3 {
		brand := EscapeTagValue(w.Corpus.Brands[w.RNG.IntN(len(w.Corpus.Brands))])
		bool_extra = fmt.Sprintf(" @brand:{%s}", brand)
	}
	q := fmt.Sprintf("(@description:%s|@description:%s) @categories:{%s} @in_stock:{true}%s",
		term1, term2, cat, bool_extra)
	if w.RNG.Float64() < 0.3 {
		q += " ~comfortable"
	}

	start := time.Now()
	res, err := w.Rdb.FTSearchWithArgs(ctx, w.Cfg.Indexes.Product.Name, q, &redis.FTSearchOptions{
		DialectVersion: 2,
		Return:         []redis.FTSearchReturn{{FieldName: "sku"}, {FieldName: "brand"}},
		LimitOffset:    0,
		Limit:          10,
	}).Result()
	lat := time.Since(start)
	if err != nil {
		return ExecResult{Latency: lat}, err
	}
	return ExecResult{
		Latency:     lat,
		ResultCount: res.Total,
		TopIDs:      docIDs(res.Docs),
	}, nil
}

func docIDs(docs []redis.Document) []string {
	out := make([]string, len(docs))
	for i, d := range docs {
		out[i] = d.ID
	}
	return out
}
