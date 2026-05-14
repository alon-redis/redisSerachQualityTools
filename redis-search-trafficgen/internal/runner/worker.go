package runner

import (
	"context"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/alon-redis/redis-search-trafficgen/internal/assertx"
	"github.com/alon-redis/redis-search-trafficgen/internal/debug"
	"github.com/alon-redis/redis-search-trafficgen/internal/metrics"
	"github.com/alon-redis/redis-search-trafficgen/internal/ops"
)

// Worker is one goroutine in a phase. Picks ops from the mix, executes,
// records metrics + coverage + sampled assertions.
type Worker struct {
	ID      int
	Mix     *Mix
	Limiter *rate.Limiter
	Timeout time.Duration
	Runner  *Runner
	RNG     *rand.Rand
}

// Loop runs until the phase context is cancelled (timeout or signal).
func (w *Worker) Loop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if w.Limiter != nil {
			if err := w.Limiter.Wait(ctx); err != nil {
				return
			}
		}
		opName := w.Mix.Pick(w.RNG)
		op, ok := w.Runner.Registry[opName]
		if !ok {
			continue
		}
		w.executeOne(ctx, op)
	}
}

func (w *Worker) executeOne(ctx context.Context, op ops.Op) {
	opCtx, cancel := context.WithTimeout(ctx, w.Timeout)
	defer cancel()

	debugOn := w.Runner.Debug != nil
	wctx := &ops.WorkerCtx{
		Rdb:    w.Runner.Rdb,
		RNG:    w.RNG,
		Cfg:    w.Runner.Cfg,
		Corpus: w.Runner.Corpus,
		Caps:   w.Runner.Caps,
		Debug:  debugOn,
	}

	res, err := op.Execute(opCtx, wctx)
	if err != nil {
		cls := ops.ClassifyError(err)
		w.Runner.Metrics.RecordError(op.Name(), cls, res.Latency)
		if cls == metrics.ErrClassQuerySyntax {
			atomic.StoreUint32(&w.Runner.syntaxBug, 1)
			w.Runner.Log.Error("query_syntax error", "op", op.Name(), "err", err)
		}
		if debugOn {
			w.Runner.Debug.RecordError(debug.Entry{
				When:    time.Now(),
				Op:      op.Name(),
				Latency: res.Latency,
				Request: res.RequestString,
				Err:     err.Error(),
			})
		}
		return
	}
	w.Runner.Metrics.RecordSuccess(op.Name(), res.Latency, res.ResultCount)
	w.Runner.Coverage.MarkAll(op.Features())
	if debugOn {
		w.Runner.Debug.RecordSuccess(debug.Entry{
			When:     time.Now(),
			Op:       op.Name(),
			Latency:  res.Latency,
			Request:  res.RequestString,
			Response: res.ResponseSummary,
		})
	}

	// Sampled assertions.
	w.runAssertions(opCtx, op.Name(), res)
}

func (w *Worker) runAssertions(ctx context.Context, opName string, res ops.ExecResult) {
	cfg := w.Runner.Cfg
	// Flex strips every signal the assertions need: prefix titles aren't
	// returned (NOCONTENT forced), FLAT feat_vec doesn't exist for the
	// recall side query, and FT.HYBRID itself is unsupported. Skip
	// assertions wholesale rather than emitting noisy false negatives.
	if w.Runner.Caps != nil && w.Runner.Caps.IsFlex {
		return
	}

	switch opName {
	case "ft_search_prefix":
		a := cfg.Assertions.PrefixMembership
		if !a.Enabled {
			return
		}
		prefix, _ := res.AssertHint["prefix"].(string)
		titles, _ := res.AssertHint["titles"].([]string)
		if r, fired := assertx.PrefixMembership(w.RNG, a.SampleRate, severity(a.Severity), prefix, titles); fired {
			w.Runner.Asserts.Record(r)
		}
	case "ft_search_knn":
		a := cfg.Assertions.KNNRecallAt10
		if !a.Enabled {
			return
		}
		skus, _ := res.AssertHint["skus"].([]string)
		if r, fired := assertx.KNNRecallAt10(
			ctx, w.RNG, w.Runner.Rdb, cfg.Indexes.Product.Name,
			a.SampleRate, a.MinRecall, severity(a.Severity),
			skus, w.Runner.Corpus.FeatCentroids,
		); fired {
			w.Runner.Asserts.Record(r)
		}
	case "ft_hybrid_rrf":
		a := cfg.Assertions.HybridTop1InEitherLeg
		if !a.Enabled {
			return
		}
		topIDs, _ := res.AssertHint["top_ids"].([]string)
		var topID string
		if len(topIDs) > 0 {
			topID = topIDs[0]
		}
		qv, _ := res.AssertHint["qv"].([]float32)
		textLeg, _ := res.AssertHint["text_leg"].(string)
		if r, fired := assertx.HybridTop1InEitherLeg(
			ctx, w.RNG, w.Runner.Rdb, cfg.Indexes.Product.Name,
			a.SampleRate, severity(a.Severity),
			topID, textLeg, qv, 20,
		); fired {
			w.Runner.Asserts.Record(r)
		}
	}
}

func severity(s string) assertx.Severity {
	switch s {
	case "error":
		return assertx.SeverityError
	default:
		return assertx.SeverityWarn
	}
}
