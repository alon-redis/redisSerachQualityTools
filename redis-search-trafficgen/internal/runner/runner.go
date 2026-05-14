package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"

	"github.com/alon-redis/redis-search-trafficgen/internal/assertx"
	"github.com/alon-redis/redis-search-trafficgen/internal/client"
	"github.com/alon-redis/redis-search-trafficgen/internal/config"
	"github.com/alon-redis/redis-search-trafficgen/internal/coverage"
	"github.com/alon-redis/redis-search-trafficgen/internal/datagen"
	"github.com/alon-redis/redis-search-trafficgen/internal/metrics"
	"github.com/alon-redis/redis-search-trafficgen/internal/ops"
)

// Runner ties everything together. Build once with New; call Run to execute
// every phase against an already-preloaded dataset.
type Runner struct {
	Rdb       redis.UniversalClient
	Cfg       *config.Config
	Caps      *client.Capabilities
	Corpus    *datagen.Corpus
	Coverage  *coverage.Tracker
	Metrics   *metrics.MetricSet
	Asserts   *assertx.Registry
	Log       *slog.Logger
	Registry  map[string]ops.Op
	AnchorKey string

	// SyntaxBug is set true (by a worker) if a query_syntax error was ever
	// observed. Run() returns ErrQuerySyntaxBug at the end if so.
	syntaxBug uint32

	infoStats InfoStats
}

// ErrQuerySyntaxBug is returned by Run if any op ever produced a
// query_syntax error — those indicate generator bugs, not Redis issues.
var ErrQuerySyntaxBug = errors.New("query_syntax error observed; generator bug")

func New(rdb redis.UniversalClient, cfg *config.Config, caps *client.Capabilities, corpus *datagen.Corpus, log *slog.Logger) *Runner {
	return &Runner{
		Rdb:       rdb,
		Cfg:       cfg,
		Caps:      caps,
		Corpus:    corpus,
		Coverage:  coverage.NewTracker(),
		Metrics:   metrics.New(cfg.Metrics.HistogramMaxValueMS, cfg.Metrics.HistogramSignificantDigits),
		Asserts:   assertx.NewRegistry(),
		Log:       log,
		Registry:  ops.Registry(caps),
		AnchorKey: datagen.ProductKey(cfg.Indexes.Product.Prefix, cfg.Seed, 0),
	}
}

// Run executes all phases sequentially.
func (r *Runner) Run(ctx context.Context) error {
	if unknown := UnknownInMix(r.Cfg, r.Registry); len(unknown) > 0 {
		r.Log.Warn("ignoring mix entries that aren't registered (caps gated or typo)", "ops", unknown)
	}

	// Start background tickers (info_poll + anchor verify) for the entire run.
	// Defer order is LIFO: cancelBG runs *before* bgWg.Wait so the goroutines
	// see ctx.Done and exit before we block on the WaitGroup.
	bgCtx, cancelBG := context.WithCancel(ctx)
	var bgWg sync.WaitGroup
	bgWg.Add(2)
	go func() { defer bgWg.Done(); r.runInfoPoll(bgCtx) }()
	go func() { defer bgWg.Done(); r.runAnchorVerify(bgCtx) }()
	if iv := r.Cfg.Metrics.LiveInterval.D(); iv > 0 {
		bgWg.Add(1)
		go func() { defer bgWg.Done(); r.runLiveTicker(bgCtx, iv) }()
	}
	defer bgWg.Wait()
	defer cancelBG()

	for i, ph := range r.Cfg.Phases {
		r.Log.Info("phase starting", "n", i+1, "name", ph.Name, "duration", ph.Duration.D(), "qps", ph.TargetQPS, "concurrency", ph.Concurrency)
		if err := r.runPhase(ctx, ph); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("phase %s: %w", ph.Name, err)
		}
	}

	if atomic.LoadUint32(&r.syntaxBug) != 0 {
		return ErrQuerySyntaxBug
	}
	return nil
}

func (r *Runner) runPhase(ctx context.Context, ph config.PhaseConfig) error {
	mix, err := BuildMix(r.Cfg.Mix, ph.MixOverrides, r.Registry)
	if err != nil {
		return err
	}

	phaseCtx, cancel := context.WithTimeout(ctx, ph.Duration.D())
	defer cancel()

	var limiter *rate.Limiter
	if ph.TargetQPS > 0 {
		burst := ph.TargetQPS / 10
		if burst < 1 {
			burst = 1
		}
		limiter = rate.NewLimiter(rate.Limit(ph.TargetQPS), burst)
	}

	timeout := ph.OpTimeout.D()
	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	var wg sync.WaitGroup
	for i := 0; i < ph.Concurrency; i++ {
		wg.Add(1)
		w := &Worker{
			ID:      i,
			Mix:     mix,
			Limiter: limiter,
			Timeout: timeout,
			Runner:  r,
			RNG:     datagen.WorkerRNG(r.Cfg.Seed, uint64(1<<24)+uint64(i), i),
		}
		go func() {
			defer wg.Done()
			w.Loop(phaseCtx)
		}()
	}
	wg.Wait()
	return nil
}
