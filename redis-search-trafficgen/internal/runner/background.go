package runner

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/alon-redis/redis-search-trafficgen/internal/coverage"
	"github.com/alon-redis/redis-search-trafficgen/internal/datagen"
)

const (
	bgPollInterval   = 5 * time.Second
	anchorPollInterval = 5 * time.Second
)

// InfoStats is the rolling snapshot of FT.INFO + INFO search counters the
// reporter prints at end-of-run. Counters are mutated under atomic + the
// LastXxx fields under mu.
type InfoStats struct {
	mu sync.Mutex

	Polls          uint64
	Errors         uint64
	AnchorVerifies uint64
	AnchorFailures uint64

	LastTotalDocs       int64
	LastNumDocs         int64
	LastIndexing        int64
	LastInvIdxSize      int64
	LastBackgroundIndex int64
}

// InfoStatsView is the lock-free, copyable, JSON-serializable snapshot.
type InfoStatsView struct {
	Polls               uint64 `json:"polls"`
	Errors              uint64 `json:"errors"`
	AnchorVerifies      uint64 `json:"anchor_verifies"`
	AnchorFailures      uint64 `json:"anchor_failures"`
	LastTotalDocs       int64  `json:"last_total_docs"`
	LastNumDocs         int64  `json:"last_num_docs"`
	LastIndexing        int64  `json:"last_indexing"`
	LastInvIdxSize      int64  `json:"last_inverted_sz_mb"`
	LastBackgroundIndex int64  `json:"last_background_indexing"`
}

// InfoStatsSnapshot is the report-side accessor for runner.infoStats.
func (r *Runner) InfoStatsSnapshot() InfoStatsView {
	r.infoStats.mu.Lock()
	defer r.infoStats.mu.Unlock()
	return InfoStatsView{
		Polls:               atomic.LoadUint64(&r.infoStats.Polls),
		Errors:              atomic.LoadUint64(&r.infoStats.Errors),
		AnchorVerifies:      atomic.LoadUint64(&r.infoStats.AnchorVerifies),
		AnchorFailures:      atomic.LoadUint64(&r.infoStats.AnchorFailures),
		LastTotalDocs:       r.infoStats.LastTotalDocs,
		LastNumDocs:         r.infoStats.LastNumDocs,
		LastIndexing:        r.infoStats.LastIndexing,
		LastInvIdxSize:      r.infoStats.LastInvIdxSize,
		LastBackgroundIndex: r.infoStats.LastBackgroundIndex,
	}
}

func (r *Runner) runInfoPoll(ctx context.Context) {
	t := time.NewTicker(bgPollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.pollOnce(ctx)
		}
	}
}

func (r *Runner) pollOnce(ctx context.Context) {
	pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	atomic.AddUint64(&r.infoStats.Polls, 1)
	r.Coverage.Mark(coverage.FeatFTInfo)
	r.Coverage.Mark(coverage.FeatInfoSearch)

	res, err := r.Rdb.Do(pollCtx, "FT.INFO", r.Cfg.Indexes.Product.Name).Result()
	if err != nil {
		atomic.AddUint64(&r.infoStats.Errors, 1)
		return
	}
	r.absorbFTInfo(res)

	// INFO search section. Some builds expose it as `INFO search`, others as
	// `INFO modules`. Try both; ignore errors.
	_, _ = r.Rdb.Info(pollCtx, "search").Result()
	_, _ = r.Rdb.Info(pollCtx, "modules").Result()
}

func (r *Runner) absorbFTInfo(res interface{}) {
	walk := func(get func(string) (interface{}, bool)) {
		r.infoStats.mu.Lock()
		defer r.infoStats.mu.Unlock()
		if v, ok := get("num_docs"); ok {
			r.infoStats.LastNumDocs = int64(asInt(v))
		}
		if v, ok := get("total_docs"); ok {
			r.infoStats.LastTotalDocs = int64(asInt(v))
		}
		if v, ok := get("indexing"); ok {
			r.infoStats.LastIndexing = int64(asInt(v))
		}
		if v, ok := get("inverted_sz_mb"); ok {
			r.infoStats.LastInvIdxSize = int64(asInt(v))
		}
		if v, ok := get("background_indexing"); ok {
			r.infoStats.LastBackgroundIndex = int64(asInt(v))
		}
	}

	switch v := res.(type) {
	case map[interface{}]interface{}:
		walk(func(k string) (interface{}, bool) {
			for kk, vv := range v {
				if s, ok := kk.(string); ok && s == k {
					return vv, true
				}
			}
			return nil, false
		})
	case map[string]interface{}:
		walk(func(k string) (interface{}, bool) {
			vv, ok := v[k]
			return vv, ok
		})
	case []interface{}:
		walk(func(k string) (interface{}, bool) {
			for i := 0; i+1 < len(v); i++ {
				if s, ok := v[i].(string); ok && s == k {
					return v[i+1], true
				}
			}
			return nil, false
		})
	}
}

// runAnchorVerify hits the anchor product on a steady cadence so we get a
// live "is the index still alive?" signal. Counts successes + failures.
func (r *Runner) runAnchorVerify(ctx context.Context) {
	t := time.NewTicker(anchorPollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.anchorVerifyOnce(ctx)
		}
	}
}

func (r *Runner) anchorVerifyOnce(ctx context.Context) {
	atomic.AddUint64(&r.infoStats.AnchorVerifies, 1)
	q := "@title:" + escapeDoubleQuotes(`"`+datagen.AnchorTitle+`"`)
	pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	res, err := r.Rdb.FTSearchWithArgs(pollCtx, r.Cfg.Indexes.Product.Name, q, &redis.FTSearchOptions{
		DialectVersion: 2,
		LimitOffset:    0,
		Limit:          1,
	}).Result()
	total := -1
	if err == nil {
		total = res.Total
	}
	if err != nil || total == 0 {
		atomic.AddUint64(&r.infoStats.AnchorFailures, 1)
		r.Log.Warn("anchor verify failed", "err", err, "total", total)
	}
}

func escapeDoubleQuotes(s string) string {
	// "Alon Shmuely QA architect" stays intact for an exact-phrase match.
	return strings.ReplaceAll(s, "\\", "\\\\")
}
