package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/alon-redis/redis-search-trafficgen/internal/client"
	"github.com/alon-redis/redis-search-trafficgen/internal/config"
	"github.com/alon-redis/redis-search-trafficgen/internal/datagen"
	"github.com/alon-redis/redis-search-trafficgen/internal/schema"
)

const preloadPipelineBatch = 500

// BuildCorpus builds the in-memory query corpus deterministically from the
// scenario seed without touching Redis. Workers need this so their op
// streams are seed-stable; `run` (no preload) calls this directly so it
// can hit an already-populated index without re-writing anything.
func BuildCorpus(cfg *config.Config) *datagen.Corpus {
	return datagen.BuildCorpus(
		cfg.Seed,
		cfg.Vectors.DescDim, cfg.Vectors.ImgDim, cfg.Vectors.FeatDim, cfg.Vectors.Clusters,
		2000, 200, 10000, 1000, 200,
	)
}

// Preload drops the indexes (if requested), creates them honoring caps, then
// writes the deterministic product + event corpora. Returns the same Corpus
// the runtime phases will use, so call sites don't have to re-derive it.
func Preload(
	ctx context.Context,
	rdb redis.UniversalClient,
	cfg *config.Config,
	caps *client.Capabilities,
	log *slog.Logger,
) (*datagen.Corpus, error) {
	if cfg.Dataset.FlushDB {
		log.Warn("flushing entire Redis DB — set dataset.flush_db: false in shared environments")
		if _, err := rdb.Do(ctx, "FLUSHDB").Result(); err != nil {
			return nil, fmt.Errorf("FLUSHDB: %w", err)
		}
	}

	flex := caps != nil && caps.IsFlex
	if cfg.Dataset.DropIndexes {
		if err := schema.DropProduct(ctx, rdb, cfg.Indexes.Product.Name, flex); err != nil {
			return nil, err
		}
		if err := schema.DropEvent(ctx, rdb, cfg.Indexes.Event.Name, flex); err != nil {
			return nil, err
		}
	}

	corpus := BuildCorpus(cfg)

	if err := schema.CreateProduct(ctx, rdb, schema.ProductIndexOpts{
		Name:    cfg.Indexes.Product.Name,
		Prefix:  cfg.Indexes.Product.Prefix,
		DescDim: cfg.Vectors.DescDim,
		ImgDim:  cfg.Vectors.ImgDim,
		FeatDim: cfg.Vectors.FeatDim,
		UseSVS:  caps != nil && caps.SVSVamana,
		Flex:    flex,
	}); err != nil {
		return nil, err
	}
	if err := schema.CreateEvent(ctx, rdb, schema.EventIndexOpts{
		Name:   cfg.Indexes.Event.Name,
		Prefix: cfg.Indexes.Event.Prefix,
		Flex:   flex,
	}); err != nil {
		return nil, err
	}

	// Progress counters consumed by the (optional) preload progress ticker.
	// Heavy preloads (250k products w/ vectors) take minutes against a
	// cross-region endpoint; without these the user sees nothing between
	// "capabilities probed" and "wrote products".
	var productsWritten, eventsWritten atomic.Int64
	progressCtx, cancelProgress := context.WithCancel(ctx)
	defer cancelProgress()
	if iv := cfg.Metrics.LiveInterval.D(); iv > 0 {
		go preloadProgressTicker(progressCtx, iv,
			&productsWritten, int64(cfg.Dataset.Products),
			&eventsWritten, int64(cfg.Dataset.Events))
	}

	startIdx := cfg.Dataset.StartIndex

	// Stream the product corpus into the writer pool. Channel buffer is
	// generous enough that the generator doesn't stall under normal
	// pipeline latency, but capped so peak memory stays at ~tens of MB
	// regardless of cfg.Dataset.Products. (Previously the whole corpus
	// was materialized in one []ProductDoc; 50M products × ~4KB blew up
	// at ~200 GB.)
	productsCh := make(chan datagen.ProductDoc, preloadPipelineBatch*2)
	go datagen.GenProductsStream(ctx,
		cfg.Seed, cfg.Indexes.Product.Prefix,
		startIdx, cfg.Dataset.Products,
		corpus.DescCentroids, corpus.ImgCentroids, corpus.FeatCentroids,
		productsCh)
	if err := writeProductsConcurrent(ctx, rdb, productsCh, log, flex, &productsWritten); err != nil {
		return nil, fmt.Errorf("writing products: %w", err)
	}
	log.Info("wrote products", "count", cfg.Dataset.Products, "start_index", startIdx, "flex", flex)

	// Events may reference any SKU in the *current total* product space
	// (existing + just-written), so pass startIdx+count as the reference range.
	eventsCh := make(chan datagen.EventDoc, preloadPipelineBatch*2)
	go datagen.GenEventsStream(ctx,
		cfg.Seed, cfg.Indexes.Event.Prefix,
		startIdx, cfg.Dataset.Events,
		startIdx+cfg.Dataset.Products,
		eventsCh)
	if err := writeEventsConcurrent(ctx, rdb, eventsCh, log, &eventsWritten); err != nil {
		return nil, fmt.Errorf("writing events: %w", err)
	}
	log.Info("wrote events", "count", cfg.Dataset.Events, "start_index", startIdx)

	if err := waitForIndexing(ctx, rdb, cfg.Indexes.Product.Name, 10*time.Minute, log); err != nil {
		return nil, err
	}
	if err := waitForIndexing(ctx, rdb, cfg.Indexes.Event.Name, 10*time.Minute, log); err != nil {
		return nil, err
	}

	return corpus, nil
}

// writeProductsConcurrent pulls product docs off `in` and pipelines them
// in batches of preloadPipelineBatch across `workers` writer goroutines.
// Each worker accumulates docs locally — peak memory per worker is one
// batch (~500 docs × ~4 KB ≈ 2 MB), so total resident memory for the
// writer pool stays around 16 MB regardless of how many docs the
// generator produces.
func writeProductsConcurrent(ctx context.Context, rdb redis.UniversalClient, in <-chan datagen.ProductDoc, log *slog.Logger, flex bool, written *atomic.Int64) error {
	const workers = 8
	errs := make(chan error, workers)
	var wg sync.WaitGroup

	flushBatch := func(batch []datagen.ProductDoc) {
		if len(batch) == 0 {
			return
		}
		pipe := rdb.Pipeline()
		for _, d := range batch {
			if flex {
				// Flex requires HASH storage with vectors as raw FP32 bytes.
				pipe.HSet(ctx, d.Key, d.Product.FlatHashFlex()...)
				continue
			}
			b, err := json.Marshal(d.Product)
			if err != nil {
				errs <- err
				return
			}
			pipe.Do(ctx, "JSON.SET", d.Key, "$", b)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			// pipeline-level error; one bad cmd shouldn't fail the rest
			// but go-redis returns the first error. Log and continue.
			log.Warn("product pipeline exec returned error", "err", err)
		}
		if written != nil {
			written.Add(int64(len(batch)))
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]datagen.ProductDoc, 0, preloadPipelineBatch)
			for doc := range in {
				batch = append(batch, doc)
				if len(batch) >= preloadPipelineBatch {
					flushBatch(batch)
					batch = batch[:0]
				}
			}
			flushBatch(batch)
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

// writeEventsConcurrent — analogous streaming consumer for the event corpus.
func writeEventsConcurrent(ctx context.Context, rdb redis.UniversalClient, in <-chan datagen.EventDoc, log *slog.Logger, written *atomic.Int64) error {
	const workers = 8
	errs := make(chan error, workers)
	var wg sync.WaitGroup

	flushBatch := func(batch []datagen.EventDoc) {
		if len(batch) == 0 {
			return
		}
		pipe := rdb.Pipeline()
		for _, d := range batch {
			pipe.HSet(ctx, d.Key, d.Event.FlatHash()...)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			log.Warn("event pipeline exec returned error", "err", err)
		}
		if written != nil {
			written.Add(int64(len(batch)))
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]datagen.EventDoc, 0, preloadPipelineBatch)
			for doc := range in {
				batch = append(batch, doc)
				if len(batch) >= preloadPipelineBatch {
					flushBatch(batch)
					batch = batch[:0]
				}
			}
			flushBatch(batch)
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func waitForIndexing(ctx context.Context, rdb redis.UniversalClient, indexName string, timeout time.Duration, log *slog.Logger) error {
	deadline := time.Now().Add(timeout)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s to finish indexing", indexName)
		}
		res, err := rdb.Do(ctx, "FT.INFO", indexName).Result()
		if err != nil {
			return fmt.Errorf("FT.INFO %s: %w", indexName, err)
		}
		indexing, ok := parseFTInfoIndexing(res)
		if !ok {
			// Older builds report indexing differently; one tick of grace then assume done.
			time.Sleep(500 * time.Millisecond)
			return nil
		}
		if indexing == 0 {
			return nil
		}
		log.Debug("waiting for indexing", "index", indexName, "indexing", indexing)
		time.Sleep(500 * time.Millisecond)
	}
}

// parseFTInfoIndexing walks the FT.INFO result (RESP3 map OR RESP2 flat
// array) for the `indexing` field. Returns (value, found).
func parseFTInfoIndexing(res interface{}) (int, bool) {
	switch v := res.(type) {
	case map[interface{}]interface{}:
		for k, val := range v {
			if s, ok := k.(string); ok && s == "indexing" {
				return asInt(val), true
			}
		}
	case map[string]interface{}:
		if val, ok := v["indexing"]; ok {
			return asInt(val), true
		}
	case []interface{}:
		for i := 0; i+1 < len(v); i++ {
			if s, ok := v[i].(string); ok && s == "indexing" {
				return asInt(v[i+1]), true
			}
		}
	}
	return 0, false
}

func asInt(v interface{}) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	case string:
		var n int
		_, _ = fmt.Sscan(x, &n)
		return n
	}
	return 0
}

// preloadProgressTicker prints products / events written every `iv` on
// stderr. Runs only while ctx is alive; the Preload caller cancels its
// context once both write phases complete. Uses ANSI cursor-up to
// overwrite the previous tick when stderr is a TTY; otherwise scrolls.
func preloadProgressTicker(ctx context.Context, iv time.Duration,
	productsWritten *atomic.Int64, productsTotal int64,
	eventsWritten *atomic.Int64, eventsTotal int64) {
	isTTY := stderrIsTTY()
	startedAt := time.Now()
	t := time.NewTicker(iv)
	defer t.Stop()

	var lastLines int32
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p := productsWritten.Load()
			e := eventsWritten.Load()
			elapsed := time.Since(startedAt).Round(time.Second)

			pp, ep := 0.0, 0.0
			if productsTotal > 0 {
				pp = 100 * float64(p) / float64(productsTotal)
			}
			if eventsTotal > 0 {
				ep = 100 * float64(e) / float64(eventsTotal)
			}

			var b strings.Builder
			if isTTY && atomic.LoadInt32(&lastLines) > 0 {
				fmt.Fprintf(&b, "\x1b[%dA\x1b[J", atomic.LoadInt32(&lastLines))
			}
			fmt.Fprintf(&b, "[preload %s] products: %d/%d (%.1f%%)   events: %d/%d (%.1f%%)\n",
				elapsed, p, productsTotal, pp, e, eventsTotal, ep)
			_, _ = os.Stderr.WriteString(b.String())
			atomic.StoreInt32(&lastLines, 1)
		}
	}
}
