package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/alon-redis/redis-search-trafficgen/internal/client"
	"github.com/alon-redis/redis-search-trafficgen/internal/config"
	"github.com/alon-redis/redis-search-trafficgen/internal/datagen"
	"github.com/alon-redis/redis-search-trafficgen/internal/schema"
)

const preloadPipelineBatch = 500

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

	corpus := datagen.BuildCorpus(
		cfg.Seed,
		cfg.Vectors.DescDim, cfg.Vectors.ImgDim, cfg.Vectors.FeatDim, cfg.Vectors.Clusters,
		2000, 200, 10000, 1000, 200,
	)

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

	products := datagen.GenProducts(
		cfg.Seed, cfg.Indexes.Product.Prefix, cfg.Dataset.Products,
		corpus.DescCentroids, corpus.ImgCentroids, corpus.FeatCentroids,
	)
	if err := writeProductsConcurrent(ctx, rdb, products, log, flex); err != nil {
		return nil, fmt.Errorf("writing products: %w", err)
	}
	log.Info("wrote products", "count", len(products), "flex", flex)

	events := datagen.GenEvents(cfg.Seed, cfg.Indexes.Event.Prefix, cfg.Dataset.Events, cfg.Dataset.Products)
	if err := writeEventsConcurrent(ctx, rdb, events, log); err != nil {
		return nil, fmt.Errorf("writing events: %w", err)
	}
	log.Info("wrote events", "count", len(events))

	if err := waitForIndexing(ctx, rdb, cfg.Indexes.Product.Name, 10*time.Minute, log); err != nil {
		return nil, err
	}
	if err := waitForIndexing(ctx, rdb, cfg.Indexes.Event.Name, 10*time.Minute, log); err != nil {
		return nil, err
	}

	return corpus, nil
}

func writeProductsConcurrent(ctx context.Context, rdb redis.UniversalClient, docs []datagen.ProductDoc, log *slog.Logger, flex bool) error {
	const workers = 8
	jobs := make(chan []datagen.ProductDoc, workers*2)
	errs := make(chan error, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
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
			}
		}()
	}

	for i := 0; i < len(docs); i += preloadPipelineBatch {
		end := i + preloadPipelineBatch
		if end > len(docs) {
			end = len(docs)
		}
		jobs <- docs[i:end]
	}
	close(jobs)
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func writeEventsConcurrent(ctx context.Context, rdb redis.UniversalClient, docs []datagen.EventDoc, log *slog.Logger) error {
	const workers = 8
	jobs := make(chan []datagen.EventDoc, workers*2)
	errs := make(chan error, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				pipe := rdb.Pipeline()
				for _, d := range batch {
					pipe.HSet(ctx, d.Key, d.Event.FlatHash()...)
				}
				if _, err := pipe.Exec(ctx); err != nil {
					log.Warn("event pipeline exec returned error", "err", err)
				}
			}
		}()
	}

	for i := 0; i < len(docs); i += preloadPipelineBatch {
		end := i + preloadPipelineBatch
		if end > len(docs) {
			end = len(docs)
		}
		jobs <- docs[i:end]
	}
	close(jobs)
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
