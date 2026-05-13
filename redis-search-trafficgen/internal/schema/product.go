// Package schema issues the FT.CREATE statements for idx:product and idx:event.
// All commands go through rdb.Do() with raw args; the typed go-redis builders
// don't yet expose every option we need (SVS-VAMANA, COMPRESSION LVQ8, etc.).
package schema

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// ProductIndexOpts shapes idx:product based on capabilities probed at startup.
type ProductIndexOpts struct {
	Name    string
	Prefix  string
	DescDim int
	ImgDim  int
	FeatDim int
	UseSVS  bool // false → fall back to HNSW FLOAT16 for img_vec
	Flex    bool // true → use HASH-backed, Flex-compatible schema
}

// CreateProduct issues FT.CREATE for idx:product. Idempotent only when
// preceded by FT.DROPINDEX; caller's job to drop.
func CreateProduct(ctx context.Context, rdb redis.UniversalClient, o ProductIndexOpts) error {
	if o.Flex {
		return createProductFlex(ctx, rdb, o)
	}
	args := []interface{}{
		"FT.CREATE", o.Name,
		"ON", "JSON", "PREFIX", "1", o.Prefix,
		"SCHEMA",
		"$.sku", "AS", "sku", "TAG", "CASESENSITIVE", "SORTABLE",
		"$.brand", "AS", "brand", "TAG", "SORTABLE",
		"$.categories[*]", "AS", "categories", "TAG", "SEPARATOR", "|", "WITHSUFFIXTRIE",
		"$.title", "AS", "title", "TEXT", "WEIGHT", "5.0", "NOSTEM",
		"$.description", "AS", "description", "TEXT", "WEIGHT", "1.0", "PHONETIC", "dm:en", "WITHSUFFIXTRIE",
		"$.internal_notes", "AS", "notes", "TEXT", "NOINDEX",
		"$.price", "AS", "price", "NUMERIC", "SORTABLE",
		"$.rating", "AS", "rating", "NUMERIC", "SORTABLE",
		"$.in_stock", "AS", "in_stock", "TAG",
		"$.created_ts", "AS", "created_ts", "NUMERIC", "SORTABLE",
		"$.store_location", "AS", "store_loc", "GEO", "SORTABLE",
		"$.pickup_zone", "AS", "pickup_zone", "GEOSHAPE", "SPHERICAL",
		"$.desc_embedding", "AS", "desc_vec", "VECTOR", "HNSW", "10",
		"TYPE", "FLOAT32",
		"DIM", strconv.Itoa(o.DescDim),
		"DISTANCE_METRIC", "COSINE",
		"M", "16", "EF_CONSTRUCTION", "200",
		"$.feat_embedding", "AS", "feat_vec", "VECTOR", "FLAT", "6",
		"TYPE", "FLOAT32",
		"DIM", strconv.Itoa(o.FeatDim),
		"DISTANCE_METRIC", "L2",
	}

	// img_vec depends on capability probe outcome.
	if o.UseSVS {
		args = append(args,
			"$.img_embedding", "AS", "img_vec", "VECTOR", "SVS-VAMANA", "12",
			"TYPE", "FLOAT16",
			"DIM", strconv.Itoa(o.ImgDim),
			"DISTANCE_METRIC", "IP",
			"COMPRESSION", "LVQ8",
			"GRAPH_MAX_DEGREE", "64",
			"CONSTRUCTION_WINDOW_SIZE", "200",
		)
	} else {
		args = append(args,
			"$.img_embedding", "AS", "img_vec", "VECTOR", "HNSW", "10",
			"TYPE", "FLOAT16",
			"DIM", strconv.Itoa(o.ImgDim),
			"DISTANCE_METRIC", "IP",
			"M", "16", "EF_CONSTRUCTION", "200",
		)
	}

	if _, err := rdb.Do(ctx, args...).Result(); err != nil {
		// FT.CREATE failures on SVS-VAMANA compression flags should auto-fall back.
		if o.UseSVS && looksLikeSVSReject(err) {
			o.UseSVS = false
			return CreateProduct(ctx, rdb, o)
		}
		return fmt.Errorf("FT.CREATE %s: %w", o.Name, err)
	}
	return nil
}

// createProductFlex builds an FT.CREATE that satisfies every Search-on-Disk
// restriction probed against Redis 8.6.2 Flex:
//
//   - `ON HASH` only (JSON-backed indexes rejected).
//   - `SKIPINITIALSCAN` required.
//   - HNSW vectors must carry M / EF_CONSTRUCTION / EF_RUNTIME / RERANK TRUE.
//   - FLOAT16 vectors are rejected → FP32 only.
//   - FLAT and SVS-VAMANA vector types are rejected.
//   - NUMERIC, GEO, GEOSHAPE fields are rejected.
//   - NOINDEX is fine; SORTABLE is dropped because the matching ops can't use SORTBY anyway.
//
// The dropped fields (price, rating, created_ts, store_location, pickup_zone,
// internal_notes, feat_embedding) are still written to the hash but go
// unindexed.
func createProductFlex(ctx context.Context, rdb redis.UniversalClient, o ProductIndexOpts) error {
	args := []interface{}{
		"FT.CREATE", o.Name,
		"ON", "HASH", "PREFIX", "1", o.Prefix,
		"SKIPINITIALSCAN",
		"SCHEMA",
		"sku", "TAG", "CASESENSITIVE",
		"brand", "TAG",
		"categories", "TAG", "SEPARATOR", "|",
		"title", "TEXT", "WEIGHT", "5.0", "NOSTEM",
		"description", "TEXT",
		"in_stock", "TAG",
		"desc_vec", "VECTOR", "HNSW", "14",
		"TYPE", "FLOAT32",
		"DIM", strconv.Itoa(o.DescDim),
		"DISTANCE_METRIC", "COSINE",
		"M", "16", "EF_CONSTRUCTION", "200", "EF_RUNTIME", "10", "RERANK", "TRUE",
		"img_vec", "VECTOR", "HNSW", "14",
		"TYPE", "FLOAT32",
		"DIM", strconv.Itoa(o.ImgDim),
		"DISTANCE_METRIC", "IP",
		"M", "16", "EF_CONSTRUCTION", "200", "EF_RUNTIME", "10", "RERANK", "TRUE",
	}
	if _, err := rdb.Do(ctx, args...).Result(); err != nil {
		return fmt.Errorf("FT.CREATE %s (flex): %w", o.Name, err)
	}
	return nil
}

// DropProduct removes the product index. Returns nil if the index didn't
// exist (idempotent). Flex rejects `DD` (Search-on-Disk doesn't tie the
// index drop to a doc DEL pass) so we omit it there.
func DropProduct(ctx context.Context, rdb redis.UniversalClient, name string, flex bool) error {
	args := []interface{}{"FT.DROPINDEX", name}
	if !flex {
		args = append(args, "DD")
	}
	_, err := rdb.Do(ctx, args...).Result()
	if err != nil && !isUnknownIndex(err) {
		return fmt.Errorf("FT.DROPINDEX %s: %w", name, err)
	}
	return nil
}

func isUnknownIndex(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "unknown index name") ||
		strings.Contains(s, "no such index") ||
		strings.Contains(s, "index not found") ||
		strings.Contains(s, "search_index_not_found")
}

func looksLikeSVSReject(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "svs") ||
		strings.Contains(s, "vamana") ||
		strings.Contains(s, "lvq") ||
		strings.Contains(s, "compression")
}
