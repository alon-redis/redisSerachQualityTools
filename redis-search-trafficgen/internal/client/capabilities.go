package client

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Capabilities records what the connected Redis supports. The runner consults
// it to gate feature use (FT.HYBRID, SVS-VAMANA, etc.).
type Capabilities struct {
	RedisVersion         string
	SearchVersion        string
	HasJSON              bool
	HasSearch            bool
	SVSVamana            bool
	HybridSupported      bool
	HybridAcceptsDialect bool
	Dialect3             bool
	// IsFlex is true when the backing index storage is Redis Flex
	// (Search-on-Disk). Flex disallows JSON-backed indexes, FT.HYBRID,
	// FT.AGGREGATE, NUMERIC/GEO/GEOSHAPE/FLAT/SVS fields, and FP16
	// vectors; FT.SEARCH must use NOCONTENT or RETURN 0.
	IsFlex bool
}

const probeIndexSVS = "idx:_probe_svs"
const probeIndexHybrid = "idx:_probe_hybrid"

// FlexModeAuto / FlexModeForce / FlexModeDisable mirror the YAML
// redis.flex_mode values.
const (
	FlexModeAuto    = "auto"
	FlexModeForce   = "force"
	FlexModeDisable = "disable"
)

// ResolveFlex picks the final IsFlex value by combining the capability
// probe outcome with the configured flex_mode (and an optional `--flex`
// CLI force toggle). Auto trusts the probe; force always returns true;
// disable always returns false.
func ResolveFlex(probed bool, mode string, cliForce bool) bool {
	if cliForce {
		return true
	}
	switch mode {
	case FlexModeForce:
		return true
	case FlexModeDisable:
		return false
	default: // auto / unset
		return probed
	}
}

// Probe runs the full capability discovery sequence.
func Probe(ctx context.Context, c redis.UniversalClient) (*Capabilities, error) {
	caps := &Capabilities{}

	if v, err := infoField(ctx, c, "server", "redis_version"); err == nil {
		caps.RedisVersion = v
	} else {
		return nil, fmt.Errorf("INFO server failed (is Redis reachable?): %w", err)
	}

	modules, err := listModules(ctx, c)
	if err == nil {
		for name, ver := range modules {
			switch strings.ToLower(name) {
			case "search", "searchlight":
				caps.HasSearch = true
				caps.SearchVersion = ver
			case "rejson", "json":
				caps.HasJSON = true
			}
		}
	}
	// Redis Enterprise / managed services often return an empty MODULE LIST
	// (or restrict access to it). Fall back to direct command probes so we
	// don't refuse to run on perfectly healthy clusters.
	if !caps.HasSearch && commandExists(ctx, c, "FT._LIST") {
		caps.HasSearch = true
	}
	if !caps.HasJSON && jsonModuleLoaded(ctx, c) {
		caps.HasJSON = true
	}
	if !caps.HasSearch {
		return nil, errors.New("RediSearch is not available on the target Redis (MODULE LIST empty, FT._LIST failed)")
	}
	if !caps.HasJSON {
		return nil, errors.New("RedisJSON is not available on the target Redis; the product index requires JSON storage")
	}

	caps.Dialect3 = true // 8.4+ always supports DIALECT 3
	caps.IsFlex = probeFlex(ctx, c)
	if caps.IsFlex {
		// Flex disables hybrid + aggregate + SVS-VAMANA entirely. Skip the
		// fragile per-feature probes (they'd fail with confusing parse errors).
		return caps, nil
	}
	caps.SVSVamana = probeSVS(ctx, c)
	caps.HybridSupported, caps.HybridAcceptsDialect = probeHybrid(ctx, c)

	return caps, nil
}

const probeIndexFlex = "idx:_probe_flex"

// probeFlex returns true when the connected Redis is a Flex (Search-on-Disk)
// database. Detection: try creating a trivial JSON-backed index. Flex
// rejects with `SEARCH_FLEX_UNSUPPORTED_FT_CREATE_ARGUMENT` ("Only HASH is
// supported as index data type for Flex indexes").
func probeFlex(ctx context.Context, c redis.UniversalClient) bool {
	_, _ = c.Do(ctx, "FT.DROPINDEX", probeIndexFlex, "DD").Result()
	args := []interface{}{
		"FT.CREATE", probeIndexFlex,
		"ON", "JSON", "PREFIX", "1", "_probe_flex:",
		"SCHEMA",
		"$.x", "AS", "x", "TEXT",
	}
	_, err := c.Do(ctx, args...).Result()
	if err == nil {
		_, _ = c.Do(ctx, "FT.DROPINDEX", probeIndexFlex, "DD").Result()
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "flex") || strings.Contains(s, "only hash is supported")
}

func infoField(ctx context.Context, c redis.UniversalClient, section, field string) (string, error) {
	out, err := c.Info(ctx, section).Result()
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, field+":") {
			return strings.TrimPrefix(line, field+":"), nil
		}
	}
	return "", fmt.Errorf("field %q not found in INFO %s", field, section)
}

func listModules(ctx context.Context, c redis.UniversalClient) (map[string]string, error) {
	res, err := c.Do(ctx, "MODULE", "LIST").Result()
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	arr, ok := res.([]interface{})
	if !ok {
		return out, nil
	}
	for _, entry := range arr {
		fields, ok := entry.([]interface{})
		if !ok {
			continue
		}
		var name, ver string
		for i := 0; i+1 < len(fields); i += 2 {
			k, _ := fields[i].(string)
			switch k {
			case "name":
				name, _ = fields[i+1].(string)
			case "ver":
				switch v := fields[i+1].(type) {
				case int64:
					ver = fmt.Sprintf("%d", v)
				case string:
					ver = v
				}
			}
		}
		if name != "" {
			out[name] = ver
		}
	}
	return out, nil
}

// commandExists pings a no-arg command and reports whether the server
// recognizes it. We treat "unknown command" as the only signal of absence;
// any other error (auth, args, etc.) still implies the module is loaded.
func commandExists(ctx context.Context, c redis.UniversalClient, name string) bool {
	_, err := c.Do(ctx, name).Result()
	if err == nil {
		return true
	}
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "unknown command") {
		return false
	}
	return true
}

func jsonModuleLoaded(ctx context.Context, c redis.UniversalClient) bool {
	// JSON.GET on a missing key returns redis.Nil (success path); any
	// "unknown command" reply means the module isn't loaded.
	_, err := c.Do(ctx, "JSON.GET", "__trafficgen_probe__").Result()
	if err == nil || err == redis.Nil {
		return true
	}
	s := strings.ToLower(err.Error())
	return !strings.Contains(s, "unknown command")
}

func probeSVS(ctx context.Context, c redis.UniversalClient) bool {
	// Try creating an SVS-VAMANA index against an arbitrary prefix. If the
	// engine rejects the vector type we get an error and the capability is
	// false. Always drop the probe index afterward.
	_, _ = c.Do(ctx, "FT.DROPINDEX", probeIndexSVS, "DD").Result()
	args := []interface{}{
		"FT.CREATE", probeIndexSVS,
		"ON", "JSON", "PREFIX", "1", "_probe_svs:",
		"SCHEMA",
		"$.v", "AS", "v", "VECTOR", "SVS-VAMANA", "10",
		"TYPE", "FLOAT16", "DIM", "8", "DISTANCE_METRIC", "IP",
	}
	_, err := c.Do(ctx, args...).Result()
	if err != nil {
		return false
	}
	_, _ = c.Do(ctx, "FT.DROPINDEX", probeIndexSVS, "DD").Result()
	return true
}

func probeHybrid(ctx context.Context, c redis.UniversalClient) (supported, acceptsDialect bool) {
	_, _ = c.Do(ctx, "FT.DROPINDEX", probeIndexHybrid, "DD").Result()
	createArgs := []interface{}{
		"FT.CREATE", probeIndexHybrid,
		"ON", "HASH", "PREFIX", "1", "_probe_hybrid:",
		"SCHEMA",
		"title", "TEXT",
		"v", "VECTOR", "HNSW", "6",
		"TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "COSINE",
	}
	if _, err := c.Do(ctx, createArgs...).Result(); err != nil {
		return false, false
	}
	defer c.Do(ctx, "FT.DROPINDEX", probeIndexHybrid, "DD")

	qv := []byte{0, 0, 0x80, 0x3f, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} // [1,0,0,0] LE FP32

	// First probe: vanilla FT.HYBRID — does the command itself exist?
	// (WINDOW is rejected on some 8.6.x builds; we use the minimal canonical
	// RRF form that works across them.)
	plainArgs := []interface{}{
		"FT.HYBRID", probeIndexHybrid,
		"SEARCH", "*",
		"VSIM", "@v", "$qv",
		"COMBINE", "RRF", "2", "CONSTANT", "60",
		"PARAMS", "2", "qv", qv,
		"LIMIT", "0", "1",
	}
	if _, err := c.Do(ctx, plainArgs...).Result(); err != nil {
		return false, false
	}
	supported = true

	// Second probe: does it tolerate DIALECT? painPoints says no, but a future
	// build might change this. If it errors, we just omit DIALECT going forward.
	withDialect := append([]interface{}{}, plainArgs...)
	withDialect = append(withDialect, "DIALECT", "2")
	if _, err := c.Do(ctx, withDialect...).Result(); err == nil {
		acceptsDialect = true
	}
	return supported, acceptsDialect
}
