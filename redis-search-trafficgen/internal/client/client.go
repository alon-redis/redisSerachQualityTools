// Package client builds the Redis UniversalClient and runs capability probes.
package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/alon-redis/redis-search-trafficgen/internal/config"
)

// Connect builds a UniversalClient from the YAML config. Cluster mode is
// activated by any of:
//   - explicit `redis.cluster: true` in YAML
//   - multiple entries in `redis.addrs` (UniversalClient's native behaviour)
//   - auto-detection: a single-shot CLUSTER INFO probe sees `cluster_enabled:1`
//
// Auto-detect catches Redis Enterprise endpoints that present a single
// hostname but resolve to multiple cluster nodes (the source of the
// `MOVED N <ip>:<port>` errors against a plain single-shard client).
func Connect(cfg *config.Config) (redis.UniversalClient, error) {
	poolSize := cfg.Redis.PoolSize
	if poolSize == 0 {
		poolSize = 4 * runtime.GOMAXPROCS(0)
	}
	if mp := cfg.MaxPhaseConcurrency(); mp > poolSize {
		poolSize = mp
	}

	var tlsCfg *tls.Config
	if cfg.Redis.TLS.Enabled {
		tlsCfg = &tls.Config{
			InsecureSkipVerify: cfg.Redis.TLS.InsecureSkipVerify,
		}
	}

	opts := &redis.UniversalOptions{
		Addrs:        cfg.Redis.Addrs,
		Username:     cfg.Redis.Username,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		Protocol:     cfg.Redis.Protocol,
		PoolSize:     poolSize,
		MinIdleConns: cfg.Redis.MinIdleConns,
		ReadTimeout:  cfg.Redis.ReadTimeout.D(),
		WriteTimeout: cfg.Redis.WriteTimeout.D(),
		DialTimeout:  cfg.Redis.DialTimeout.D(),
		TLSConfig:    tlsCfg,
		// go-redis v9.7 panics when typed FT.SEARCH / FT.AGGREGATE helpers
		// run under RESP3 unless we explicitly opt in to the unstable shape.
		UnstableResp3: true,
	}

	useCluster := cfg.Redis.Cluster
	if !useCluster && len(cfg.Redis.Addrs) == 1 {
		useCluster = probeIsCluster(opts)
	}

	if useCluster && len(cfg.Redis.Addrs) == 1 {
		// Force cluster mode against a single endpoint (Redis Enterprise style).
		// Note: ClusterOptions in go-redis v9.7 doesn't expose UnstableResp3
		// directly; the cluster path inherits whatever the underlying nodes
		// negotiate. Stick with UniversalClient unless cluster is forced.
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        opts.Addrs,
			Username:     opts.Username,
			Password:     opts.Password,
			Protocol:     opts.Protocol,
			PoolSize:     opts.PoolSize,
			MinIdleConns: opts.MinIdleConns,
			ReadTimeout:  opts.ReadTimeout,
			WriteTimeout: opts.WriteTimeout,
			DialTimeout:  opts.DialTimeout,
			TLSConfig:    tlsCfg,
		}), nil
	}

	uc := redis.NewUniversalClient(opts)
	if uc == nil {
		return nil, fmt.Errorf("failed to build UniversalClient")
	}
	return uc, nil
}

// probeIsCluster opens a one-shot single-node connection to the first addr
// and asks `CLUSTER INFO`. Any error or non-cluster reply collapses to
// false so the caller safely falls back to single-node mode.
func probeIsCluster(opts *redis.UniversalOptions) bool {
	probe := redis.NewClient(&redis.Options{
		Addr:        opts.Addrs[0],
		Username:    opts.Username,
		Password:    opts.Password,
		DB:          opts.DB,
		Protocol:    opts.Protocol,
		ReadTimeout: opts.ReadTimeout,
		DialTimeout: opts.DialTimeout,
		TLSConfig:   opts.TLSConfig,
	})
	defer probe.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err := probe.Do(ctx, "CLUSTER", "INFO").Result()
	if err != nil {
		return false
	}
	s, ok := res.(string)
	if !ok {
		return false
	}
	return looksLikeCluster(s)
}

// looksLikeCluster inspects a CLUSTER INFO payload and decides whether the
// node is part of a multi-shard cluster. Two signals are accepted because
// the canonical `cluster_enabled:1` line that standalone OSS Redis emits
// is *omitted entirely* by Redis Enterprise endpoints (even multi-node
// ones); Enterprise still reports `cluster_slots_assigned:16384` and
// `cluster_size:N>0`, which is what we lean on as a fallback.
func looksLikeCluster(clusterInfo string) bool {
	if strings.Contains(clusterInfo, "cluster_enabled:1") {
		return true
	}
	for _, line := range strings.Split(clusterInfo, "\n") {
		line = strings.TrimRight(line, "\r")
		v, ok := strings.CutPrefix(line, "cluster_slots_assigned:")
		if !ok {
			continue
		}
		if v = strings.TrimSpace(v); v != "" && v != "0" {
			return true
		}
	}
	return false
}
