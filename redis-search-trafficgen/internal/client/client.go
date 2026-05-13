// Package client builds the Redis UniversalClient and runs capability probes.
package client

import (
	"crypto/tls"
	"fmt"
	"runtime"

	"github.com/redis/go-redis/v9"

	"github.com/alon-redis/redis-search-trafficgen/internal/config"
)

// Connect builds a UniversalClient from the YAML config. Cluster mode is
// triggered either by multiple addrs or by an explicit `cluster: true`.
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

	if cfg.Redis.Cluster && len(cfg.Redis.Addrs) == 1 {
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
