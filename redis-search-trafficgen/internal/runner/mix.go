package runner

import (
	"fmt"
	"math/rand/v2"
	"sort"

	"github.com/alon-redis/redis-search-trafficgen/internal/config"
	"github.com/alon-redis/redis-search-trafficgen/internal/ops"
)

// Mix is a normalized weighted selector over Op names. Built once per phase
// from the merged (global mix + phase overrides) weights.
type Mix struct {
	names      []string
	cumWeights []int
	total      int
}

func BuildMix(global map[string]int, override map[string]int, registry map[string]ops.Op) (*Mix, error) {
	merged := make(map[string]int, len(global))
	for k, v := range global {
		merged[k] = v
	}
	for k, v := range override {
		merged[k] = v
	}

	names := make([]string, 0, len(merged))
	for n, w := range merged {
		if w <= 0 {
			continue
		}
		if _, ok := registry[n]; !ok {
			// Op named in YAML but not registered (e.g. hybrid disabled by caps).
			// Skip silently; a top-level warning is logged separately.
			continue
		}
		names = append(names, n)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no enabled ops in mix")
	}
	sort.Strings(names)

	m := &Mix{
		names:      names,
		cumWeights: make([]int, len(names)),
	}
	for i, n := range names {
		m.total += merged[n]
		m.cumWeights[i] = m.total
	}
	return m, nil
}

// Pick draws a weighted op name.
func (m *Mix) Pick(rng *rand.Rand) string {
	r := rng.IntN(m.total)
	idx := sort.SearchInts(m.cumWeights, r+1)
	if idx >= len(m.names) {
		idx = len(m.names) - 1
	}
	return m.names[idx]
}

// UnknownInMix reports YAML entries that don't match the registry — caller
// uses this for an at-startup warning so silent misconfig doesn't bite.
func UnknownInMix(cfg *config.Config, registry map[string]ops.Op) []string {
	seen := map[string]bool{}
	for k := range cfg.Mix {
		seen[k] = true
	}
	for _, ph := range cfg.Phases {
		for k := range ph.MixOverrides {
			seen[k] = true
		}
	}
	var unknown []string
	for k := range seen {
		if _, ok := registry[k]; !ok {
			unknown = append(unknown, k)
		}
	}
	sort.Strings(unknown)
	return unknown
}
