// Package report writes end-of-run summaries in text and JSON form.
package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/alon-redis/redis-search-trafficgen/internal/assertx"
	"github.com/alon-redis/redis-search-trafficgen/internal/client"
	"github.com/alon-redis/redis-search-trafficgen/internal/coverage"
	"github.com/alon-redis/redis-search-trafficgen/internal/metrics"
	"github.com/alon-redis/redis-search-trafficgen/internal/runner"
)

// Summary is the canonical end-of-run document. Written to disk as JSON
// + rendered as text.
type Summary struct {
	RunID        string                   `json:"run_id"`
	Scenario     string                   `json:"scenario"`
	StartedAt    time.Time                `json:"started_at"`
	EndedAt      time.Time                `json:"ended_at"`
	DurationSec  float64                  `json:"duration_sec"`
	Capabilities client.Capabilities      `json:"capabilities"`
	Metrics      metrics.Snapshot         `json:"metrics"`
	Coverage     []coverage.FeatureCount  `json:"coverage"`
	Missing      []string                 `json:"missing_features"`
	ZeroRate     []ZeroRateWarning        `json:"zero_rate_warnings,omitempty"`
	Assertions   map[string]assertx.Counter `json:"assertions"`
	InfoStats    runner.InfoStatsView     `json:"info_stats"`
	ExitCode     int                      `json:"exit_code"`
}

type ZeroRateWarning struct {
	Op       string  `json:"op"`
	ZeroRate float64 `json:"zero_rate"`
	Count    uint64  `json:"count"`
}

// BuildSummary stitches a Summary together from the live trackers.
func BuildSummary(scenario, runID string, startedAt, endedAt time.Time, caps *client.Capabilities, r *runner.Runner, exitCode int) Summary {
	mSnap := r.Metrics.Snapshot()
	covSnap := r.Coverage.Snapshot()
	missing := []string{}
	for _, fc := range covSnap {
		if fc.Count == 0 {
			missing = append(missing, string(fc.Feature))
		}
	}
	zw := []ZeroRateWarning{}
	for _, op := range mSnap.Ops {
		if op.ZeroRate > 0.5 && op.Count >= 20 {
			zw = append(zw, ZeroRateWarning{Op: op.Op, ZeroRate: op.ZeroRate, Count: op.Count})
		}
	}
	sort.Slice(zw, func(i, j int) bool { return zw[i].Op < zw[j].Op })

	capsCopy := client.Capabilities{}
	if caps != nil {
		capsCopy = *caps
	}
	return Summary{
		RunID:        runID,
		Scenario:     scenario,
		StartedAt:    startedAt,
		EndedAt:      endedAt,
		DurationSec:  endedAt.Sub(startedAt).Seconds(),
		Capabilities: capsCopy,
		Metrics:      mSnap,
		Coverage:     covSnap,
		Missing:      missing,
		ZeroRate:     zw,
		Assertions:   r.Asserts.Snapshot(),
		InfoStats:    r.InfoStatsSnapshot(),
		ExitCode:     exitCode,
	}
}

// Write emits both <run-id>.summary.json and <run-id>.txt under outDir.
func Write(outDir string, s Summary) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}
	jpath := filepath.Join(outDir, s.RunID+".summary.json")
	tpath := filepath.Join(outDir, s.RunID+".txt")

	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(jpath, b, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(tpath, []byte(RenderText(s)), 0o644); err != nil {
		return err
	}
	return nil
}

// RenderText produces the human-readable summary printed at end-of-run and
// written to <run-id>.txt.
func RenderText(s Summary) string {
	var b strings.Builder
	fmt.Fprintf(&b, "=== trafficgen run %s (scenario: %s) ===\n", s.RunID, s.Scenario)
	fmt.Fprintf(&b, "Duration: %.2fs   Total ops: %d   Errors: %d\n",
		s.DurationSec, s.Metrics.TotalOps, s.Metrics.TotalErrors)
	fmt.Fprintf(&b, "Redis: %s   Search: %s   SVS-VAMANA: %v   Hybrid: %v   Hybrid+DIALECT: %v\n",
		s.Capabilities.RedisVersion, s.Capabilities.SearchVersion,
		s.Capabilities.SVSVamana, s.Capabilities.HybridSupported, s.Capabilities.HybridAcceptsDialect)

	fmt.Fprintf(&b, "\n-- Per-op latency (ms) --\n")
	fmt.Fprintf(&b, "%-22s %8s %8s %8s %8s %8s %8s %10s\n",
		"op", "count", "errs", "p50", "p95", "p99", "p99.9", "zero_rate")
	for _, op := range s.Metrics.Ops {
		fmt.Fprintf(&b, "%-22s %8d %8d %8.2f %8.2f %8.2f %8.2f %10.3f\n",
			op.Op, op.Count, op.Errors,
			op.P50MS, op.P95MS, op.P99MS, op.P999MS, op.ZeroRate)
	}

	if len(s.ZeroRate) > 0 {
		fmt.Fprintf(&b, "\n-- Zero-result warnings (>50%%) --\n")
		for _, z := range s.ZeroRate {
			fmt.Fprintf(&b, "%-22s zero_rate=%.2f count=%d\n", z.Op, z.ZeroRate, z.Count)
		}
	}

	fmt.Fprintf(&b, "\n-- Coverage (%d features exercised) --\n", countExercised(s.Coverage))
	for _, fc := range s.Coverage {
		fmt.Fprintf(&b, "  %-30s %d\n", fc.Feature, fc.Count)
	}
	if len(s.Missing) > 0 {
		fmt.Fprintf(&b, "Missing (zero-count): %s\n", strings.Join(s.Missing, ", "))
	}

	if len(s.Assertions) > 0 {
		fmt.Fprintf(&b, "\n-- Assertions --\n")
		names := make([]string, 0, len(s.Assertions))
		for k := range s.Assertions {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, n := range names {
			c := s.Assertions[n]
			fmt.Fprintf(&b, "  %-30s sampled=%d passed=%d failed=%d severity=%s\n",
				n, c.Sampled, c.Passed, c.Failed, c.Severity)
			for _, e := range c.Examples {
				fmt.Fprintf(&b, "    e.g. %s\n", e)
			}
		}
	}

	fmt.Fprintf(&b, "\n-- Background --\n")
	fmt.Fprintf(&b, "  FT.INFO polls: %d (errors: %d)  num_docs: %d  indexing: %d\n",
		s.InfoStats.Polls, s.InfoStats.Errors, s.InfoStats.LastNumDocs, s.InfoStats.LastIndexing)
	fmt.Fprintf(&b, "  Anchor verifies: %d (failures: %d)\n",
		s.InfoStats.AnchorVerifies, s.InfoStats.AnchorFailures)
	fmt.Fprintf(&b, "\nExit code: %d\n", s.ExitCode)
	return b.String()
}

func countExercised(cov []coverage.FeatureCount) int {
	n := 0
	for _, fc := range cov {
		if fc.Count > 0 {
			n++
		}
	}
	return n
}
