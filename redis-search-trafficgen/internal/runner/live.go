package runner

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// runLiveTicker drives the in-flight stats printer. Stops when ctx is done.
// Cadence is the interval parameter; 0 (= disabled) is gated by the caller.
func (r *Runner) runLiveTicker(ctx context.Context, interval time.Duration) {
	isTTY := stderrIsTTY()
	startedAt := time.Now()
	t := time.NewTicker(interval)
	defer t.Stop()

	var lastTableLines int32
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if isTTY {
				n := r.printLiveTTY(startedAt, int(atomic.LoadInt32(&lastTableLines)))
				atomic.StoreInt32(&lastTableLines, int32(n))
			} else {
				r.printLivePlain(startedAt)
			}
		}
	}
}

// printLiveTTY redraws the full per-op table in place each tick using ANSI
// cursor-up + erase-to-end. Returns the line count it wrote so the next
// tick can rewind exactly that far.
func (r *Runner) printLiveTTY(startedAt time.Time, lastLines int) int {
	snap := r.Metrics.Snapshot()
	info := r.InfoStatsSnapshot()
	elapsed := time.Since(startedAt).Round(time.Second)

	var b strings.Builder
	if lastLines > 0 {
		// Move cursor up `lastLines` lines, then erase from cursor to end of screen.
		fmt.Fprintf(&b, "\x1b[%dA\x1b[J", lastLines)
	}
	throughput := 0.0
	if s := elapsed.Seconds(); s > 0 {
		throughput = float64(snap.TotalOps) / s
	}
	fmt.Fprintf(&b, "[live %s] Total ops: %d (%.1f/s)   Errors: %d   num_docs: %d   Anchor fails: %d\n",
		elapsed, snap.TotalOps, throughput, snap.TotalErrors, info.LastNumDocs, info.AnchorFailures)
	fmt.Fprintf(&b, "%-22s %8s %8s %8s %8s %8s %8s %10s\n",
		"op", "count", "errs", "p50", "p95", "p99", "p99.9", "zero_rate")
	lines := 2
	for _, op := range snap.Ops {
		fmt.Fprintf(&b, "%-22s %8d %8d %8.2f %8.2f %8.2f %8.2f %10.3f\n",
			op.Op, op.Count, op.Errors, op.P50MS, op.P95MS, op.P99MS, op.P999MS, op.ZeroRate)
		lines++
	}
	_, _ = os.Stderr.WriteString(b.String())
	return lines
}

// printLivePlain emits a single-line compact stat per tick for non-TTY
// stderr (piped to a file / log aggregator). Each tick is its own line so
// it's easy to grep/tail.
func (r *Runner) printLivePlain(startedAt time.Time) {
	snap := r.Metrics.Snapshot()
	info := r.InfoStatsSnapshot()
	elapsed := time.Since(startedAt).Round(time.Second)
	throughput := 0.0
	if s := elapsed.Seconds(); s > 0 {
		throughput = float64(snap.TotalOps) / s
	}
	var b strings.Builder
	fmt.Fprintf(&b, "[live %s] ops=%d (%.1f/s) errs=%d num_docs=%d anchor_fails=%d",
		elapsed, snap.TotalOps, throughput, snap.TotalErrors, info.LastNumDocs, info.AnchorFailures)
	for _, op := range snap.Ops {
		fmt.Fprintf(&b, "  %s(c=%d,p50=%.0f,p99=%.0f,zr=%.2f)",
			op.Op, op.Count, op.P50MS, op.P99MS, op.ZeroRate)
	}
	b.WriteByte('\n')
	_, _ = os.Stderr.WriteString(b.String())
}

// stderrIsTTY returns true if stderr is attached to a character device —
// the cheap "is this an interactive terminal?" heuristic that works on
// Unix without pulling in golang.org/x/term.
func stderrIsTTY() bool {
	fi, err := os.Stderr.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
