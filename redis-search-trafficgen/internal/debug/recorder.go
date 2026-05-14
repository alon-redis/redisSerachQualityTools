// Package debug captures a bounded sample of per-op detail for offline
// inspection: the last N errors and the N slowest requests. Enabled by
// the --debug-mode CLI flag; nil-safe so callers can always invoke the
// recorder without checking whether debug is on.
package debug

import (
	"container/heap"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	// MaxErrors / MaxSlow set the bounded sample sizes. Both are 25 per the
	// initial spec ("25 requests with errors / 25 with the highest latency").
	MaxErrors = 25
	MaxSlow   = 25
)

// Entry is one captured request/response sample.
type Entry struct {
	When     time.Time
	Op       string
	Latency  time.Duration
	Request  string
	Response string // op-specific summary (top ids, total count, etc.)
	Err      string // empty on success
}

// Recorder holds the bounded samples. All methods are goroutine-safe and
// nil-safe — methods on a nil *Recorder are no-ops, so callers can always
// invoke them without first checking whether debug mode is enabled.
type Recorder struct {
	mu sync.Mutex

	// Errors: ring buffer of the most recent MaxErrors error entries.
	errBuf    [MaxErrors]Entry
	errHead   int // next write position
	errCount  int // total error entries written (capped at MaxErrors for buffer use)

	// Slow: min-heap of size <= MaxSlow keyed by Latency, smallest at root.
	slow slowHeap
}

func NewRecorder() *Recorder {
	return &Recorder{}
}

// RecordError pushes an error entry into the ring buffer.
func (r *Recorder) RecordError(e Entry) {
	if r == nil || e.Err == "" {
		return
	}
	r.mu.Lock()
	r.errBuf[r.errHead] = e
	r.errHead = (r.errHead + 1) % MaxErrors
	if r.errCount < MaxErrors {
		r.errCount++
	}
	r.mu.Unlock()
}

// RecordSuccess offers a successful entry to the slow heap. Kept only if
// its latency exceeds the current slowest-of-the-fast threshold.
func (r *Recorder) RecordSuccess(e Entry) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.slow.Len() < MaxSlow {
		heap.Push(&r.slow, e)
		return
	}
	if e.Latency > r.slow[0].Latency {
		r.slow[0] = e
		heap.Fix(&r.slow, 0)
	}
}

// Flush writes both samples to `path` (truncating any prior content).
// Returns the number of entries written across both sections.
func (r *Recorder) Flush(path string) (int, error) {
	if r == nil {
		return 0, nil
	}
	r.mu.Lock()
	errs := make([]Entry, 0, r.errCount)
	// Walk the ring buffer in chronological order (oldest first).
	start := 0
	if r.errCount == MaxErrors {
		start = r.errHead
	}
	for i := 0; i < r.errCount; i++ {
		errs = append(errs, r.errBuf[(start+i)%MaxErrors])
	}
	slowCopy := append([]Entry{}, r.slow...)
	r.mu.Unlock()

	// Sort the slow entries descending by latency for human readability.
	// Pop-sorting a copy keeps the original heap untouched.
	sortedSlow := make([]Entry, 0, len(slowCopy))
	h := slowHeap(slowCopy)
	heap.Init(&h)
	for h.Len() > 0 {
		sortedSlow = append(sortedSlow, heap.Pop(&h).(Entry))
	}
	// Reverse: heap.Pop yielded ascending; we want descending.
	for i, j := 0, len(sortedSlow)-1; i < j; i, j = i+1, j-1 {
		sortedSlow[i], sortedSlow[j] = sortedSlow[j], sortedSlow[i]
	}

	f, err := os.Create(path)
	if err != nil {
		return 0, fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	fmt.Fprintf(f, "=== ERRORS (%d sampled; ring of last %d) ===\n\n", len(errs), MaxErrors)
	for i, e := range errs {
		writeEntry(f, i+1, e)
	}

	fmt.Fprintf(f, "\n=== TOP-%d SLOWEST (descending) ===\n\n", MaxSlow)
	for i, e := range sortedSlow {
		writeEntry(f, i+1, e)
	}
	return len(errs) + len(sortedSlow), nil
}

func writeEntry(f *os.File, idx int, e Entry) {
	fmt.Fprintf(f, "[%d] ts=%s  op=%s  lat=%s\n",
		idx, e.When.UTC().Format(time.RFC3339Nano), e.Op, e.Latency.Round(time.Microsecond))
	if e.Request != "" {
		fmt.Fprintf(f, "    request:  %s\n", e.Request)
	}
	if e.Err != "" {
		fmt.Fprintf(f, "    error:    %s\n", e.Err)
	}
	if e.Response != "" {
		fmt.Fprintf(f, "    response: %s\n", e.Response)
	}
	fmt.Fprintln(f)
}

// slowHeap is a min-heap of Entry keyed by Latency.
type slowHeap []Entry

func (h slowHeap) Len() int            { return len(h) }
func (h slowHeap) Less(i, j int) bool  { return h[i].Latency < h[j].Latency }
func (h slowHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *slowHeap) Push(x interface{}) { *h = append(*h, x.(Entry)) }
func (h *slowHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
