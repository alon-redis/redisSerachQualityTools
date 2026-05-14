// Package metrics tracks per-op latency histograms, counters (success,
// errors by class, zero-result hits), and exposes thread-safe Snapshot.
package metrics

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

const (
	ErrClassClientTimeout = "client_timeout"
	ErrClassServerTimeout = "server_timeout"
	ErrClassDial          = "dial"
	ErrClassConn          = "conn"
	ErrClassQuerySyntax   = "query_syntax"
	ErrClassFeatureUnsupp = "feature_unsupported"
	ErrClassOther         = "other"
)

// MetricSet aggregates everything we report: per-op histograms and counters.
// HdrHistogram is NOT goroutine-safe so each op gets its own mutex.
type MetricSet struct {
	mu sync.RWMutex
	// op name -> *OpMetrics
	ops map[string]*OpMetrics

	maxValueMS         int
	significantFigures int

	// Global counters
	totalOps          uint64
	totalErrors       uint64
	totalZeroResults  uint64 // successful ops that returned an empty result set
}

type OpMetrics struct {
	mu sync.Mutex

	hist *hdrhistogram.Histogram

	success      uint64
	errorTotal   uint64
	errorByClass map[string]*uint64

	zeroResults uint64
	totalResults uint64
	queryCount   uint64
}

func New(maxValueMS, sigFigs int) *MetricSet {
	if maxValueMS <= 0 {
		maxValueMS = 60000
	}
	if sigFigs <= 0 {
		sigFigs = 3
	}
	return &MetricSet{
		ops:                make(map[string]*OpMetrics),
		maxValueMS:         maxValueMS,
		significantFigures: sigFigs,
	}
}

func (m *MetricSet) opFor(name string) *OpMetrics {
	m.mu.RLock()
	o, ok := m.ops[name]
	m.mu.RUnlock()
	if ok {
		return o
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if o, ok = m.ops[name]; ok {
		return o
	}
	o = &OpMetrics{
		hist:         hdrhistogram.New(1, int64(m.maxValueMS)*1000, m.significantFigures),
		errorByClass: make(map[string]*uint64),
	}
	m.ops[name] = o
	return o
}

// RecordSuccess records a latency sample on a successful op.
func (m *MetricSet) RecordSuccess(op string, lat time.Duration, resultCount int) {
	o := m.opFor(op)
	usec := lat.Microseconds()
	if usec < 1 {
		usec = 1
	}
	o.mu.Lock()
	_ = o.hist.RecordValue(usec)
	o.mu.Unlock()
	atomic.AddUint64(&o.success, 1)
	atomic.AddUint64(&o.queryCount, 1)
	if resultCount == 0 {
		atomic.AddUint64(&o.zeroResults, 1)
		atomic.AddUint64(&m.totalZeroResults, 1)
	}
	atomic.AddUint64(&o.totalResults, uint64(resultCount))
	atomic.AddUint64(&m.totalOps, 1)
}

// RecordError records a latency sample on a failed op + classifies the error.
func (m *MetricSet) RecordError(op, errClass string, lat time.Duration) {
	o := m.opFor(op)
	usec := lat.Microseconds()
	if usec < 1 {
		usec = 1
	}
	o.mu.Lock()
	_ = o.hist.RecordValue(usec)
	o.mu.Unlock()
	atomic.AddUint64(&o.errorTotal, 1)
	atomic.AddUint64(&o.queryCount, 1)

	o.mu.Lock()
	cls, ok := o.errorByClass[errClass]
	if !ok {
		var n uint64
		cls = &n
		o.errorByClass[errClass] = cls
	}
	o.mu.Unlock()
	atomic.AddUint64(cls, 1)
	atomic.AddUint64(&m.totalOps, 1)
	atomic.AddUint64(&m.totalErrors, 1)
}

// Snapshot returns an immutable report-friendly view, sorted by op name.
type Snapshot struct {
	TotalOps         uint64    `json:"total_ops"`
	TotalErrors      uint64    `json:"total_errors"`
	TotalZeroResults uint64    `json:"total_zero_results"`
	TotalZeroRate    float64   `json:"total_zero_rate"`
	Ops              []OpStats `json:"ops"`
}

type OpStats struct {
	Op            string            `json:"op"`
	Count         uint64            `json:"count"`
	Success       uint64            `json:"success"`
	Errors        uint64            `json:"errors"`
	ErrorByClass  map[string]uint64 `json:"error_by_class,omitempty"`
	ZeroResults   uint64            `json:"zero_results"`
	ZeroRate      float64           `json:"zero_rate"`
	P50MS         float64           `json:"p50_ms"`
	P95MS         float64           `json:"p95_ms"`
	P99MS         float64           `json:"p99_ms"`
	P999MS        float64           `json:"p999_ms"`
	MaxMS         float64           `json:"max_ms"`
	MeanResults   float64           `json:"mean_results"`
}

func (m *MetricSet) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	totalOps := atomic.LoadUint64(&m.totalOps)
	totalZero := atomic.LoadUint64(&m.totalZeroResults)
	var totalZeroRate float64
	if totalOps > 0 {
		totalZeroRate = float64(totalZero) / float64(totalOps)
	}
	out := Snapshot{
		TotalOps:         totalOps,
		TotalErrors:      atomic.LoadUint64(&m.totalErrors),
		TotalZeroResults: totalZero,
		TotalZeroRate:    totalZeroRate,
		Ops:              make([]OpStats, 0, len(m.ops)),
	}
	for name, o := range m.ops {
		o.mu.Lock()
		errByCls := make(map[string]uint64, len(o.errorByClass))
		for k, v := range o.errorByClass {
			errByCls[k] = atomic.LoadUint64(v)
		}
		count := atomic.LoadUint64(&o.queryCount)
		var meanRes float64
		if count > 0 {
			meanRes = float64(atomic.LoadUint64(&o.totalResults)) / float64(count)
		}
		zero := atomic.LoadUint64(&o.zeroResults)
		var zeroRate float64
		if count > 0 {
			zeroRate = float64(zero) / float64(count)
		}
		s := OpStats{
			Op:           name,
			Count:        count,
			Success:      atomic.LoadUint64(&o.success),
			Errors:       atomic.LoadUint64(&o.errorTotal),
			ErrorByClass: errByCls,
			ZeroResults:  zero,
			ZeroRate:     zeroRate,
			P50MS:        usecToMS(o.hist.ValueAtQuantile(50)),
			P95MS:        usecToMS(o.hist.ValueAtQuantile(95)),
			P99MS:        usecToMS(o.hist.ValueAtQuantile(99)),
			P999MS:       usecToMS(o.hist.ValueAtQuantile(99.9)),
			MaxMS:        usecToMS(o.hist.Max()),
			MeanResults:  meanRes,
		}
		o.mu.Unlock()
		out.Ops = append(out.Ops, s)
	}
	sort.Slice(out.Ops, func(i, j int) bool { return out.Ops[i].Op < out.Ops[j].Op })
	return out
}

func usecToMS(usec int64) float64 {
	return float64(usec) / 1000.0
}
