// Package assertx holds the sampled correctness checks. Named "assertx"
// (not "assert") to avoid colliding with the stdlib testing/assert idiom.
package assertx

import (
	"sync"
	"sync/atomic"
)

type Severity string

const (
	SeverityWarn  Severity = "warn"
	SeverityError Severity = "error"
)

type Result struct {
	Name     string
	Passed   bool
	Severity Severity
	Detail   string
}

// Registry aggregates per-assertion totals + failures across the whole run.
type Registry struct {
	mu      sync.Mutex
	totals  map[string]*Counter
}

type Counter struct {
	Sampled  uint64
	Passed   uint64
	Failed   uint64
	Severity Severity
	Examples []string // up to 5 failure detail messages
}

func NewRegistry() *Registry {
	return &Registry{totals: map[string]*Counter{}}
}

func (r *Registry) Record(res Result) {
	if res.Name == "" {
		return
	}
	r.mu.Lock()
	c, ok := r.totals[res.Name]
	if !ok {
		c = &Counter{Severity: res.Severity}
		r.totals[res.Name] = c
	}
	if c.Severity == "" {
		c.Severity = res.Severity
	}
	r.mu.Unlock()
	atomic.AddUint64(&c.Sampled, 1)
	if res.Passed {
		atomic.AddUint64(&c.Passed, 1)
		return
	}
	atomic.AddUint64(&c.Failed, 1)
	r.mu.Lock()
	if len(c.Examples) < 5 && res.Detail != "" {
		c.Examples = append(c.Examples, res.Detail)
	}
	r.mu.Unlock()
}

func (r *Registry) Snapshot() map[string]Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]Counter, len(r.totals))
	for k, v := range r.totals {
		out[k] = Counter{
			Sampled:  atomic.LoadUint64(&v.Sampled),
			Passed:   atomic.LoadUint64(&v.Passed),
			Failed:   atomic.LoadUint64(&v.Failed),
			Severity: v.Severity,
			Examples: append([]string{}, v.Examples...),
		}
	}
	return out
}
