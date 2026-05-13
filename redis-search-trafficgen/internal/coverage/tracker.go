package coverage

import (
	"sort"
	"sync"
	"sync/atomic"
)

// Tracker is a thread-safe counter map keyed by Feature. Workers call Mark
// after every op execution; the reporter reads Snapshot at end-of-run.
type Tracker struct {
	mu       sync.RWMutex
	counters map[Feature]*uint64
}

func NewTracker() *Tracker {
	t := &Tracker{counters: make(map[Feature]*uint64, len(AllFeatures()))}
	for _, f := range AllFeatures() {
		var c uint64
		t.counters[f] = &c
	}
	return t
}

// Mark increments the counter for a feature. Returns silently if the feature
// is unknown; the reporter only displays AllFeatures().
func (t *Tracker) Mark(f Feature) {
	t.mu.RLock()
	c, ok := t.counters[f]
	t.mu.RUnlock()
	if !ok {
		// Late-registered feature; take the write lock to add it.
		t.mu.Lock()
		if c, ok = t.counters[f]; !ok {
			var n uint64
			c = &n
			t.counters[f] = c
		}
		t.mu.Unlock()
	}
	atomic.AddUint64(c, 1)
}

func (t *Tracker) MarkAll(fs []Feature) {
	for _, f := range fs {
		t.Mark(f)
	}
}

// Snapshot returns feature counts sorted by name.
func (t *Tracker) Snapshot() []FeatureCount {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]FeatureCount, 0, len(t.counters))
	for f, c := range t.counters {
		out = append(out, FeatureCount{Feature: f, Count: atomic.LoadUint64(c)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Feature < out[j].Feature })
	return out
}

// CountExercised returns how many features have non-zero counts.
func (t *Tracker) CountExercised() int {
	n := 0
	for _, fc := range t.Snapshot() {
		if fc.Count > 0 {
			n++
		}
	}
	return n
}

type FeatureCount struct {
	Feature Feature
	Count   uint64
}
