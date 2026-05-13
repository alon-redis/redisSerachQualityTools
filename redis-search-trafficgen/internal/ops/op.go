// Package ops implements the operation catalog. Each op declares which
// coverage features it exercises and executes against a worker's redis
// client + corpus.
package ops

import (
	"context"
	"errors"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/alon-redis/redis-search-trafficgen/internal/client"
	"github.com/alon-redis/redis-search-trafficgen/internal/config"
	"github.com/alon-redis/redis-search-trafficgen/internal/coverage"
	"github.com/alon-redis/redis-search-trafficgen/internal/datagen"
	"github.com/alon-redis/redis-search-trafficgen/internal/metrics"
)

// ExecResult is what each op returns; the worker hands it to the metrics
// recorder and (optionally) to the assertion sampler.
type ExecResult struct {
	Latency     time.Duration
	ResultCount int
	// Doc IDs (or SKUs) returned in order, populated by ops that participate
	// in sampled correctness assertions. Empty otherwise.
	TopIDs []string
	// AssertHint is op-specific data the assertion code may need (e.g. the
	// query vector for a KNN sample, the prefix string for prefix assertions).
	AssertHint map[string]interface{}
}

// Op is the polymorphic unit of work.
type Op interface {
	Name() string
	Features() []coverage.Feature
	Execute(ctx context.Context, w *WorkerCtx) (ExecResult, error)
}

// WorkerCtx is the read-only view a worker exposes to ops.
type WorkerCtx struct {
	Rdb    redis.UniversalClient
	RNG    *rand.Rand
	Cfg    *config.Config
	Corpus *datagen.Corpus
	Caps   *client.Capabilities
}

// ClassifyError buckets an error into one of the metrics.ErrClass* constants.
func ClassifyError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return metrics.ErrClassClientTimeout
	}
	if errors.Is(err, context.Canceled) {
		return metrics.ErrClassClientTimeout
	}
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "syntax error"),
		strings.Contains(s, "unknown argument"),
		strings.Contains(s, "unexpected"):
		return metrics.ErrClassQuerySyntax
	case strings.Contains(s, "i/o timeout"),
		strings.Contains(s, "timeout"):
		return metrics.ErrClassServerTimeout
	case strings.Contains(s, "dial"),
		strings.Contains(s, "no such host"):
		return metrics.ErrClassDial
	case strings.Contains(s, "eof"),
		strings.Contains(s, "broken pipe"),
		strings.Contains(s, "connection reset"):
		return metrics.ErrClassConn
	case strings.Contains(s, "unknown index type"),
		strings.Contains(s, "not supported"):
		return metrics.ErrClassFeatureUnsupp
	default:
		return metrics.ErrClassOther
	}
}

// Registry returns every op enabled in the MVP, keyed by name. On Flex
// (Search-on-Disk) the registry drops FT.AGGREGATE and FT.HYBRID — both are
// rejected by the Flex query path.
func Registry(caps *client.Capabilities) map[string]Op {
	r := map[string]Op{
		"ft_search_text":   &TextOp{},
		"ft_search_prefix": &PrefixOp{},
		"ft_search_knn":    &KNNOp{},
	}
	if caps == nil || !caps.IsFlex {
		r["ft_aggregate_facet"] = &AggregateFacetOp{}
	}
	if caps != nil && caps.HybridSupported && !caps.IsFlex {
		r["ft_hybrid_rrf"] = &HybridRRFOp{}
	}
	return r
}

// IsFlex pulls the IsFlex flag off a (possibly nil) capabilities pointer.
func IsFlex(caps *client.Capabilities) bool {
	return caps != nil && caps.IsFlex
}

// EscapeTagValue applies the painPoints-prescribed escaping for TAG values
// inside an FT.SEARCH/FT.AGGREGATE query string. Hyphens, commas, periods,
// and whitespace must be backslash-escaped under DIALECT 2+.
func EscapeTagValue(v string) string {
	var sb strings.Builder
	sb.Grow(len(v) + 4)
	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '-', ',', '.', ' ', '\t', ':', ';', '/', '\\', '(', ')', '{', '}', '|':
			sb.WriteByte('\\')
		}
		sb.WriteByte(c)
	}
	return sb.String()
}
