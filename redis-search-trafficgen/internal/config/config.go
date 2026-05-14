// Package config loads and validates the scenario YAML.
package config

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Name       string           `yaml:"name"`
	Seed       uint64           `yaml:"seed"`
	Redis      RedisConfig      `yaml:"redis"`
	Dataset    DatasetConfig    `yaml:"dataset"`
	Indexes    IndexesConfig    `yaml:"indexes"`
	Vectors    VectorsConfig    `yaml:"vectors"`
	Phases     []PhaseConfig    `yaml:"phases"`
	Mix        map[string]int   `yaml:"mix"`
	Assertions AssertionsConfig `yaml:"assertions"`
	Coverage   CoverageConfig   `yaml:"coverage"`
	Metrics    MetricsConfig    `yaml:"metrics"`
	Logging    LoggingConfig    `yaml:"logging"`
}

// Duration is a yaml-decodable wrapper for time.Duration so "10s"-style
// strings in scenarios parse correctly. Callers convert via .D().
type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Tag == "!!int" || node.Tag == "!!float" {
		// Allow raw numbers as nanoseconds for backwards-compat.
		var n int64
		if err := node.Decode(&n); err != nil {
			return err
		}
		*d = Duration(time.Duration(n))
		return nil
	}
	var s string
	if err := node.Decode(&s); err != nil {
		return err
	}
	td, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(td)
	return nil
}

type RedisConfig struct {
	Addrs        []string  `yaml:"addrs"`
	Username     string    `yaml:"username"`
	Password     string    `yaml:"password"`
	DB           int       `yaml:"db"`
	Protocol     int       `yaml:"protocol"`
	PoolSize     int       `yaml:"pool_size"`
	MinIdleConns int       `yaml:"min_idle_conns"`
	ReadTimeout  Duration  `yaml:"read_timeout"`
	WriteTimeout Duration  `yaml:"write_timeout"`
	DialTimeout  Duration  `yaml:"dial_timeout"`
	TLS          TLSConfig `yaml:"tls"`
	Cluster      bool      `yaml:"cluster"`

	// FlexMode controls how the generator decides whether to use the
	// Search-on-Disk (Flex) compatible schema + op set.
	//
	//   "auto"    (default) — trust the capability probe. Auto-detects Flex
	//                          and switches schema, op registry, NOCONTENT,
	//                          and assertion gating accordingly.
	//   "force"             — always use the Flex-compatible schema, even
	//                          if the probe says the server isn't Flex.
	//                          Useful for testing the Flex code path or
	//                          for `validate` runs without Redis access.
	//   "disable"           — never use the Flex schema, even if the probe
	//                          says the server is Flex (FT.CREATE will
	//                          likely fail; surface that failure loudly).
	FlexMode string `yaml:"flex_mode"`
}

type TLSConfig struct {
	Enabled            bool   `yaml:"enabled"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
	CAFile             string `yaml:"ca_file"`
	CertFile           string `yaml:"cert_file"`
	KeyFile            string `yaml:"key_file"`
}

type DatasetConfig struct {
	Products    int  `yaml:"products"`
	Events      int  `yaml:"events"`
	Preload     bool `yaml:"preload"`
	DropIndexes bool `yaml:"drop_indexes"`
	FlushDB     bool `yaml:"flush_db"`
}

type IndexesConfig struct {
	Product IndexConfig `yaml:"product"`
	Event   IndexConfig `yaml:"event"`
}

type IndexConfig struct {
	Name   string `yaml:"name"`
	Prefix string `yaml:"prefix"`
}

type VectorsConfig struct {
	DescDim  int `yaml:"desc_dim"`
	ImgDim   int `yaml:"img_dim"`
	FeatDim  int `yaml:"feat_dim"`
	Clusters int `yaml:"clusters"`
}

type PhaseConfig struct {
	Name         string         `yaml:"name"`
	Duration     Duration       `yaml:"duration"`
	TargetQPS    int            `yaml:"target_qps"`
	Concurrency  int            `yaml:"concurrency"`
	OpTimeout    Duration       `yaml:"op_timeout"`
	MixOverrides map[string]int `yaml:"mix_overrides"`
}

type AssertionsConfig struct {
	BM25Descending          AssertConfig `yaml:"bm25_descending"`
	KNNRecallAt10           AssertConfig `yaml:"knn_recall_at_10"`
	HybridTop1InEitherLeg   AssertConfig `yaml:"hybrid_top1_in_either_leg"`
	PrefixMembership        AssertConfig `yaml:"prefix_membership"`
}

type AssertConfig struct {
	Enabled    bool    `yaml:"enabled"`
	SampleRate float64 `yaml:"sample_rate"`
	MinRecall  float64 `yaml:"min_recall"`
	Severity   string  `yaml:"severity"`
}

type CoverageConfig struct {
	MinFeaturesExercised int `yaml:"min_features_exercised"`
}

type MetricsConfig struct {
	OutDir                     string   `yaml:"out_dir"`
	HistogramSignificantDigits int      `yaml:"histogram_significant_digits"`
	HistogramMaxValueMS        int      `yaml:"histogram_max_value_ms"`
	// LiveInterval drives the in-flight stats printer. 0 disables it; a
	// TTY-attached stderr renders the same table as the end-of-run report
	// and overwrites it in place each tick; non-TTY stderr emits a one-line
	// compact summary per tick so log files don't bloat.
	LiveInterval Duration `yaml:"live_interval"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	c := defaultConfig()
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(c); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	applyEnvOverrides(c)
	if err := Validate(c); err != nil {
		return nil, err
	}
	return c, nil
}

func defaultConfig() *Config {
	return &Config{
		Redis: RedisConfig{
			DB:           0,
			Protocol:     3,
			ReadTimeout:  Duration(3 * time.Second),
			WriteTimeout: Duration(3 * time.Second),
			DialTimeout:  Duration(5 * time.Second),
			FlexMode:     "auto",
		},
		Dataset: DatasetConfig{
			Products:    100000,
			Events:      500000,
			Preload:     true,
			DropIndexes: true,
		},
		Indexes: IndexesConfig{
			Product: IndexConfig{Name: "idx:product", Prefix: "product:"},
			Event:   IndexConfig{Name: "idx:event", Prefix: "event:"},
		},
		Vectors: VectorsConfig{
			DescDim:  384,
			ImgDim:   512,
			FeatDim:  8,
			Clusters: 50,
		},
		Metrics: MetricsConfig{
			OutDir:                     "./out",
			HistogramSignificantDigits: 3,
			HistogramMaxValueMS:        60000,
			LiveInterval:               Duration(time.Second),
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "console",
		},
	}
}

func applyEnvOverrides(c *Config) {
	if v := os.Getenv("REDIS_PASSWORD"); v != "" && c.Redis.Password == "" {
		c.Redis.Password = v
	}
	if v := os.Getenv("REDIS_USERNAME"); v != "" && c.Redis.Username == "" {
		c.Redis.Username = v
	}
}

func Validate(c *Config) error {
	if c.Seed == 0 {
		return fmt.Errorf("seed must be non-zero")
	}
	if c.Name == "" {
		return fmt.Errorf("name is required")
	}
	if len(c.Redis.Addrs) == 0 {
		return fmt.Errorf("redis.addrs must have at least one entry")
	}
	if len(c.Phases) == 0 {
		return fmt.Errorf("phases must be non-empty")
	}
	mixSum := 0
	for _, w := range c.Mix {
		if w < 0 {
			return fmt.Errorf("mix weight must be non-negative")
		}
		mixSum += w
	}
	if mixSum == 0 {
		return fmt.Errorf("sum of mix weights must be > 0")
	}
	if c.Dataset.Products < c.Vectors.Clusters*2 {
		return fmt.Errorf("dataset.products (%d) must be >= 2 * vectors.clusters (%d)", c.Dataset.Products, c.Vectors.Clusters)
	}
	switch c.Redis.FlexMode {
	case "", "auto", "force", "disable":
		// ok
	default:
		return fmt.Errorf("redis.flex_mode must be one of: auto, force, disable (got %q)", c.Redis.FlexMode)
	}
	if c.Redis.FlexMode == "" {
		c.Redis.FlexMode = "auto"
	}
	for i, ph := range c.Phases {
		if ph.Name == "" {
			return fmt.Errorf("phases[%d].name is required", i)
		}
		if ph.Duration.D() <= 0 {
			return fmt.Errorf("phases[%d].duration must be > 0", i)
		}
		if ph.Concurrency <= 0 {
			return fmt.Errorf("phases[%d].concurrency must be > 0", i)
		}
	}
	return nil
}

// MaxPhaseConcurrency reports the largest concurrency across phases. Used to
// size the connection pool so workers never starve waiting on a connection.
func (c *Config) MaxPhaseConcurrency() int {
	max := 0
	for _, ph := range c.Phases {
		if ph.Concurrency > max {
			max = ph.Concurrency
		}
	}
	return max
}
