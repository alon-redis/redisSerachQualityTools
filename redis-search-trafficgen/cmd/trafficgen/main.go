// trafficgen is the CLI entry point for the Redis Search P0 traffic generator.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/alon-redis/redis-search-trafficgen/internal/assertx"
	"github.com/alon-redis/redis-search-trafficgen/internal/client"
	"github.com/alon-redis/redis-search-trafficgen/internal/config"
	"github.com/alon-redis/redis-search-trafficgen/internal/report"
	"github.com/alon-redis/redis-search-trafficgen/internal/runner"
	"github.com/alon-redis/redis-search-trafficgen/internal/schema"
)

var (
	flagConfig    string
	flagRedisAddr string
	flagSeed      uint64
	flagOutDir    string
	flagLogLevel  string
	flagFlex      bool
)

const trafficGenVersion = "0.1.0-mvp"

func main() {
	root := &cobra.Command{
		Use:   "trafficgen",
		Short: "Redis Search P0 traffic generator",
	}
	root.PersistentFlags().StringVar(&flagConfig, "config", "", "YAML scenario")
	root.PersistentFlags().StringVar(&flagRedisAddr, "redis-addr", "", "override redis.addrs[0]")
	root.PersistentFlags().Uint64Var(&flagSeed, "seed", 0, "override config seed")
	root.PersistentFlags().StringVar(&flagOutDir, "out-dir", "", "override metrics.out_dir")
	root.PersistentFlags().StringVar(&flagLogLevel, "log-level", "info", "debug|info|warn|error")
	root.PersistentFlags().BoolVar(&flagFlex, "flex", false, "force Flex (Search-on-Disk) schema + op set regardless of capability probe (same as redis.flex_mode: force)")

	root.AddCommand(cmdPreload(), cmdRun(), cmdFull(), cmdValidate(), cmdDrop(), cmdCapabilities(), cmdVersion())

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func loadCfg() (*config.Config, error) {
	if flagConfig == "" {
		return nil, errors.New("--config PATH is required")
	}
	cfg, err := config.Load(flagConfig)
	if err != nil {
		return nil, err
	}
	if flagRedisAddr != "" {
		cfg.Redis.Addrs = []string{flagRedisAddr}
	}
	if flagSeed != 0 {
		cfg.Seed = flagSeed
	}
	if flagOutDir != "" {
		cfg.Metrics.OutDir = flagOutDir
	}
	return cfg, nil
}

func makeLogger() *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(flagLogLevel) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

func newRootCtx() (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()
	return ctx, func() {
		signal.Stop(ch)
		cancel()
	}
}

func cmdValidate() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Parse and validate config; no Redis traffic",
		RunE: func(c *cobra.Command, _ []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			fmt.Printf("OK: %s seed=%d phases=%d products=%d events=%d\n",
				cfg.Name, cfg.Seed, len(cfg.Phases), cfg.Dataset.Products, cfg.Dataset.Events)
			return nil
		},
	}
}

func cmdCapabilities() *cobra.Command {
	return &cobra.Command{
		Use:   "capabilities",
		Short: "Probe the connected Redis: version, modules, supported vector types",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			ctx, stop := newRootCtx()
			defer stop()
			rdb, err := client.Connect(cfg)
			if err != nil {
				return err
			}
			defer rdb.Close()
			caps, err := client.Probe(ctx, rdb)
			if err != nil {
				return err
			}
			caps.IsFlex = client.ResolveFlex(caps.IsFlex, cfg.Redis.FlexMode, flagFlex)
			fmt.Printf("Redis %s   Search %s   JSON=%v   Flex=%v   SVS-VAMANA=%v   Hybrid=%v   Hybrid+DIALECT=%v   Dialect3=%v\n",
				caps.RedisVersion, caps.SearchVersion, caps.HasJSON, caps.IsFlex, caps.SVSVamana,
				caps.HybridSupported, caps.HybridAcceptsDialect, caps.Dialect3)
			return nil
		},
	}
}

func cmdDrop() *cobra.Command {
	var yes bool
	c := &cobra.Command{
		Use:   "drop",
		Short: "FT.DROPINDEX + DEL the prefix (DANGEROUS, requires --yes)",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			if !yes {
				return errors.New("--yes is required for destructive drop")
			}
			ctx, stop := newRootCtx()
			defer stop()
			rdb, err := client.Connect(cfg)
			if err != nil {
				return err
			}
			defer rdb.Close()
			caps, _ := client.Probe(ctx, rdb)
			flex := false
			if caps != nil {
				flex = client.ResolveFlex(caps.IsFlex, cfg.Redis.FlexMode, flagFlex)
			}
			if err := schema.DropProduct(ctx, rdb, cfg.Indexes.Product.Name, flex); err != nil {
				return err
			}
			if err := schema.DropEvent(ctx, rdb, cfg.Indexes.Event.Name, flex); err != nil {
				return err
			}
			fmt.Println("dropped")
			return nil
		},
	}
	c.Flags().BoolVar(&yes, "yes", false, "confirm destructive drop")
	return c
}

func cmdVersion() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print trafficgen version",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Printf("trafficgen %s\n", trafficGenVersion)
		},
	}
}

func cmdPreload() *cobra.Command {
	return &cobra.Command{
		Use:   "preload",
		Short: "Create indexes + write dataset",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			log := makeLogger()
			ctx, stop := newRootCtx()
			defer stop()
			return doPreload(ctx, cfg, log)
		},
	}
}

func cmdRun() *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Execute phases against an already-loaded dataset",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			log := makeLogger()
			ctx, stop := newRootCtx()
			defer stop()
			return doRun(ctx, cfg, log, false)
		},
	}
}

func cmdFull() *cobra.Command {
	return &cobra.Command{
		Use:   "full",
		Short: "preload + run in one shot",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := loadCfg()
			if err != nil {
				return err
			}
			log := makeLogger()
			ctx, stop := newRootCtx()
			defer stop()
			return doRun(ctx, cfg, log, true)
		},
	}
}

func doPreload(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	rdb, err := client.Connect(cfg)
	if err != nil {
		return err
	}
	defer rdb.Close()
	caps, err := client.Probe(ctx, rdb)
	if err != nil {
		return err
	}
	caps.IsFlex = client.ResolveFlex(caps.IsFlex, cfg.Redis.FlexMode, flagFlex)
	log.Info("capabilities probed",
		"redis", caps.RedisVersion, "search", caps.SearchVersion,
		"flex", caps.IsFlex, "flex_mode", cfg.Redis.FlexMode,
		"svs_vamana", caps.SVSVamana, "hybrid", caps.HybridSupported,
		"hybrid_dialect", caps.HybridAcceptsDialect)
	if _, err := runner.Preload(ctx, rdb, cfg, caps, log); err != nil {
		return err
	}
	log.Info("preload complete")
	return nil
}

func doRun(ctx context.Context, cfg *config.Config, log *slog.Logger, withPreload bool) error {
	rdb, err := client.Connect(cfg)
	if err != nil {
		return err
	}
	defer rdb.Close()

	caps, err := client.Probe(ctx, rdb)
	if err != nil {
		return err
	}
	caps.IsFlex = client.ResolveFlex(caps.IsFlex, cfg.Redis.FlexMode, flagFlex)
	log.Info("capabilities probed",
		"redis", caps.RedisVersion, "search", caps.SearchVersion,
		"flex", caps.IsFlex, "flex_mode", cfg.Redis.FlexMode,
		"svs_vamana", caps.SVSVamana, "hybrid", caps.HybridSupported,
		"hybrid_dialect", caps.HybridAcceptsDialect)

	corpus, err := runner.Preload(ctx, rdb, cfg, caps, log)
	if err != nil {
		if !withPreload {
			return err
		}
		return err
	}

	startedAt := time.Now()
	rn := runner.New(rdb, cfg, caps, corpus, log)
	runErr := rn.Run(ctx)

	exitCode := 0
	switch {
	case errors.Is(runErr, runner.ErrQuerySyntaxBug):
		exitCode = 3
	case errors.Is(runErr, context.Canceled):
		exitCode = 130
	case runErr != nil:
		exitCode = 1
	}

	// Errors with severity=error count toward exit 4.
	for _, c := range rn.Asserts.Snapshot() {
		if c.Severity == assertx.SeverityError && c.Failed > 0 && exitCode == 0 {
			exitCode = 4
		}
	}

	endedAt := time.Now()
	runID := fmt.Sprintf("%s-%s", cfg.Name, endedAt.Format("20060102-150405"))
	outDir := filepath.Join(cfg.Metrics.OutDir, cfg.Name)
	summary := report.BuildSummary(cfg.Name, runID, startedAt, endedAt, caps, rn, exitCode)
	if err := report.Write(outDir, summary); err != nil {
		log.Error("write report", "err", err)
	} else {
		log.Info("report written", "dir", outDir, "run_id", runID)
	}
	fmt.Println(report.RenderText(summary))
	if exitCode != 0 {
		os.Exit(exitCode)
	}
	return nil
}
