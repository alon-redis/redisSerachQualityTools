package schema

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type EventIndexOpts struct {
	Name   string
	Prefix string
}

func CreateEvent(ctx context.Context, rdb redis.UniversalClient, o EventIndexOpts) error {
	args := []interface{}{
		"FT.CREATE", o.Name,
		"ON", "HASH", "PREFIX", "1", o.Prefix,
		"SCHEMA",
		"user_id", "TAG",
		"session_id", "TAG",
		"product_sku", "TAG", "CASESENSITIVE",
		"event_type", "TAG",
		"query_text", "TEXT", "NOSTEM",
		"ts", "NUMERIC", "SORTABLE",
		"dwell_ms", "NUMERIC", "SORTABLE",
		"country", "TAG", "SORTABLE",
		"device", "TAG",
	}
	if _, err := rdb.Do(ctx, args...).Result(); err != nil {
		return fmt.Errorf("FT.CREATE %s: %w", o.Name, err)
	}
	return nil
}

func DropEvent(ctx context.Context, rdb redis.UniversalClient, name string) error {
	_, err := rdb.Do(ctx, "FT.DROPINDEX", name, "DD").Result()
	if err != nil && !isUnknownIndex(err) {
		return fmt.Errorf("FT.DROPINDEX %s: %w", name, err)
	}
	return nil
}
