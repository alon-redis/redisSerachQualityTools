package schema

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type EventIndexOpts struct {
	Name   string
	Prefix string
	Flex   bool // true → drop NUMERIC fields + SORTABLE; required for Search-on-Disk
}

func CreateEvent(ctx context.Context, rdb redis.UniversalClient, o EventIndexOpts) error {
	if o.Flex {
		return createEventFlex(ctx, rdb, o)
	}
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
		if isIndexExists(err) {
			return nil
		}
		return fmt.Errorf("FT.CREATE %s: %w", o.Name, err)
	}
	return nil
}

// createEventFlex drops every Flex-unsupported field/modifier: NUMERIC
// (ts, dwell_ms), SORTABLE, and adds SKIPINITIALSCAN. ts/dwell_ms are
// still written to the hash but go unindexed.
func createEventFlex(ctx context.Context, rdb redis.UniversalClient, o EventIndexOpts) error {
	args := []interface{}{
		"FT.CREATE", o.Name,
		"ON", "HASH", "PREFIX", "1", o.Prefix,
		"SKIPINITIALSCAN",
		"SCHEMA",
		"user_id", "TAG",
		"session_id", "TAG",
		"product_sku", "TAG", "CASESENSITIVE",
		"event_type", "TAG",
		"query_text", "TEXT", "NOSTEM",
		"country", "TAG",
		"device", "TAG",
	}
	if _, err := rdb.Do(ctx, args...).Result(); err != nil {
		if isIndexExists(err) {
			return nil
		}
		return fmt.Errorf("FT.CREATE %s (flex): %w", o.Name, err)
	}
	return nil
}

// DropEvent removes the event index. Flex rejects the `DD` keyword.
func DropEvent(ctx context.Context, rdb redis.UniversalClient, name string, flex bool) error {
	args := []interface{}{"FT.DROPINDEX", name}
	if !flex {
		args = append(args, "DD")
	}
	_, err := rdb.Do(ctx, args...).Result()
	if err != nil && !isUnknownIndex(err) {
		return fmt.Errorf("FT.DROPINDEX %s: %w", name, err)
	}
	return nil
}
