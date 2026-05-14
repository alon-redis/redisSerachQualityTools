package datagen

import (
	"context"
	"fmt"
	"math"
)

// Event is the HASH document under event:<idx>. Field names match the
// idx:event schema (HSET-flat layout, all string/int values).
type Event struct {
	UserID     string
	SessionID  string
	ProductSKU string
	EventType  string
	QueryText  string
	TS         int64
	DwellMS    int64
	Country    string
	Device     string
}

type EventDoc struct {
	Key   string
	Event Event
}

var (
	eventTypes   = []string{"view", "view", "view", "view", "view", "view", "view", "add_to_cart", "add_to_cart", "search"}
	countryCodes = []string{"US", "GB", "DE", "FR", "IT", "ES", "NL", "SE", "JP", "AU", "BR", "CA", "IN", "IL", "ZA", "MX", "PL", "TR", "KR", "SG"}
	devices      = []string{"mobile", "mobile", "desktop", "tablet"}
)

// GenEventsStream generates `count` event docs deterministically and emits
// them on `out`, closing `out` when done (or when ctx is cancelled).
// `productCount` is the total number of products that generated events may
// reference via `product_sku` (typically startIdx + new-products-being-written
// when growing a dataset).
//
// Streaming variant — kept peak resident memory at O(channel buffer)
// regardless of `count`. Necessary for multi-million-event preloads.
func GenEventsStream(
	ctx context.Context,
	master uint64,
	prefix string,
	startIdx, count, productCount int,
	out chan<- EventDoc,
) {
	defer close(out)
	rng := RNG(master, StreamEvents)
	for i := 0; i < count; i++ {
		idx := startIdx + i
		userID := fmt.Sprintf("u%05d", rng.IntN(10000))
		sessionID := fmt.Sprintf("s%07d", rng.IntN(1000000))
		productIdx := rng.IntN(productCount)
		productSKU := fmt.Sprintf("SKU-%06d", productIdx)
		if productIdx == 0 {
			productSKU = "ANCHOR-0"
		}
		etype := eventTypes[rng.IntN(len(eventTypes))]
		var queryText string
		if etype == "search" {
			queryText = TitleWords[rng.IntN(len(TitleWords))]
		}
		ts := int64(1700000000) + int64(rng.IntN(2*365*24*3600))
		dwell := int64(math.Exp(7 + 1.0*rng.NormFloat64()))
		country := countryCodes[zipf(rng, len(countryCodes))]
		device := devices[rng.IntN(len(devices))]
		doc := EventDoc{
			Key: fmt.Sprintf("%s%07d", prefix, idx),
			Event: Event{
				UserID:     userID,
				SessionID:  sessionID,
				ProductSKU: productSKU,
				EventType:  etype,
				QueryText:  queryText,
				TS:         ts,
				DwellMS:    dwell,
				Country:    country,
				Device:     device,
			},
		}
		select {
		case <-ctx.Done():
			return
		case out <- doc:
		}
	}
}

// FlatHash projects an Event into the (field, value) slice HSET expects.
func (e Event) FlatHash() []interface{} {
	return []interface{}{
		"user_id", e.UserID,
		"session_id", e.SessionID,
		"product_sku", e.ProductSKU,
		"event_type", e.EventType,
		"query_text", e.QueryText,
		"ts", e.TS,
		"dwell_ms", e.DwellMS,
		"country", e.Country,
		"device", e.Device,
	}
}
