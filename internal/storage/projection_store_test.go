package storage

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/pedramkarimii/go-ledger-event-processor/internal/projection"
)

func TestProjectionStoreApplyIsIdempotent(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := OpenPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, "TRUNCATE TABLE processed_events, order_projections"); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}

	store := NewProjectionStore(pool)
	createdAt := time.Date(2026, time.June, 27, 12, 0, 0, 0, time.UTC)
	created := projection.OrderEvent{
		EventKey:   "order.created:order-pg-1",
		EventType:  projection.EventOrderCreated,
		OccurredAt: createdAt,
		Payload: projection.OrderPayload{
			OrderID:           "order-pg-1",
			UserID:            "user-pg-1",
			Side:              "buy",
			BaseAssetCode:     "BTC",
			QuoteAssetCode:    "USDT",
			ReservedAssetCode: "USDT",
			ReservedAmount:    "20.000",
		},
	}

	order, applied, err := store.Apply(ctx, created)
	if err != nil {
		t.Fatalf("apply created event: %v", err)
	}
	if !applied || order.Status != "open" {
		t.Fatalf("unexpected created result: %#v, applied=%t", order, applied)
	}

	_, applied, err = store.Apply(ctx, created)
	if err != nil {
		t.Fatalf("apply duplicate event: %v", err)
	}
	if applied {
		t.Fatal("duplicate event must not be applied")
	}

	canceled := projection.OrderEvent{
		EventKey:   "order.canceled:order-pg-1",
		EventType:  projection.EventOrderCanceled,
		OccurredAt: createdAt.Add(time.Minute),
		Payload:    projection.OrderPayload{OrderID: "order-pg-1"},
	}

	order, applied, err = store.Apply(ctx, canceled)
	if err != nil {
		t.Fatalf("apply canceled event: %v", err)
	}
	if !applied || order.Status != "canceled" {
		t.Fatalf("unexpected canceled result: %#v, applied=%t", order, applied)
	}

	stored, found, err := store.Get(ctx, "order-pg-1")
	if err != nil {
		t.Fatalf("get stored order: %v", err)
	}
	if !found || stored.Status != "canceled" {
		t.Fatalf("unexpected stored projection: %#v, found=%t", stored, found)
	}
}
