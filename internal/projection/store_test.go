package projection

import (
	"errors"
	"testing"
	"time"
)

func TestApplyOrderEventsIdempotently(t *testing.T) {
	store := NewInMemoryStore()
	createdAt := time.Date(2026, time.June, 27, 10, 0, 0, 0, time.UTC)

	created := OrderEvent{
		EventKey:   "order.created:order-1",
		EventType:  EventOrderCreated,
		OccurredAt: createdAt,
		Payload: OrderPayload{
			OrderID:           "order-1",
			UserID:            "user-1",
			Side:              "buy",
			Status:            "open",
			BaseAssetCode:     "BTC",
			QuoteAssetCode:    "USDT",
			ReservedAssetCode: "USDT",
			ReservedAmount:    "20.000",
		},
	}

	order, applied, err := store.Apply(created)
	if err != nil {
		t.Fatalf("apply created event: %v", err)
	}
	if !applied {
		t.Fatal("created event should be applied")
	}
	if order.Status != "open" || order.ReservedAmount != "20.000" {
		t.Fatalf("unexpected created projection: %#v", order)
	}

	duplicate, applied, err := store.Apply(created)
	if err != nil {
		t.Fatalf("apply duplicate event: %v", err)
	}
	if applied {
		t.Fatal("duplicate event should not be applied")
	}
	if duplicate.OrderID != order.OrderID || duplicate.Status != "open" {
		t.Fatalf("unexpected duplicate projection: %#v", duplicate)
	}

	canceled := OrderEvent{
		EventKey:   "order.canceled:order-1",
		EventType:  EventOrderCanceled,
		OccurredAt: createdAt.Add(time.Minute),
		Payload:    OrderPayload{OrderID: "order-1"},
	}

	order, applied, err = store.Apply(canceled)
	if err != nil {
		t.Fatalf("apply canceled event: %v", err)
	}
	if !applied || order.Status != "canceled" {
		t.Fatalf("unexpected canceled projection: %#v, applied=%t", order, applied)
	}

	if store.ProcessedCount() != 2 {
		t.Fatalf("expected two processed event keys, got %d", store.ProcessedCount())
	}
}

func TestApplyRejectsCancellationWithoutOrder(t *testing.T) {
	store := NewInMemoryStore()

	_, _, err := store.Apply(OrderEvent{
		EventKey:  "order.canceled:missing",
		EventType: EventOrderCanceled,
		Payload:   OrderPayload{OrderID: "missing"},
	})

	if !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("expected ErrOrderNotFound, got %v", err)
	}
}

func TestApplyRejectsIncompleteCreatedEvent(t *testing.T) {
	store := NewInMemoryStore()

	_, _, err := store.Apply(OrderEvent{
		EventKey:  "order.created:order-2",
		EventType: EventOrderCreated,
		Payload:   OrderPayload{OrderID: "order-2"},
	})

	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}
}
