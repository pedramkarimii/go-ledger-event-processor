package consumer

import (
	"testing"

	"github.com/pedramkarimii/go-ledger-event-processor/internal/projection"
)

func TestDecodeOrderEvent(t *testing.T) {
	body := []byte(`{"event_key":"order.created:order-1","event_type":"order.created","occurred_at":"2026-06-27T12:00:00Z","payload":{"order_id":"order-1","user_id":"user-1","side":"buy","base_asset_code":"BTC","quote_asset_code":"USDT","reserved_asset_code":"USDT","reserved_amount":"20.000"}}`)

	event, err := DecodeOrderEvent(body)
	if err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if event.EventType != projection.EventOrderCreated {
		t.Fatalf("unexpected event type: %s", event.EventType)
	}
	if event.Payload.OrderID != "order-1" {
		t.Fatalf("unexpected order ID: %s", event.Payload.OrderID)
	}
}

func TestDecodeOrderEventRejectsInvalidPayload(t *testing.T) {
	_, err := DecodeOrderEvent([]byte(`{"event_type":"order.created"}`))
	if err == nil {
		t.Fatal("expected validation error")
	}
}
