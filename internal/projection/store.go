package projection

import (
	"sync"
	"time"
)

type OrderProjection struct {
	OrderID           string    `json:"order_id"`
	UserID            string    `json:"user_id"`
	Side              string    `json:"side"`
	Status            string    `json:"status"`
	BaseAssetCode     string    `json:"base_asset_code"`
	QuoteAssetCode    string    `json:"quote_asset_code"`
	ReservedAssetCode string    `json:"reserved_asset_code"`
	ReservedAmount    string    `json:"reserved_amount"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type InMemoryStore struct {
	mu        sync.RWMutex
	orders    map[string]OrderProjection
	processed map[string]struct{}
	now       func() time.Time
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		orders:    make(map[string]OrderProjection),
		processed: make(map[string]struct{}),
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

func (store *InMemoryStore) Apply(event OrderEvent) (OrderProjection, bool, error) {
	if err := event.Validate(); err != nil {
		return OrderProjection{}, false, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if _, exists := store.processed[event.EventKey]; exists {
		order := store.orders[event.Payload.OrderID]
		return order, false, nil
	}

	timestamp := event.OccurredAt.UTC()
	if timestamp.IsZero() {
		timestamp = store.now()
	}

	var order OrderProjection
	switch event.EventType {
	case EventOrderCreated:
		if _, exists := store.orders[event.Payload.OrderID]; exists {
			return OrderProjection{}, false, ErrOrderExists
		}

		order = OrderProjection{
			OrderID:           event.Payload.OrderID,
			UserID:            event.Payload.UserID,
			Side:              event.Payload.Side,
			Status:            "open",
			BaseAssetCode:     event.Payload.BaseAssetCode,
			QuoteAssetCode:    event.Payload.QuoteAssetCode,
			ReservedAssetCode: event.Payload.ReservedAssetCode,
			ReservedAmount:    event.Payload.ReservedAmount,
			CreatedAt:         timestamp,
			UpdatedAt:         timestamp,
		}
	case EventOrderCanceled:
		var exists bool
		order, exists = store.orders[event.Payload.OrderID]
		if !exists {
			return OrderProjection{}, false, ErrOrderNotFound
		}
		order.Status = "canceled"
		order.UpdatedAt = timestamp
	default:
		return OrderProjection{}, false, ErrUnsupportedType
	}

	store.orders[order.OrderID] = order
	store.processed[event.EventKey] = struct{}{}

	return order, true, nil
}

func (store *InMemoryStore) Get(orderID string) (OrderProjection, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	order, exists := store.orders[orderID]
	return order, exists
}

func (store *InMemoryStore) ProcessedCount() int {
	store.mu.RLock()
	defer store.mu.RUnlock()

	return len(store.processed)
}
