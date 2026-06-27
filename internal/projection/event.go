package projection

import (
	"errors"
	"strings"
	"time"
)

const (
	EventOrderCreated  = "order.created"
	EventOrderCanceled = "order.canceled"
)

var (
	ErrInvalidEvent    = errors.New("invalid order event")
	ErrOrderNotFound   = errors.New("order projection not found")
	ErrOrderExists     = errors.New("order projection already exists")
	ErrUnsupportedType = errors.New("unsupported order event type")
)

type OrderEvent struct {
	EventKey   string       `json:"event_key"`
	EventType  string       `json:"event_type"`
	OccurredAt time.Time    `json:"occurred_at"`
	Payload    OrderPayload `json:"payload"`
}

type OrderPayload struct {
	OrderID           string `json:"order_id"`
	UserID            string `json:"user_id"`
	Side              string `json:"side"`
	Status            string `json:"status"`
	BaseAssetCode     string `json:"base_asset_code"`
	QuoteAssetCode    string `json:"quote_asset_code"`
	ReservedAssetCode string `json:"reserved_asset_code"`
	ReservedAmount    string `json:"reserved_amount"`
}

func (event OrderEvent) Validate() error {
	if strings.TrimSpace(event.EventKey) == "" || strings.TrimSpace(event.Payload.OrderID) == "" {
		return ErrInvalidEvent
	}

	switch event.EventType {
	case EventOrderCreated:
		if strings.TrimSpace(event.Payload.UserID) == "" ||
			strings.TrimSpace(event.Payload.Side) == "" ||
			strings.TrimSpace(event.Payload.BaseAssetCode) == "" ||
			strings.TrimSpace(event.Payload.QuoteAssetCode) == "" ||
			strings.TrimSpace(event.Payload.ReservedAssetCode) == "" ||
			strings.TrimSpace(event.Payload.ReservedAmount) == "" {
			return ErrInvalidEvent
		}
	case EventOrderCanceled:
		return nil
	default:
		return ErrUnsupportedType
	}

	return nil
}
