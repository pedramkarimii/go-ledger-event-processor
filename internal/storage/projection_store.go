package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/projection"
)

type ProjectionStore struct {
	pool *pgxpool.Pool
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func NewProjectionStore(pool *pgxpool.Pool) *ProjectionStore {
	return &ProjectionStore{pool: pool}
}

func (store *ProjectionStore) Apply(ctx context.Context, event projection.OrderEvent) (projection.OrderProjection, bool, error) {
	if err := event.Validate(); err != nil {
		return projection.OrderProjection{}, false, err
	}

	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return projection.OrderProjection{}, false, fmt.Errorf("begin projection transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	inserted, err := recordEvent(ctx, tx, event)
	if err != nil {
		return projection.OrderProjection{}, false, err
	}

	if !inserted {
		order, found, err := getOrder(ctx, tx, event.Payload.OrderID)
		if err != nil {
			return projection.OrderProjection{}, false, err
		}
		if !found {
			return projection.OrderProjection{}, false, fmt.Errorf("load already processed order: %w", projection.ErrOrderNotFound)
		}
		if err := tx.Commit(ctx); err != nil {
			return projection.OrderProjection{}, false, fmt.Errorf("commit duplicate event transaction: %w", err)
		}
		return order, false, nil
	}

	timestamp := event.OccurredAt.UTC()
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}

	var order projection.OrderProjection
	switch event.EventType {
	case projection.EventOrderCreated:
		order, err = insertOrder(ctx, tx, event.Payload, timestamp)
	case projection.EventOrderCanceled:
		order, err = cancelOrder(ctx, tx, event.Payload.OrderID, timestamp)
	default:
		return projection.OrderProjection{}, false, projection.ErrUnsupportedType
	}
	if err != nil {
		return projection.OrderProjection{}, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return projection.OrderProjection{}, false, fmt.Errorf("commit projection transaction: %w", err)
	}

	return order, true, nil
}

func (store *ProjectionStore) Get(ctx context.Context, orderID string) (projection.OrderProjection, bool, error) {
	if strings.TrimSpace(orderID) == "" {
		return projection.OrderProjection{}, false, nil
	}
	return getOrder(ctx, store.pool, orderID)
}

func recordEvent(ctx context.Context, tx pgx.Tx, event projection.OrderEvent) (bool, error) {
	const query = `
INSERT INTO processed_events (event_key, event_type, order_id)
VALUES ($1, $2, $3)
ON CONFLICT (event_key) DO NOTHING
RETURNING event_key`

	var eventKey string
	err := tx.QueryRow(ctx, query, event.EventKey, event.EventType, event.Payload.OrderID).Scan(&eventKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("record processed event: %w", err)
	}
	return true, nil
}

func insertOrder(ctx context.Context, tx pgx.Tx, payload projection.OrderPayload, timestamp time.Time) (projection.OrderProjection, error) {
	const query = `
INSERT INTO order_projections (
    order_id, user_id, side, status, base_asset_code, quote_asset_code,
    reserved_asset_code, reserved_amount, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (order_id) DO NOTHING
RETURNING order_id, user_id, side, status, base_asset_code, quote_asset_code,
          reserved_asset_code, reserved_amount::text, created_at, updated_at`

	order, err := scanOrder(tx.QueryRow(ctx, query,
		payload.OrderID,
		payload.UserID,
		payload.Side,
		"open",
		payload.BaseAssetCode,
		payload.QuoteAssetCode,
		payload.ReservedAssetCode,
		payload.ReservedAmount,
		timestamp,
		timestamp,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return projection.OrderProjection{}, projection.ErrOrderExists
	}
	if err != nil {
		return projection.OrderProjection{}, fmt.Errorf("insert order projection: %w", err)
	}
	return order, nil
}

func cancelOrder(ctx context.Context, tx pgx.Tx, orderID string, timestamp time.Time) (projection.OrderProjection, error) {
	const query = `
UPDATE order_projections
SET status = $2, updated_at = $3
WHERE order_id = $1
RETURNING order_id, user_id, side, status, base_asset_code, quote_asset_code,
          reserved_asset_code, reserved_amount::text, created_at, updated_at`

	order, err := scanOrder(tx.QueryRow(ctx, query, orderID, "canceled", timestamp))
	if errors.Is(err, pgx.ErrNoRows) {
		return projection.OrderProjection{}, projection.ErrOrderNotFound
	}
	if err != nil {
		return projection.OrderProjection{}, fmt.Errorf("cancel order projection: %w", err)
	}
	return order, nil
}

func getOrder(ctx context.Context, db queryRower, orderID string) (projection.OrderProjection, bool, error) {
	const query = `
SELECT order_id, user_id, side, status, base_asset_code, quote_asset_code,
       reserved_asset_code, reserved_amount::text, created_at, updated_at
FROM order_projections
WHERE order_id = $1`

	order, err := scanOrder(db.QueryRow(ctx, query, orderID))
	if errors.Is(err, pgx.ErrNoRows) {
		return projection.OrderProjection{}, false, nil
	}
	if err != nil {
		return projection.OrderProjection{}, false, fmt.Errorf("get order projection: %w", err)
	}
	return order, true, nil
}

func scanOrder(row pgx.Row) (projection.OrderProjection, error) {
	var order projection.OrderProjection
	err := row.Scan(
		&order.OrderID,
		&order.UserID,
		&order.Side,
		&order.Status,
		&order.BaseAssetCode,
		&order.QuoteAssetCode,
		&order.ReservedAssetCode,
		&order.ReservedAmount,
		&order.CreatedAt,
		&order.UpdatedAt,
	)
	if err != nil {
		return projection.OrderProjection{}, err
	}
	return order, nil
}
