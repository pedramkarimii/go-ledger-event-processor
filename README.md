# Go Ledger Event Processor

Go service for idempotent RabbitMQ order-event processing, PostgreSQL projections, and a read API.

## Overview

The service consumes `order.created` and `order.canceled` events, records each `event_key` once, updates an order projection in the same PostgreSQL transaction, and exposes the projection through HTTP.

```text
RabbitMQ events
|
v
Go consumer
|
v
PostgreSQL: processed_events + order_projections
|
v
Go read API
```

## Reliability behavior

* Uses manual RabbitMQ acknowledgements and acknowledges only after PostgreSQL processing succeeds.
* Rejects invalid JSON or invalid event payloads without requeueing; RabbitMQ routes them to a durable dead-letter queue (DLQ).
* Requeues messages when projection processing fails.
* Reconnects after RabbitMQ connection or delivery-channel failures with capped exponential backoff.
* Uses `processed_events.event_key` as the idempotency key.
* Writes the processed-event record and order projection in one PostgreSQL transaction.
* Handles `SIGINT` and `SIGTERM` for graceful consumer shutdown.

## Health and readiness

`GET /health` confirms that the API process is running.

`GET /ready` verifies that both runtime dependencies are usable at request time:

* PostgreSQL responds to a pool ping.
* RabbitMQ accepts a short AMQP connection and authentication handshake.

When either dependency is unavailable, `/ready` returns HTTP `503` with `{"status":"not_ready"}`. It returns HTTP `200` with `{"status":"ready"}` only when both checks pass.

## Observability

Both processes write structured JSON logs to standard output.

The API exposes request counters in Prometheus text format at `GET /metrics`:

* `http_requests_total{method,route,status}`

The consumer exposes event counters in Prometheus text format at its own metrics address:

* `consumer_events_total{outcome="processed|rejected|requeued"}`
* `consumer_reconnects_total`

A rejected invalid event increments the consumer `rejected` counter and is placed in the DLQ. A successfully projected event increments the `processed` counter.

## Supported events

### order.created

```json
{
  "event_key": "order.created:order-1",
  "event_type": "order.created",
  "occurred_at": "2026-06-27T16:30:00Z",
  "payload": {
    "order_id": "order-1",
    "user_id": "user-1",
    "side": "buy",
    "base_asset_code": "BTC",
    "quote_asset_code": "USDT",
    "reserved_asset_code": "USDT",
    "reserved_amount": "20.000"
  }
}
```

### order.canceled

```json
{
  "event_key": "order.canceled:order-1",
  "event_type": "order.canceled",
  "occurred_at": "2026-06-27T16:31:00Z",
  "payload": {
    "order_id": "order-1"
  }
}
```

## Run locally

Requirements: Go 1.25+, Docker Engine, and Docker Compose.

```bash
docker compose up -d --build
```

| Service             | Address                         |
| ------------------- | ------------------------------- |
| HTTP API            | `http://localhost:8084`         |
| API health          | `http://localhost:8084/health`  |
| API readiness       | `http://localhost:8084/ready`   |
| API metrics         | `http://localhost:8084/metrics` |
| Consumer metrics    | `http://localhost:8085/metrics` |
| PostgreSQL          | `localhost:5434`                |
| RabbitMQ AMQP       | `localhost:5674`                |
| RabbitMQ Management | `http://localhost:15674`        |

RabbitMQ credentials: `app` / `app`.

The default RabbitMQ topology is:

| Resource | Name |
| --- | --- |
| Event exchange | `crypto.ledger.events` |
| Projection queue | `go-ledger-order-projections` |
| Dead-letter exchange | `crypto.ledger.events.dlx` |
| Dead-letter queue | `go-ledger-order-projections.dlq` |

Useful commands:

```bash
docker compose ps
docker compose logs -f api consumer
curl http://localhost:8084/health
curl http://localhost:8084/ready
curl http://localhost:8084/metrics
curl http://localhost:8085/metrics
docker compose down -v
```

## API

```text
GET /health
GET /ready
GET /metrics
GET /v1/orders/{orderID}
```

Example:

```bash
curl http://localhost:8084/v1/orders/order-1
```

## Tests

```bash
go test ./...
go vet ./...
```

Run PostgreSQL integration tests against the local stack:

```bash
env TEST_DATABASE_URL="postgres://processor:processor@localhost:5434/ledger_processor?sslmode=disable" go test ./internal/storage -v
```

## Continuous integration

GitHub Actions runs:

* formatting, module consistency, `go vet`, and unit tests;
* Docker Compose validation;
* a Docker end-to-end test that verifies health, real readiness, RabbitMQ topology, and the consumer metrics endpoint;
* readiness failure and recovery while RabbitMQ and PostgreSQL are stopped and restarted;
* invalid-event routing to the DLQ plus the consumer rejection counter;
* valid `order.created` processing plus the API projection and consumer success counter.

## Project structure

```text
cmd/api                 HTTP API entry point
cmd/consumer            RabbitMQ consumer and metrics entry point
internal/config         Environment configuration
internal/consumer       Event decoding, delivery handling, and consumer metrics
internal/httpapi        HTTP routing, API metrics, readiness response, and JSON responses
internal/readiness      PostgreSQL and RabbitMQ readiness checks
internal/projection     Event model and in-memory test store
internal/storage        PostgreSQL pool and projection store
migrations              PostgreSQL schema initialization
```

## Scope

This repository focuses on reliable order projections with local observability and dependency-aware readiness. Natural next additions include distributed tracing, alerting rules, metric scraping configuration, and production migration management.
