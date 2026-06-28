package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/pedramkarimii/go-ledger-event-processor/internal/projection"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ProjectionApplier interface {
	Apply(ctx context.Context, event projection.OrderEvent) (projection.OrderProjection, bool, error)
}

type Config struct {
	URL                string
	Exchange           string
	Queue              string
	DeadLetterExchange string
	DeadLetterQueue    string
}

type Worker struct {
	config  Config
	applier ProjectionApplier
	metrics *Metrics
}

func New(config Config, applier ProjectionApplier) (*Worker, error) {
	if strings.TrimSpace(config.URL) == "" {
		return nil, fmt.Errorf("RABBITMQ_URL must not be empty")
	}
	if strings.TrimSpace(config.Exchange) == "" {
		return nil, fmt.Errorf("RABBITMQ_EXCHANGE must not be empty")
	}
	if strings.TrimSpace(config.Queue) == "" {
		return nil, fmt.Errorf("RABBITMQ_QUEUE must not be empty")
	}
	if strings.TrimSpace(config.DeadLetterExchange) == "" {
		return nil, fmt.Errorf("dead-letter exchange must not be empty")
	}
	if strings.TrimSpace(config.DeadLetterQueue) == "" {
		return nil, fmt.Errorf("dead-letter queue must not be empty")
	}
	if applier == nil {
		return nil, fmt.Errorf("projection applier must not be nil")
	}

	return &Worker{
		config:  config,
		applier: applier,
		metrics: NewMetrics(),
	}, nil
}

func (worker *Worker) Metrics() *Metrics {
	return worker.metrics
}

func (worker *Worker) Run(ctx context.Context) error {
	attempt := 0

	for {
		connected, err := worker.runSession(ctx)
		if ctx.Err() != nil {
			return nil
		}

		if err == nil {
			err = fmt.Errorf("RabbitMQ session ended unexpectedly")
		}

		if connected {
			attempt = 0
		}

		delay := reconnectDelay(attempt)
		worker.metrics.IncReconnects()
		slog.Warn("RabbitMQ consumer reconnecting", "delay", delay.String(), "error", err)

		if !waitForReconnect(ctx, delay) {
			return nil
		}

		attempt++
	}
}

func (worker *Worker) runSession(ctx context.Context) (bool, error) {
	connection, err := amqp.Dial(worker.config.URL)
	if err != nil {
		return false, fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		return false, fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	defer channel.Close()

	if err := channel.ExchangeDeclare(worker.config.Exchange, "topic", true, false, false, false, nil); err != nil {
		return false, fmt.Errorf("declare RabbitMQ exchange: %w", err)
	}

	if err := channel.ExchangeDeclare(worker.config.DeadLetterExchange, "topic", true, false, false, false, nil); err != nil {
		return false, fmt.Errorf("declare RabbitMQ dead-letter exchange: %w", err)
	}

	deadLetterQueue, err := channel.QueueDeclare(worker.config.DeadLetterQueue, true, false, false, false, nil)
	if err != nil {
		return false, fmt.Errorf("declare RabbitMQ dead-letter queue: %w", err)
	}

	if err := channel.QueueBind(deadLetterQueue.Name, "#", worker.config.DeadLetterExchange, false, nil); err != nil {
		return false, fmt.Errorf("bind RabbitMQ dead-letter queue: %w", err)
	}

	queue, err := channel.QueueDeclare(
		worker.config.Queue,
		true,
		false,
		false,
		false,
		amqp.Table{"x-dead-letter-exchange": worker.config.DeadLetterExchange},
	)
	if err != nil {
		return false, fmt.Errorf("declare RabbitMQ queue: %w", err)
	}

	if err := channel.QueueBind(queue.Name, "order.*", worker.config.Exchange, false, nil); err != nil {
		return false, fmt.Errorf("bind RabbitMQ queue: %w", err)
	}

	if err := channel.Qos(1, 0, false); err != nil {
		return false, fmt.Errorf("configure RabbitMQ QoS: %w", err)
	}

	deliveries, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return false, fmt.Errorf("start RabbitMQ consumer: %w", err)
	}

	slog.Info("RabbitMQ consumer connected", "queue", queue.Name)

	for {
		select {
		case <-ctx.Done():
			return true, nil
		case delivery, open := <-deliveries:
			if !open {
				return true, fmt.Errorf("RabbitMQ delivery channel closed")
			}
			if err := worker.process(ctx, delivery); err != nil {
				return true, fmt.Errorf("process RabbitMQ delivery: %w", err)
			}
		}
	}
}

func (worker *Worker) process(ctx context.Context, delivery amqp.Delivery) error {
	event, err := DecodeOrderEvent(delivery.Body)
	if err != nil {
		if err := delivery.Reject(false); err != nil {
			return err
		}

		worker.metrics.IncRejected()
		slog.Warn("RabbitMQ event rejected", "reason", "invalid_event", "error", err)
		return nil
	}

	_, applied, err := worker.applier.Apply(ctx, event)
	if err != nil {
		if err := delivery.Nack(false, true); err != nil {
			return err
		}

		worker.metrics.IncRequeued()
		slog.Error(
			"RabbitMQ event requeued",
			"event_key", event.EventKey,
			"event_type", event.EventType,
			"order_id", event.Payload.OrderID,
			"error", err,
		)
		return nil
	}

	if err := delivery.Ack(false); err != nil {
		return err
	}

	worker.metrics.IncProcessed()
	slog.Info(
		"RabbitMQ event processed",
		"event_key", event.EventKey,
		"event_type", event.EventType,
		"order_id", event.Payload.OrderID,
		"applied", applied,
	)
	return nil
}

func DecodeOrderEvent(body []byte) (projection.OrderEvent, error) {
	var event projection.OrderEvent

	if err := json.Unmarshal(body, &event); err != nil {
		return projection.OrderEvent{}, fmt.Errorf("decode order event: %w", err)
	}
	if err := event.Validate(); err != nil {
		return projection.OrderEvent{}, err
	}

	return event, nil
}
