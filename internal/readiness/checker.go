package readiness

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const defaultTimeout = 2 * time.Second

type databasePinger interface {
	Ping(context.Context) error
}

type Checker struct {
	database    databasePinger
	rabbitMQURL string
	timeout     time.Duration
	dial        func(string, time.Duration) (io.Closer, error)
}

func New(database databasePinger, rabbitMQURL string) (*Checker, error) {
	if database == nil {
		return nil, fmt.Errorf("PostgreSQL pinger must not be nil")
	}
	if strings.TrimSpace(rabbitMQURL) == "" {
		return nil, fmt.Errorf("RABBITMQ_URL must not be empty")
	}

	return &Checker{
		database:    database,
		rabbitMQURL: rabbitMQURL,
		timeout:     defaultTimeout,
		dial:        dialRabbitMQ,
	}, nil
}

func (checker *Checker) Check(ctx context.Context) error {
	checkContext, cancel := context.WithTimeout(ctx, checker.timeout)
	defer cancel()

	if err := checker.database.Ping(checkContext); err != nil {
		return fmt.Errorf("PostgreSQL unavailable: %w", err)
	}

	connection, err := checker.dial(checker.rabbitMQURL, checker.timeout)
	if err != nil {
		return fmt.Errorf("RabbitMQ unavailable: %w", err)
	}
	defer connection.Close()

	return nil
}

func dialRabbitMQ(url string, timeout time.Duration) (io.Closer, error) {
	dialer := &net.Dialer{Timeout: timeout}

	return amqp.DialConfig(url, amqp.Config{
		Dial: func(network, address string) (net.Conn, error) {
			return dialer.Dial(network, address)
		},
	})
}
