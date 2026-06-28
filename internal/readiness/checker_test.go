package readiness

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

type fakeDatabasePinger struct {
	err   error
	calls int
}

func (pinger *fakeDatabasePinger) Ping(context.Context) error {
	pinger.calls++
	return pinger.err
}

type fakeCloser struct {
	closed bool
}

func (closer *fakeCloser) Close() error {
	closer.closed = true
	return nil
}

func TestCheckerChecksPostgreSQLAndRabbitMQ(t *testing.T) {
	database := &fakeDatabasePinger{}
	connection := &fakeCloser{}
	dialCalls := 0

	checker := &Checker{
		database:    database,
		rabbitMQURL: "amqp://app:app@rabbitmq:5672/",
		timeout:     time.Second,
		dial: func(url string, timeout time.Duration) (io.Closer, error) {
			dialCalls++

			if url != "amqp://app:app@rabbitmq:5672/" {
				t.Fatalf("dial URL = %q", url)
			}
			if timeout != time.Second {
				t.Fatalf("dial timeout = %s, want %s", timeout, time.Second)
			}

			return connection, nil
		},
	}

	if err := checker.Check(context.Background()); err != nil {
		t.Fatalf("check readiness: %v", err)
	}
	if database.calls != 1 {
		t.Fatalf("database calls = %d, want 1", database.calls)
	}
	if dialCalls != 1 {
		t.Fatalf("dial calls = %d, want 1", dialCalls)
	}
	if !connection.closed {
		t.Fatal("RabbitMQ connection was not closed")
	}
}

func TestCheckerStopsWhenPostgreSQLIsUnavailable(t *testing.T) {
	database := &fakeDatabasePinger{err: errors.New("database down")}
	dialCalls := 0

	checker := &Checker{
		database:    database,
		rabbitMQURL: "amqp://app:app@rabbitmq:5672/",
		timeout:     time.Second,
		dial: func(string, time.Duration) (io.Closer, error) {
			dialCalls++
			return &fakeCloser{}, nil
		},
	}

	err := checker.Check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "PostgreSQL unavailable") {
		t.Fatalf("check error = %v, want PostgreSQL unavailable", err)
	}
	if dialCalls != 0 {
		t.Fatalf("dial calls = %d, want 0", dialCalls)
	}
}

func TestCheckerReportsRabbitMQFailure(t *testing.T) {
	checker := &Checker{
		database:    &fakeDatabasePinger{},
		rabbitMQURL: "amqp://app:app@rabbitmq:5672/",
		timeout:     time.Second,
		dial: func(string, time.Duration) (io.Closer, error) {
			return nil, errors.New("broker down")
		},
	}

	err := checker.Check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "RabbitMQ unavailable") {
		t.Fatalf("check error = %v, want RabbitMQ unavailable", err)
	}
}
