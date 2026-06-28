package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pedramkarimii/go-ledger-event-processor/internal/config"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/consumer"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/storage"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if err := run(); err != nil {
		slog.Error("consumer stopped with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	pool, err := storage.OpenPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open PostgreSQL pool: %w", err)
	}
	defer pool.Close()

	worker, err := consumer.New(consumer.Config{
		URL:                cfg.RabbitMQURL,
		Exchange:           cfg.RabbitMQExchange,
		Queue:              cfg.RabbitMQQueue,
		DeadLetterExchange: cfg.RabbitMQExchange + ".dlx",
		DeadLetterQueue:    cfg.RabbitMQQueue + ".dlq",
	}, storage.NewProjectionStore(pool))
	if err != nil {
		return fmt.Errorf("create RabbitMQ consumer: %w", err)
	}

	metricsListener, err := net.Listen("tcp", cfg.ConsumerMetricsAddr)
	if err != nil {
		return fmt.Errorf("listen for consumer metrics: %w", err)
	}

	metricsServer := &http.Server{
		Handler:           worker.Metrics().Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	metricsErrors := make(chan error, 1)
	go func() {
		slog.Info("consumer metrics server listening", "address", cfg.ConsumerMetricsAddr)

		if err := metricsServer.Serve(metricsListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			metricsErrors <- fmt.Errorf("serve consumer metrics: %w", err)
		}
	}()

	workerErrors := make(chan error, 1)
	go func() {
		workerErrors <- worker.Run(ctx)
	}()

	slog.Info("RabbitMQ consumer starting", "queue", cfg.RabbitMQQueue)

	select {
	case err := <-metricsErrors:
		stop()
		_ = metricsServer.Close()
		<-workerErrors
		return err
	case err := <-workerErrors:
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if shutdownErr := metricsServer.Shutdown(shutdownContext); shutdownErr != nil {
			return fmt.Errorf("shutdown consumer metrics server: %w", shutdownErr)
		}

		return err
	}
}
