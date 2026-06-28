package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/pedramkarimii/go-ledger-event-processor/internal/config"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/httpapi"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/readiness"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/storage"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if err := run(); err != nil {
		slog.Error("API stopped with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	pool, err := storage.OpenPool(context.Background(), cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open PostgreSQL pool: %w", err)
	}
	defer pool.Close()

	readinessChecker, err := readiness.New(pool, cfg.RabbitMQURL)
	if err != nil {
		return fmt.Errorf("create readiness checker: %w", err)
	}

	store := storage.NewProjectionStore(pool)
	server := httpapi.NewServer(cfg.HTTPAddr, httpapi.NewRouter(store, readinessChecker))

	slog.Info("HTTP server listening", "address", cfg.HTTPAddr)

	if err := server.ListenAndServe(); err != nil {
		return fmt.Errorf("serve HTTP API: %w", err)
	}

	return nil
}
