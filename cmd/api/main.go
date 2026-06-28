package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/pedramkarimii/go-ledger-event-processor/internal/config"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/httpapi"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/storage"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("load configuration", "error", err)
		os.Exit(1)
	}

	pool, err := storage.OpenPool(context.Background(), cfg.DatabaseURL)
	if err != nil {
		slog.Error("open PostgreSQL pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	store := storage.NewProjectionStore(pool)
	server := httpapi.NewServer(cfg.HTTPAddr, httpapi.NewRouter(store))

	slog.Info("HTTP server listening", "address", cfg.HTTPAddr)

	if err := server.ListenAndServe(); err != nil {
		slog.Error("HTTP server stopped", "error", err)
		os.Exit(1)
	}
}
