package main

import (
	"context"
	"log"

	"github.com/pedramkarimii/go-ledger-event-processor/internal/config"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/httpapi"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/storage"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	pool, err := storage.OpenPool(context.Background(), cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	store := storage.NewProjectionStore(pool)
	server := httpapi.NewServer(cfg.HTTPAddr, httpapi.NewRouter(store))

	log.Printf("HTTP server listening on %s", cfg.HTTPAddr)
	log.Fatal(server.ListenAndServe())
}
