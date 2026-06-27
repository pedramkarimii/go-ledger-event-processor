package main

import (
	"log"

	"github.com/pedramkarimii/go-ledger-event-processor/internal/config"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/httpapi"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/projection"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	store := projection.NewInMemoryStore()
	server := httpapi.NewServer(cfg.HTTPAddr, httpapi.NewRouter(store))

	log.Printf("HTTP server listening on %s", cfg.HTTPAddr)
	log.Fatal(server.ListenAndServe())

}
