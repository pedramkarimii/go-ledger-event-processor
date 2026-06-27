package main

import (
	"errors"
	"log"
	"net/http"

	"github.com/pedramkarimii/go-ledger-event-processor/internal/config"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/httpapi"
)

func main() {
	config, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	server := httpapi.NewServer(config.HTTPAddr, httpapi.NewRouter())
	log.Printf("HTTP API listening on %s", config.HTTPAddr)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
