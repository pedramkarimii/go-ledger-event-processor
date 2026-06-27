package main

import (
	"context"
	"log"

	"github.com/pedramkarimii/go-ledger-event-processor/internal/config"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/consumer"
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

	worker, err := consumer.New(consumer.Config{
		URL:      cfg.RabbitMQURL,
		Exchange: cfg.RabbitMQExchange,
		Queue:    cfg.RabbitMQQueue,
	}, storage.NewProjectionStore(pool))
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("RabbitMQ consumer listening on queue %s", cfg.RabbitMQQueue)
	if err := worker.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
