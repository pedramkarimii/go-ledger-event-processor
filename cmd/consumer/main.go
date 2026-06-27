package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pedramkarimii/go-ledger-event-processor/internal/config"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/consumer"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/storage"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	pool, err := storage.OpenPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
	}

	log.Printf("RabbitMQ consumer listening on queue %s", cfg.RabbitMQQueue)
	if err := worker.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
