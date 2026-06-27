package config

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	HTTPAddr         string
	DatabaseURL      string
	RabbitMQURL      string
	RabbitMQExchange string
	RabbitMQQueue    string
}

func Load() (Config, error) {
	config := Config{
		HTTPAddr:         valueOrDefault("HTTP_ADDR", ":8080"),
		DatabaseURL:      strings.TrimSpace(os.Getenv("DATABASE_URL")),
		RabbitMQURL:      strings.TrimSpace(os.Getenv("RABBITMQ_URL")),
		RabbitMQExchange: valueOrDefault("RABBITMQ_EXCHANGE", "crypto.ledger.events"),
		RabbitMQQueue:    valueOrDefault("RABBITMQ_QUEUE", "go-ledger-order-projections"),
	}

	if config.HTTPAddr == "" {
		return Config{}, fmt.Errorf("HTTP_ADDR must not be empty")
	}

	return config, nil
}

func valueOrDefault(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
