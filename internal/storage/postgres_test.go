package storage

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestOpenPoolRejectsMissingURL(t *testing.T) {
	_, err := OpenPool(context.Background(), "")
	if err == nil {
		t.Fatal("expected missing DATABASE_URL error")
	}
}

func TestOpenPool(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := OpenPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
}
