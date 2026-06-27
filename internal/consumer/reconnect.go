package consumer

import (
	"context"
	"time"
)

const (
	initialReconnectDelay = time.Second
	maximumReconnectDelay = 30 * time.Second
)

func reconnectDelay(attempt int) time.Duration {
	delay := initialReconnectDelay

	for attempt > 0 && delay < maximumReconnectDelay {
		if delay > maximumReconnectDelay/2 {
			return maximumReconnectDelay
		}

		delay *= 2
		attempt--
	}

	return delay
}

func waitForReconnect(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
