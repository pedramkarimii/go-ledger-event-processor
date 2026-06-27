package consumer

import (
	"testing"
	"time"
)

func TestReconnectDelay(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: time.Second},
		{attempt: 1, want: 2 * time.Second},
		{attempt: 2, want: 4 * time.Second},
		{attempt: 4, want: 16 * time.Second},
		{attempt: 5, want: 30 * time.Second},
		{attempt: 20, want: 30 * time.Second},
	}

	for _, test := range tests {
		if got := reconnectDelay(test.attempt); got != test.want {
			t.Fatalf("reconnectDelay(%d) = %s, want %s", test.attempt, got, test.want)
		}
	}
}
