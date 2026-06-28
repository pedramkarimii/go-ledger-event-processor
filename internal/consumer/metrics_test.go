package consumer

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsExposeConsumerCounters(t *testing.T) {
	metrics := NewMetrics()

	metrics.IncProcessed()
	metrics.IncProcessed()
	metrics.IncRejected()
	metrics.IncRequeued()
	metrics.IncReconnects()

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()

	metrics.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", response.Code, http.StatusOK)
	}

	body := response.Body.String()
	expectedLines := []string{
		"consumer_events_total{outcome=\"processed\"} 2",
		"consumer_events_total{outcome=\"rejected\"} 1",
		"consumer_events_total{outcome=\"requeued\"} 1",
		"consumer_reconnects_total 1",
	}

	for _, expected := range expectedLines {
		if !strings.Contains(body, expected) {
			t.Fatalf("metrics response does not contain %q:\n%s", expected, body)
		}
	}
}
