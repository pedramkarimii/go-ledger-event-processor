package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pedramkarimii/go-ledger-event-processor/internal/projection"
)

func TestHealth(t *testing.T) {
	router := NewRouter(projection.NewInMemoryStore())
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("expected application/json content type, got %q", contentType)
	}

}

func TestGetOrder(t *testing.T) {
	store := projection.NewInMemoryStore()
	_, _, err := store.Apply(projection.OrderEvent{
		EventKey:  "order.created:order-1",
		EventType: projection.EventOrderCreated,
		Payload: projection.OrderPayload{
			OrderID:           "order-1",
			UserID:            "user-1",
			Side:              "buy",
			BaseAssetCode:     "BTC",
			QuoteAssetCode:    "USDT",
			ReservedAssetCode: "USDT",
			ReservedAmount:    "20.000",
		},
	})
	if err != nil {
		t.Fatalf("seed order projection: %v", err)
	}

	router := NewRouter(store)
	request := httptest.NewRequest(http.MethodGet, "/v1/orders/order-1", nil)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}
	if !strings.Contains(response.Body.String(), `"order_id":"order-1"`) {
		t.Fatalf("expected order response, got %s", response.Body.String())
	}

}

func TestGetOrderReturnsNotFound(t *testing.T) {
	router := NewRouter(projection.NewInMemoryStore())
	request := httptest.NewRequest(http.MethodGet, "/v1/orders/missing", nil)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, response.Code)
	}

}
