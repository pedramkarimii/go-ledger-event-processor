package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/projection"
)

type OrderReader interface {
	Get(orderID string) (projection.OrderProjection, bool)
}

func NewRouter(orders OrderReader) http.Handler {
	router := chi.NewRouter()

	router.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"service": "go-ledger-event-processor",
			"status":  "ok",
		})
	})

	router.Get("/ready", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})

	router.Get("/v1/orders/{orderID}", func(w http.ResponseWriter, r *http.Request) {
		orderID := chi.URLParam(r, "orderID")
		order, exists := orders.Get(orderID)
		if !exists {
			writeJSON(w, http.StatusNotFound, map[string]string{
				"error": "order not found",
			})
			return
		}

		writeJSON(w, http.StatusOK, order)
	})

	return router

}

func NewServer(address string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
