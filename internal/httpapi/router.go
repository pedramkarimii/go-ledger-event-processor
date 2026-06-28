package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pedramkarimii/go-ledger-event-processor/internal/projection"
)

type OrderReader interface {
	Get(ctx context.Context, orderID string) (projection.OrderProjection, bool, error)
}

type ReadinessChecker interface {
	Check(context.Context) error
}

type readinessFunc func(context.Context) error

func (check readinessFunc) Check(ctx context.Context) error {
	return check(ctx)
}

func NewRouter(orders OrderReader, checks ...ReadinessChecker) http.Handler {
	readinessCheck := ReadinessChecker(readinessFunc(func(context.Context) error {
		return nil
	}))
	if len(checks) > 0 && checks[0] != nil {
		readinessCheck = checks[0]
	}

	router := chi.NewRouter()
	metrics := NewHTTPMetrics()

	router.Use(metrics.Middleware)
	router.Get("/metrics", metrics.Handler)

	router.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"service": "go-ledger-event-processor",
			"status":  "ok",
		})
	})

	router.Get("/ready", func(w http.ResponseWriter, r *http.Request) {
		if err := readinessCheck.Check(r.Context()); err != nil {
			slog.Warn("API readiness check failed", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status": "not_ready",
			})
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})

	router.Get("/v1/orders/{orderID}", func(w http.ResponseWriter, r *http.Request) {
		orderID := chi.URLParam(r, "orderID")
		order, exists, err := orders.Get(r.Context(), orderID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": "could not load order",
			})
			return
		}
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
