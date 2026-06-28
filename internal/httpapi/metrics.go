package httpapi

import (
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
)

type httpMetricKey struct {
	method string
	route  string
	status int
}

type HTTPMetrics struct {
	mu       sync.Mutex
	requests map[httpMetricKey]uint64
}

func NewHTTPMetrics() *HTTPMetrics {
	return &HTTPMetrics{
		requests: make(map[httpMetricKey]uint64),
	}
}

func (metrics *HTTPMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		recorder := &statusRecorder{
			ResponseWriter: w,
			status:         http.StatusOK,
		}

		next.ServeHTTP(recorder, r)

		route := routePattern(r)
		metrics.record(r.Method, route, recorder.status)

		if route != "/health" && route != "/ready" && route != "/metrics" {
			slog.Info(
				"HTTP request completed",
				"method", r.Method,
				"route", route,
				"status", recorder.status,
				"duration_ms", time.Since(startedAt).Milliseconds(),
			)
		}
	})
}

func (metrics *HTTPMetrics) Handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	keys, values := metrics.snapshot()

	fmt.Fprintln(w, "# HELP http_requests_total Total HTTP responses completed by the API.")
	fmt.Fprintln(w, "# TYPE http_requests_total counter")

	for _, key := range keys {
		fmt.Fprintf(
			w,
			"http_requests_total{method=%s,route=%s,status=%s} %d\n",
			strconv.Quote(key.method),
			strconv.Quote(key.route),
			strconv.Quote(strconv.Itoa(key.status)),
			values[key],
		)
	}
}

func (metrics *HTTPMetrics) record(method, route string, status int) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	metrics.requests[httpMetricKey{
		method: method,
		route:  route,
		status: status,
	}]++
}

func (metrics *HTTPMetrics) snapshot() ([]httpMetricKey, map[httpMetricKey]uint64) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	keys := make([]httpMetricKey, 0, len(metrics.requests))
	values := make(map[httpMetricKey]uint64, len(metrics.requests))

	for key, value := range metrics.requests {
		keys = append(keys, key)
		values[key] = value
	}

	sort.Slice(keys, func(left, right int) bool {
		if keys[left].route != keys[right].route {
			return keys[left].route < keys[right].route
		}
		if keys[left].method != keys[right].method {
			return keys[left].method < keys[right].method
		}
		return keys[left].status < keys[right].status
	})

	return keys, values
}

func routePattern(request *http.Request) string {
	routeContext := chi.RouteContext(request.Context())
	if routeContext == nil {
		return "unknown"
	}

	pattern := routeContext.RoutePattern()
	if pattern == "" {
		return "unknown"
	}

	return pattern
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (recorder *statusRecorder) WriteHeader(status int) {
	if recorder.wrote {
		return
	}

	recorder.status = status
	recorder.wrote = true
	recorder.ResponseWriter.WriteHeader(status)
}

func (recorder *statusRecorder) Write(body []byte) (int, error) {
	if !recorder.wrote {
		recorder.WriteHeader(http.StatusOK)
	}

	return recorder.ResponseWriter.Write(body)
}
