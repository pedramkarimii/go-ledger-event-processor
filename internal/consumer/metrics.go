package consumer

import (
	"fmt"
	"net/http"
	"sync"
)

type Metrics struct {
	mu         sync.Mutex
	processed  uint64
	rejected   uint64
	requeued   uint64
	reconnects uint64
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (metrics *Metrics) IncProcessed() {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	metrics.processed++
}

func (metrics *Metrics) IncRejected() {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	metrics.rejected++
}

func (metrics *Metrics) IncRequeued() {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	metrics.requeued++
}

func (metrics *Metrics) IncReconnects() {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	metrics.reconnects++
}

func (metrics *Metrics) Handler() http.Handler {
	return http.HandlerFunc(metrics.writePrometheus)
}

func (metrics *Metrics) writePrometheus(writer http.ResponseWriter, _ *http.Request) {
	processed, rejected, requeued, reconnects := metrics.snapshot()

	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	fmt.Fprintln(writer, "# HELP consumer_events_total Total RabbitMQ events by terminal consumer outcome.")
	fmt.Fprintln(writer, "# TYPE consumer_events_total counter")
	fmt.Fprintf(writer, "consumer_events_total{outcome=\"processed\"} %d\n", processed)
	fmt.Fprintf(writer, "consumer_events_total{outcome=\"rejected\"} %d\n", rejected)
	fmt.Fprintf(writer, "consumer_events_total{outcome=\"requeued\"} %d\n", requeued)
	fmt.Fprintln(writer, "# HELP consumer_reconnects_total Total RabbitMQ consumer reconnect attempts.")
	fmt.Fprintln(writer, "# TYPE consumer_reconnects_total counter")
	fmt.Fprintf(writer, "consumer_reconnects_total %d\n", reconnects)
}

func (metrics *Metrics) snapshot() (uint64, uint64, uint64, uint64) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	return metrics.processed, metrics.rejected, metrics.requeued, metrics.reconnects
}
