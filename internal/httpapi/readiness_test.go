package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadyReturnsOKWhenDependenciesAreReady(t *testing.T) {
	router := NewRouter(nil, readinessFunc(func(context.Context) error {
		return nil
	}))

	request := httptest.NewRequest(http.MethodGet, "/ready", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("ready status = %d, want %d", response.Code, http.StatusOK)
	}
	if !strings.Contains(response.Body.String(), `"status":"ready"`) {
		t.Fatalf("ready response = %s", response.Body.String())
	}
}

func TestReadyReturnsServiceUnavailableWhenDependencyFails(t *testing.T) {
	router := NewRouter(nil, readinessFunc(func(context.Context) error {
		return errors.New("RabbitMQ unavailable")
	}))

	request := httptest.NewRequest(http.MethodGet, "/ready", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if !strings.Contains(response.Body.String(), `"status":"not_ready"`) {
		t.Fatalf("ready response = %s", response.Body.String())
	}
}
