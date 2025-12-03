package graphservice

import (
	"context"
	"graph-alert-engine/internal/core/domain"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClient_GetCentrality_ParsesScores(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/centrality", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"scores":[{"service":"frontend","pagerank":0.5,"betweenness":0.1}]}`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New(srv.URL, 500*time.Millisecond, 0, NewCircuitBreaker(2, time.Second), 0)
	m, err := c.GetCentrality(context.Background())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// "frontend" -> ID depends on assumption. In client code we default to "default".
	// "default/frontend"
	if len(m) != 1 {
		t.Fatalf("expected 1 item, got %d", len(m))
	}

	// Check for a key ending in frontend or just check the value
	var entry domain.Centrality
	for _, v := range m {
		entry = v
		break
	}

	if entry.Service.Name != "frontend" {
		t.Errorf("expected name frontend, got %s", entry.Service.Name)
	}
	if entry.PageRank != 0.5 {
		t.Errorf("expected pagerank 0.5, got %f", entry.PageRank)
	}
}
