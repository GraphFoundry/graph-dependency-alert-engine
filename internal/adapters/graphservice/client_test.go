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

func TestClient_GetHealth_ParsesResponse(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"status":"OK","stale":false,"lastUpdatedSecondsAgo":42,"windowMinutes":5}`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New(srv.URL, 500*time.Millisecond, 0, NewCircuitBreaker(2, time.Second), 0)
	h, err := c.GetHealth(context.Background())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if h.Stale {
		t.Fatalf("expected non-stale")
	}
	if h.WindowMinutes != 5 {
		t.Fatalf("expected window 5, got %d", h.WindowMinutes)
	}
	if h.LastUpdatedSecondsAgo == nil || *h.LastUpdatedSecondsAgo != 42 {
		t.Fatalf("unexpected last updated: %+v", h.LastUpdatedSecondsAgo)
	}
}

func TestClient_GetHealth_FallbacksToGraphPrefix(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	})
	mux.HandleFunc("/graph/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"status":"OK","stale":true,"lastUpdatedSecondsAgo":7,"windowMinutes":3}`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New(srv.URL, 500*time.Millisecond, 0, NewCircuitBreaker(2, time.Second), 0)
	h, err := c.GetHealth(context.Background())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !h.Stale {
		t.Fatalf("expected stale from fallback")
	}
	if h.WindowMinutes != 3 {
		t.Fatalf("expected window 3, got %d", h.WindowMinutes)
	}
	if h.LastUpdatedSecondsAgo == nil || *h.LastUpdatedSecondsAgo != 7 {
		t.Fatalf("unexpected last updated: %+v", h.LastUpdatedSecondsAgo)
	}
}

func TestClient_GetNeighborhood_ParsesNodes(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/services/payments/neighborhood", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("k"); got != "2" {
			t.Fatalf("expected k=2, got %s", got)
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"nodes":["default/payments","frontend","checkout"]}`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New(srv.URL, 500*time.Millisecond, 0, NewCircuitBreaker(2, time.Second), 0)
	nodes, err := c.GetNeighborhood(context.Background(), domain.ServiceNode{Name: "payments", Namespace: "default"}, 2)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}
	if nodes[0].Name != "payments" {
		t.Fatalf("expected first node payments, got %s", nodes[0].Name)
	}
}
