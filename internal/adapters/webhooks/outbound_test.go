package webhooks

import (
	"context"
	"encoding/json"
	"graph-alert-engine/internal/adapters/eventbus"
	"graph-alert-engine/internal/core/domain"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestOutbound_Dispatch(t *testing.T) {
	// Mock destination
	received := make(chan domain.Alert, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Signature") == "" {
			t.Error("missing signature")
		}
		var a domain.Alert
		if err := json.NewDecoder(r.Body).Decode(&a); err != nil {
			t.Errorf("bad payload: %v", err)
			return
		}
		received <- a
		w.WriteHeader(200)
	}))
	defer srv.Close()

	bus := eventbus.New()
	out := NewOutbound(bus, []string{srv.URL}, []byte("secret"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = out.Start(ctx)

	// Publish alert
	alert := domain.Alert{
		Service: domain.ServiceNode{Name: "test"},
		Risk:    domain.RiskScore{Score: 99},
	}
	_ = bus.Publish(ctx, domain.TopicRiskAlertRaised, alert)

	select {
	case got := <-received:
		if got.Risk.Score != 99 {
			t.Errorf("expected score 99, got %f", got.Risk.Score)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for webhook")
	}
}
