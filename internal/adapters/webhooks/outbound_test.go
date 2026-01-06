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
	received := make(chan AlertEvent, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Signature") == "" {
			t.Error("missing signature")
		}
		var event AlertEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Errorf("bad payload: %v", err)
			return
		}
		received <- event
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
		Service: domain.ServiceNode{Name: "test", Namespace: "default"},
		State:   domain.AlertStateFiring,
		Type:    domain.AlertTypeAvailabilityDegraded,
		Risk: domain.RiskScore{
			Score: 99,
			Meta: domain.RiskMetadata{
				ModelVersion:     "v2.0-test",
				ThresholdVersion: "test",
				CalculationID:    "test-calc-123",
			},
		},
		CreatedAt:   time.Now(),
		DedupeKey:   "test-dedupe",
		ReasonCodes: []string{"TEST_REASON"},
		Priority:    "P1",
	}
	_ = bus.Publish(ctx, domain.TopicRiskAlertRaised, alert)

	select {
	case got := <-received:
		if got.Decision.RiskScore == nil || *got.Decision.RiskScore != 99 {
			t.Errorf("expected risk score 99 in decision, got %v", got.Decision.RiskScore)
		}
		if got.SchemaVersion != "alerts.v1" {
			t.Errorf("expected schema_version alerts.v1, got %s", got.SchemaVersion)
		}
		if got.EventID == "" {
			t.Error("expected non-empty event_id")
		}
		if got.DedupeKey == "" {
			t.Error("expected non-empty dedupe_key")
		}
		if got.Alert.State != "firing" {
			t.Errorf("expected state firing, got %s", got.Alert.State)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for webhook")
	}
}
