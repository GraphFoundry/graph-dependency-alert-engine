package forecasting

import (
	"context"
	"graph-alert-engine/internal/core/domain"
	"testing"
)

func TestHybridForecaster_Predict(t *testing.T) {
	hw := New()
	ctx := context.Background()
	svc := domain.ServiceNode{Name: "foo"}

	// Case 1: Empty history
	pred, err := hw.Predict(ctx, svc, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pred.PredictedLatencyP95Ms != 0 {
		t.Errorf("expected 0 for empty history, got %f", pred.PredictedLatencyP95Ms)
	}

	// Case 2: History
	hist := []domain.Telemetry{
		{LatencyP95Ms: 100},
		{LatencyP95Ms: 200},
	}
	pred, err = hw.Predict(ctx, svc, hist)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Stub implementation returns last value
	if pred.PredictedLatencyP95Ms != 200 {
		t.Errorf("expected 200, got %f", pred.PredictedLatencyP95Ms)
	}
}
