package forecasting

import (
	"context"
	"graph-alert-engine/internal/core/domain"
	"graph-alert-engine/internal/core/ports"
)

type HybridForecaster struct{}

func New() *HybridForecaster { return &HybridForecaster{} }

func (h *HybridForecaster) Predict(ctx context.Context, svc domain.ServiceNode, history []domain.Telemetry) (ports.Prediction, error) {
	if len(history) < 5 {
		// Not enough data, use last value or 0
		if len(history) == 0 {
			return ports.Prediction{PredictedLatencyP95Ms: 0}, nil
		}
		return ports.Prediction{PredictedLatencyP95Ms: history[len(history)-1].LatencyP95Ms}, nil
	}

	// ARIMA-like AR(3) model: Y_t = w1*Y_{t-1} + w2*Y_{t-2} + w3*Y_{t-3} + c
	// Simple pre-trained weights for a "reactionary" model
	// In production, these should be fitted online or separate service.
	ts := extractLatency(history)
	n := len(ts)

	// Weights for AR(3) - emphasizing recent trend
	w1, w2, w3 := 0.6, 0.3, 0.1

	y1 := ts[n-1]
	y2 := ts[n-2]
	y3 := ts[n-3]

	pred := (w1 * y1) + (w2 * y2) + (w3 * y3)

	return ports.Prediction{PredictedLatencyP95Ms: pred}, nil
}

func extractLatency(h []domain.Telemetry) []float64 {
	out := make([]float64, len(h))
	for i, v := range h {
		out[i] = v.LatencyP95Ms
	}
	return out
}
