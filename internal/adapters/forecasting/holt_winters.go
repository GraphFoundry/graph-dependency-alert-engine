package forecasting

import (
	"context"
	"graph-alert-engine/internal/core/domain"
	"graph-alert-engine/internal/core/ports"
)

type HoltWinters struct{}

func New() *HoltWinters { return &HoltWinters{} }

func (h *HoltWinters) Predict(ctx context.Context, svc domain.ServiceNode, history []domain.Telemetry) (ports.Prediction, error) {
	// TODO: implement real Holt-Winters.
	// Deterministic placeholder: use last observed latency (or 0).
	if len(history) == 0 {
		return ports.Prediction{PredictedLatencyP95Ms: 0}, nil
	}
	last := history[len(history)-1]
	return ports.Prediction{PredictedLatencyP95Ms: last.LatencyP95Ms}, nil
}
