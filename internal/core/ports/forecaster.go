package ports

import (
	"context"
	"graph-alert-engine/internal/core/domain"
)

type Prediction struct {
	PredictedLatencyP95Ms float64
}

type Forecaster interface {
	Predict(ctx context.Context, svc domain.ServiceNode, history []domain.Telemetry) (Prediction, error)
}
