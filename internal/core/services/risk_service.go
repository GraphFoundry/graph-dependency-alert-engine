package services

import (
	"context"
	"fmt"
	"graph-alert-engine/internal/core/domain"
	"graph-alert-engine/internal/core/ports"
	"log/slog"
	"sync"
)

type Weights struct {
	W1PageRank float64
	W2Latency  float64
	W3Error    float64
}

type RiskService struct {
	logger     *slog.Logger
	bus        ports.EventBus
	graph      ports.GraphProvider
	forecaster ports.Forecaster
	clock      ports.Clock

	weights   Weights
	threshold float64

	mu      sync.Mutex
	history map[string][]domain.Telemetry // key = ServiceNode.ID()
}

func NewRiskService(logger *slog.Logger, bus ports.EventBus, graph ports.GraphProvider, forecaster ports.Forecaster, clock ports.Clock, w Weights, threshold float64) *RiskService {
	return &RiskService{
		logger:     logger,
		bus:        bus,
		graph:      graph,
		forecaster: forecaster,
		clock:      clock,
		weights:    w,
		threshold:  threshold,
		history:    make(map[string][]domain.Telemetry),
	}
}

func (r *RiskService) Start(ctx context.Context) (stop func()) {
	unsub := r.bus.Subscribe(domain.TopicTelemetryUpdated, func(hctx context.Context, payload any) {
		t, ok := payload.(domain.Telemetry)
		if !ok {
			r.logger.Warn("received invalid payload on telemetry channel", "payload_type", fmt.Sprintf("%T", payload))
			return
		}
		if err := r.handleTelemetry(hctx, t); err != nil {
			r.logger.Error("failed to handle telemetry", "error", err, "service", t.Service.ID())
		}
	})
	return unsub
}

func (r *RiskService) handleTelemetry(ctx context.Context, t domain.Telemetry) error {
	svcID := t.Service.ID()

	// r.logger.Debug("processing telemetry", "service", svcID, "latency", t.LatencyP95Ms, "error_rate", t.ErrorRate)

	r.mu.Lock()
	r.history[svcID] = append(r.history[svcID], t)
	hist := append([]domain.Telemetry(nil), r.history[svcID]...)
	if len(hist) > 120 { // keep bounded history
		hist = hist[len(hist)-120:]
		r.history[svcID] = hist
	}
	r.mu.Unlock()

	// Health guard should be enforced by adapter too, but core still handles errors.
	centralityMap, err := r.graph.GetCentrality(ctx)
	if err != nil {
		return fmt.Errorf("getting centrality: %w", err)
	}
	c, ok := centralityMap[svcID]
	if !ok {
		// Missing centrality is a data quality issue; treat as low pagerank.
		// r.logger.Debug("service missing from centrality map, defaulting to 0", "service", svcID)
		c = domain.Centrality{Service: t.Service, PageRank: 0}
	}

	pred, err := r.forecaster.Predict(ctx, t.Service, hist)
	if err != nil {
		return fmt.Errorf("forecasting: %w", err)
	}

	// Normalize/scale before mixing units; if you skip this your score is nonsense.
	pageRank := clamp01(c.PageRank)
	latencyScaled := pred.PredictedLatencyP95Ms / 1000.0 // ms -> seconds-ish scale
	errorRate := clamp01(t.ErrorRate)

	score := (pageRank * r.weights.W1PageRank) +
		(latencyScaled * r.weights.W2Latency) +
		(errorRate * r.weights.W3Error)

	rs := domain.RiskScore{
		Service:   t.Service,
		Score:     score,
		Timestamp: r.clock.Now(),
		Factors: map[string]float64{
			"pagerank":        pageRank,
			"pred_latency_ms": pred.PredictedLatencyP95Ms,
			"error_rate":      errorRate,
		},
	}

	r.logger.Info("risk score computed",
		"service", svcID,
		"score", fmt.Sprintf("%.4f", score),
		"pagerank", fmt.Sprintf("%.4f", pageRank),
		"latency_pred", fmt.Sprintf("%.2f", pred.PredictedLatencyP95Ms),
		"error_rate", fmt.Sprintf("%.4f", errorRate),
	)

	_ = r.bus.Publish(ctx, domain.TopicRiskScoreComputed, rs)

	if score > r.threshold {
		alert := domain.Alert{
			Service:   t.Service,
			Severity:  severityFrom(score, r.threshold),
			Risk:      rs,
			CreatedAt: r.clock.Now(),
			Explanation: fmt.Sprintf(
				"risk=%.4f > threshold=%.4f (pagerank=%.4f latency=%.1fms error=%.4f)",
				score, r.threshold, pageRank, pred.PredictedLatencyP95Ms, errorRate,
			),
		}
		if err := r.bus.Publish(ctx, domain.TopicRiskAlertRaised, alert); err != nil {
			r.logger.Error("failed to publish alert", "error", err)
		} else {
			r.logger.Warn("risk alert raised", "service", svcID, "severity", alert.Severity, "score", score)
		}
	}

	return nil
}

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

func severityFrom(score, threshold float64) domain.Severity {
	if score > threshold*2 {
		return domain.SeverityCritical
	}
	return domain.SeverityWarning
}
