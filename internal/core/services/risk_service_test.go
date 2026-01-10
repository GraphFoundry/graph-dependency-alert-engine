package services

import (
	"context"
	"graph-alert-engine/internal/core/domain"
	"graph-alert-engine/internal/core/ports"
	"log/slog"
	"math"
	"os"
	"testing"
	"time"
)

// Mocks
type mockBus struct{}

func (m *mockBus) Publish(ctx context.Context, topic string, payload any) error { return nil }
func (m *mockBus) Subscribe(topic string, handler ports.EventHandler) func() {
	return func() {}
}

type mockGraph struct {
	centrality map[string]domain.Centrality
	health     domain.GraphHealth

	// Overrides
	GetPeersFunc func(ctx context.Context, svc domain.ServiceNode, direction domain.Direction, limit int) ([]domain.ServiceNode, error)
}

func (m *mockGraph) GetServices(ctx context.Context) ([]domain.ServiceNode, error) { return nil, nil }
func (m *mockGraph) GetCentrality(ctx context.Context) (map[string]domain.Centrality, error) {
	return m.centrality, nil
}
func (m *mockGraph) GetHealth(ctx context.Context) (domain.GraphHealth, error) { return m.health, nil }
func (m *mockGraph) GetPeers(ctx context.Context, svc domain.ServiceNode, direction domain.Direction, limit int) ([]domain.ServiceNode, error) {
	if m.GetPeersFunc != nil {
		return m.GetPeersFunc(ctx, svc, direction, limit)
	}
	// default mock output: 10 peers if limit > 0
	if limit > 0 {
		peers := make([]domain.ServiceNode, 10)
		for i := 0; i < 10; i++ {
			peers[i] = domain.ServiceNode{Name: "peer"}
		}
		return peers, nil
	}
	return nil, nil
}
func (m *mockGraph) GetNeighborhood(ctx context.Context, svc domain.ServiceNode, k int) ([]domain.ServiceNode, error) {
	return nil, nil
}

type mockForecaster struct{}

func (m *mockForecaster) Predict(ctx context.Context, svc domain.ServiceNode, history []domain.Telemetry) (ports.Prediction, error) {
	return ports.Prediction{PredictedLatencyP95Ms: 100}, nil // Low latency risk
}

type mockClock struct{}

func (m *mockClock) Now() time.Time { return time.Now() }

func TestRiskService_ScoringFormula(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	bus := &mockBus{}

	// Setup Graph Mock with specific centrality
	mg := &mockGraph{
		centrality: map[string]domain.Centrality{
			"default/test-svc": {
				Service:     domain.ServiceNode{Name: "test-svc", Namespace: "default"},
				PageRank:    0.5,
				Betweenness: 0.2,
			},
		},
	}

	rs := NewRiskService(
		logger,
		bus,
		mg,
		&mockForecaster{},
		&mockClock{},
		Weights{W1PageRank: 1.0, W2Latency: 0.0, W3Error: 0.0}, // Isolate Graph Score
		50.0,
	)

	svc := domain.ServiceNode{Name: "test-svc", Namespace: "default"}
	telemetry := domain.Telemetry{
		Service:      svc,
		Timestamp:    time.Now(),
		LatencyP95Ms: 100,
		ErrorRate5m:  0,
	}

	// We need to bypass Start() / bus sub and call handleTelemetry directly if possible,
	// but handleTelemetry is private.
	// Since we are in the same package 'services', checking if we can export or test internally.
	// The file `risk_service.go` is package services. This test file is package services.
	// So we can access private methods.

	err := rs.handleTelemetry(context.Background(), telemetry)
	if err != nil {
		t.Fatalf("handleTelemetry failed: %v", err)
	}

	// Retrieve score via Public Accessor
	score, ok := rs.GetRiskProfile(svc.ID())
	if !ok {
		t.Fatalf("Risk score not found for %s", svc.ID())
	}

	// Verify Formula:
	// PageRank * 0.6 + Betweenness * 0.3 + PeerRisk * 0.1
	// PR = 0.5, Bet = 0.2
	// Peers: mock returns 10 upstream (In) and 10 downstream (Out) for the call in handleTelemetry.
	// PeerRisk = downstream * ln(upstream + 1) = 10 * ln(11) = 10 * 2.397 = 23.97
	// Formula Components:
	// T1 = 0.5 * 0.6 = 0.3
	// T2 = 0.2 * 0.3 = 0.06
	// T3 = 23.97 * 0.1 = 2.397
	// Sum = 0.3 + 0.06 + 2.397 = 2.757
	// Clamp01(Sum) = 1.0 (since > 1)
	// GraphImpact = 1.0 * 100 = 100
	//
	// Latency Risk = (100 / 2000)*100 = 5. (But we set weight 0).
	// Total Risk = GraphImpact * W1(1.0) = 100.

	if score.Components.GraphScore != 100 {
		t.Errorf("Expected GraphScore 100 (clamped), got %f", score.Components.GraphScore)
	}

	// Let's try with 0 peers to verify sensitivity
	mg.GetPeersFunc = func(ctx context.Context, svc domain.ServiceNode, direction domain.Direction, limit int) ([]domain.ServiceNode, error) {
		return []domain.ServiceNode{}, nil
	}
	// Re-run
	rs.handleTelemetry(context.Background(), telemetry)
	score, _ = rs.GetRiskProfile(svc.ID())

	// PeerRisk = 0
	// T1 = 0.3
	// T2 = 0.06
	// Sum = 0.36
	// GraphImpact = 36.

	// allow float tolerance
	if math.Abs(score.Components.GraphScore-36.0) > 0.1 {
		t.Errorf("Expected GraphScore ~36.0, got %f", score.Components.GraphScore)
	}
}

func TestRiskService_GraphDown_HealthyTelemetry(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	bus := &mockBus{}

	// Graph Down (Return Empty/Error)
	mg := &mockGraph{
		centrality: make(map[string]domain.Centrality), // Empty
	}
	// Error on peers
	mg.GetPeersFunc = func(ctx context.Context, s domain.ServiceNode, d domain.Direction, l int) ([]domain.ServiceNode, error) {
		return nil, os.ErrNotExist
	}

	rs := NewRiskService(
		logger,
		bus,
		mg,
		&mockForecaster{},
		&mockClock{},
		// Standard weights
		Weights{W1PageRank: 1.0, W2Latency: 1.0, W3Error: 1.0},
		50.0,
	)

	svc := domain.ServiceNode{Name: "safe-svc", Namespace: "default"}
	// Healthy Telemetry
	telemetry := domain.Telemetry{
		Service:      svc,
		Timestamp:    time.Now(),
		LatencyP95Ms: 20, // Low
		ErrorRate5m:  0,  // Low
	}

	rs.handleTelemetry(context.Background(), telemetry)
	score, ok := rs.GetRiskProfile(svc.ID())

	if !ok {
		t.Fatal("Risk score calculation failed")
	}

	// Graph Score should be small (PageRank 0.001 * 0.6 * 100 =~ 0.06)
	// Latency Score: 20ms / 2000 * 100 = 1.0
	// Error Score: 0
	// Total < 5

	if score.Score > 10.0 {
		t.Errorf("Expected low risk score for healthy service even if graph down, got %f. Components: %+v", score.Score, score.Components)
	}
}
