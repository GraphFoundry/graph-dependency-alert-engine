package services

import (
	"context"
	"graph-alert-engine/internal/core/domain"
	"graph-alert-engine/internal/core/ports"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

type mockEventBus struct {
	events map[string][]any
}

func (m *mockEventBus) Publish(ctx context.Context, topic string, payload any) error {
	m.events[topic] = append(m.events[topic], payload)
	return nil
}

func (m *mockEventBus) Subscribe(topic string, handler ports.EventHandler) func() {
	return func() {}
}

type mockGraphProvider struct {
	services   []domain.ServiceNode
	centrality map[string]domain.Centrality
}

func (m *mockGraphProvider) GetServices(ctx context.Context) ([]domain.ServiceNode, error) {
	return m.services, nil
}

func (m *mockGraphProvider) GetCentrality(ctx context.Context) (map[string]domain.Centrality, error) {
	return m.centrality, nil
}

func (m *mockGraphProvider) GetHealth(ctx context.Context) (domain.GraphHealth, error) {
	return domain.GraphHealth{}, nil
}

func (m *mockGraphProvider) GetPeers(ctx context.Context, svc domain.ServiceNode, direction domain.Direction, limit int) ([]domain.ServiceNode, error) {
	return []domain.ServiceNode{}, nil
}

func (m *mockGraphProvider) GetNeighborhood(ctx context.Context, svc domain.ServiceNode, k int) ([]domain.ServiceNode, error) {
	return []domain.ServiceNode{}, nil
}

type simpleForecaster struct{}

func (m *simpleForecaster) Predict(ctx context.Context, svc domain.ServiceNode, history []domain.Telemetry) (ports.Prediction, error) {
	return ports.Prediction{PredictedLatencyP95Ms: 100}, nil
}

type testClock struct {
	current time.Time
}

func (c *testClock) Now() time.Time {
	return c.current
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, nil))
}

func TestRiskService_RestorationAlert(t *testing.T) {
	ctx := context.Background()
	bus := &mockEventBus{events: make(map[string][]any)}
	graph := &mockGraphProvider{
		services: []domain.ServiceNode{
			{Name: "test-svc", Namespace: "default", PodCount: 0, Availability: 1.0}, // Initially down
		},
		centrality: map[string]domain.Centrality{
			"default/test-svc": {
				Service:      domain.ServiceNode{Name: "test-svc", Namespace: "default"},
				PageRank:     0.5,
				Betweenness:  0.3,
				BlastRadius:  2,
				PodCount:     0,
				Availability: 1.0,
			},
		},
	}
	forecaster := &simpleForecaster{}
	clock := &testClock{current: time.Date(2026, 1, 4, 10, 0, 0, 0, time.UTC)}

	svc := NewRiskService(
		testLogger(),
		bus,
		graph,
		forecaster,
		clock,
		Weights{W1PageRank: 0.4, W2Latency: 0.3, W3Error: 0.3},
		75.0,
	)

	// First check - service is down
	svc.checkServiceAvailability(ctx)

	// Verify degraded alert was published
	alerts := bus.events[domain.TopicRiskAlertRaised]
	if len(alerts) != 1 {
		t.Fatalf("Expected 1 degraded alert, got %d", len(alerts))
	}
	alert1 := alerts[0].(domain.Alert)
	if alert1.Severity != domain.SeverityCritical {
		t.Errorf("Expected critical severity, got %v", alert1.Severity)
	}
	if alert1.Service.Name != "test-svc" {
		t.Errorf("Expected test-svc, got %s", alert1.Service.Name)
	}
	t.Logf("Degraded alert: %s", alert1.Explanation)

	// Wait and restore service
	clock.current = clock.current.Add(2 * time.Minute)
	graph.services = []domain.ServiceNode{
		{Name: "test-svc", Namespace: "default", PodCount: 3, Availability: 0.95}, // Now healthy
	}
	graph.centrality["default/test-svc"] = domain.Centrality{
		Service:      domain.ServiceNode{Name: "test-svc", Namespace: "default"},
		PageRank:     0.5,
		Betweenness:  0.3,
		BlastRadius:  2,
		PodCount:     3,
		Availability: 0.95,
	}

	// Second check - service is restored
	svc.checkServiceAvailability(ctx)

	// Verify restoration alert was published
	alerts = bus.events[domain.TopicRiskAlertRaised]
	if len(alerts) != 2 {
		t.Fatalf("Expected 2 total alerts (degraded + restored), got %d", len(alerts))
	}
	alert2 := alerts[1].(domain.Alert)
	if alert2.Severity != domain.SeverityInfo {
		t.Errorf("Expected info severity for restoration, got %v", alert2.Severity)
	}
	if alert2.Service.Name != "test-svc" {
		t.Errorf("Expected test-svc, got %s", alert2.Service.Name)
	}
	t.Logf("Restoration alert: %s", alert2.Explanation)

	// Verify the explanation contains downtime
	if alert2.Explanation == "" {
		t.Error("Expected non-empty explanation")
	}
	// Should mention "restored" and include downtime
	if !strings.Contains(alert2.Explanation, "restored") {
		t.Errorf("Expected restoration message, got: %s", alert2.Explanation)
	}

	// Third check - service remains healthy, no new alerts
	clock.current = clock.current.Add(1 * time.Minute)
	svc.checkServiceAvailability(ctx)

	alerts = bus.events[domain.TopicRiskAlertRaised]
	if len(alerts) != 2 {
		t.Fatalf("Expected no new alerts when service remains healthy, got %d total", len(alerts))
	}

	t.Logf("Restoration alert test passed: service went from down -> restored -> healthy")
}

func TestRiskService_RestorationAlert_LowAvailability(t *testing.T) {
	ctx := context.Background()
	bus := &mockEventBus{events: make(map[string][]any)}
	graph := &mockGraphProvider{
		services: []domain.ServiceNode{
			{Name: "degraded-svc", Namespace: "default", PodCount: 2, Availability: 0.6}, // Low availability
		},
	}
	forecaster := &simpleForecaster{}
	clock := &testClock{current: time.Date(2026, 1, 4, 10, 0, 0, 0, time.UTC)}

	svc := NewRiskService(
		testLogger(),
		bus,
		graph,
		forecaster,
		clock,
		Weights{W1PageRank: 0.4, W2Latency: 0.3, W3Error: 0.3},
		75.0,
	)

	// First check - service has low availability
	svc.checkServiceAvailability(ctx)

	alerts := bus.events[domain.TopicRiskAlertRaised]
	if len(alerts) != 1 {
		t.Fatalf("Expected 1 degraded alert, got %d", len(alerts))
	}
	t.Logf("Degraded alert: %s", alerts[0].(domain.Alert).Explanation)

	// Restore availability
	clock.current = clock.current.Add(5 * time.Minute)
	graph.services = []domain.ServiceNode{
		{Name: "degraded-svc", Namespace: "default", PodCount: 2, Availability: 0.95}, // Restored
	}

	// Second check - service restored
	svc.checkServiceAvailability(ctx)

	alerts = bus.events[domain.TopicRiskAlertRaised]
	if len(alerts) != 2 {
		t.Fatalf("Expected 2 alerts (degraded + restored), got %d", len(alerts))
	}

	alert := alerts[1].(domain.Alert)
	if alert.Severity != domain.SeverityInfo {
		t.Errorf("Expected info severity, got %v", alert.Severity)
	}
	t.Logf("Restoration alert: %s", alert.Explanation)
	t.Logf("Downtime captured correctly: service was degraded for 5m")
}
