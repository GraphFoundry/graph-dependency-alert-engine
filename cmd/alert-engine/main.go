package main

import (
	"context"
	"graph-alert-engine/config"
	"graph-alert-engine/internal/adapters/eventbus"
	"graph-alert-engine/internal/adapters/forecasting"
	"graph-alert-engine/internal/adapters/graphservice"
	"graph-alert-engine/internal/adapters/webhooks"
	"graph-alert-engine/internal/core/domain"
	"graph-alert-engine/internal/core/ports"
	"graph-alert-engine/internal/core/services"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"time"
)

func main() {
	// Initialize structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug, // verbose for now
	}))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("config error", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	bus := eventbus.New()

	cb := graphservice.NewCircuitBreaker(3, 5*time.Second)
	graph := graphservice.New(cfg.GraphBaseURL, cfg.GraphTimeout, cfg.GraphRetries, cb, cfg.CentralityCacheTTL)

	forecaster := forecasting.New()

	var clock ports.Clock = ports.RealClock{}

	rs := services.NewRiskService(
		logger,
		bus,
		graph,
		forecaster,
		clock,
		services.Weights{W1PageRank: cfg.RiskW1, W2Latency: cfg.RiskW2, W3Error: cfg.RiskW3},
		cfg.RiskThreshold,
	)

	stopRisk := rs.Start(ctx)
	defer stopRisk()

	wh := webhooks.NewOutbound(bus, cfg.WebhookTargets, cfg.WebhookSecret)
	stopWH := wh.Start(ctx)
	defer stopWH()

	// Telemetry simulator (replace with K8s informer later)
	go simulateTelemetry(ctx, bus)

	logger.Info("alert-engine started", "webhook_targets", len(cfg.WebhookTargets))
	<-ctx.Done()
	logger.Info("shutdown")
}

func simulateTelemetry(ctx context.Context, bus ports.EventBus) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	svc := domain.ServiceNode{Name: "payments", Namespace: "prod"}

	t := time.NewTicker(1 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			lat := 120.0 + r.Float64()*400.0
			if r.Float64() < 0.15 {
				lat *= 3 // spike
			}
			errRate := r.Float64() * 0.05
			if r.Float64() < 0.1 {
				errRate += 0.2
			}
			_ = bus.Publish(ctx, domain.TopicTelemetryUpdated, domain.Telemetry{
				Service:      svc,
				LatencyP95Ms: lat,
				ErrorRate:    errRate,
				Timestamp:    now,
			})
		}
	}
}
