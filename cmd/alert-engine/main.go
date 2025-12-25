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
	"net/http"
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

	// K8s Health Checks
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		// Check dependencies? For now just ok.
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		logger.Info("starting health check server", "addr", ":8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("health server failed", "error", err)
		}
	}()

	logger.Info("alert-engine started", "webhook_targets", len(cfg.WebhookTargets))
	<-ctx.Done()

	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(ctxShutdown); err != nil {
		logger.Error("server shutdown failed", "error", err)
	}

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
				Service:       svc,
				LatencyP95Ms:  lat,
				LatencyP99Ms:  lat * 1.5,
				ErrorRate:     errRate,
				ErrorRate1m:   errRate,       // simplified, assume bursty
				ErrorRate5m:   errRate * 0.8, // simplified smoothing
				ThroughputRPS: 50.0 + r.Float64()*100,
				Timestamp:     now,
			})
		}
	}
}
