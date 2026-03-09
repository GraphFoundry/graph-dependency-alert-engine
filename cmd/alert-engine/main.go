package main

import (
	"context"
	"graph-alert-engine/config"
	"graph-alert-engine/internal/adapters/eventbus"
	"graph-alert-engine/internal/adapters/forecasting"
	"graph-alert-engine/internal/adapters/graphservice"
	api "graph-alert-engine/internal/adapters/http"
	"graph-alert-engine/internal/adapters/slack"
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
	graphClient := graphservice.New(cfg.GraphBaseURL, cfg.GraphTimeout, cfg.GraphRetries, cb, cfg.CentralityCacheTTL)

	// Wrap in Poller
	graphPoller := graphservice.NewGraphPoller(logger, graphClient, 5*time.Second)
	stopPoller := graphPoller.Start(ctx)
	defer stopPoller()

	forecaster := forecasting.New()

	var clock ports.Clock = ports.RealClock{}

	rs := services.NewRiskService(
		logger,
		bus,
		graphPoller, // Use Poller as Provider
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

	// Slack notifications
	if cfg.SlackWebhookURL != "" {
		slackNotifier := slack.New(logger, bus, cfg.SlackWebhookURL)
		stopSlack := slackNotifier.Start(ctx)
		defer stopSlack()
		logger.Info("slack notifier enabled")
	}

	// Telemetry simulator (replace with K8s informer later)
	go simulateTelemetry(ctx, bus)

	// API Handler
	apiHandler := api.NewHandler(rs, graphPoller, nil)

	// K8s Health Checks & API
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

	// Register API Routes
	apiHandler.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:    ":8002",
		Handler: mux,
	}

	go func() {
		logger.Info("starting server", "addr", ":8002")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
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
			// Healthy by default
			lat := 20.0 + r.Float64()*10.0 // 20-30ms latency
			if r.Float64() < 0.01 {        // 1% chance of spike
				lat *= 5 // ~100-150ms (still not critical)
			}
			errRate := 0.0
			if r.Float64() < 0.005 { // 0.5% chance of error burst
				errRate = 0.01 // 1% error rate
			}
			_ = bus.Publish(ctx, domain.TopicTelemetryUpdated, domain.Telemetry{
				Service:       svc,
				LatencyP95Ms:  lat,
				LatencyP99Ms:  lat * 1.5,
				ErrorRate:     errRate,
				ErrorRate1m:   errRate,
				ErrorRate5m:   errRate,
				ThroughputRPS: 50.0 + r.Float64()*10,
				Timestamp:     now,
			})
		}
	}
}
