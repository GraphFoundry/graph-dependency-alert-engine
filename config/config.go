package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	GraphBaseURL       string
	GraphTimeout       time.Duration
	GraphRetries       int
	CentralityCacheTTL time.Duration

	RiskW1        float64
	RiskW2        float64
	RiskW3        float64
	RiskThreshold float64

	WebhookTargets []string
	WebhookSecret  []byte

	// Slack incoming webhook
	SlackWebhookURL string

	// Optional: Service catalog enrichment for webhooks
	ClusterName string // CLUSTER_NAME
	Region      string // REGION
	Environment string // ENVIRONMENT (prod/stage/dev)
}

func Load() (Config, error) {
	var c Config

	c.GraphBaseURL = mustEnv("GRAPH_BASE_URL")
	c.GraphTimeout = durMs(getEnv("GRAPH_TIMEOUT_MS", "2000"))
	c.GraphRetries = intEnv("GRAPH_RETRY_MAX", 2)
	c.CentralityCacheTTL = durMs(getEnv("CENTRALITY_CACHE_TTL_MS", "30000"))

	c.RiskW1 = floatEnv("RISK_W1", 1.0)
	c.RiskW2 = floatEnv("RISK_W2", 1.0)
	c.RiskW3 = floatEnv("RISK_W3", 1.0)
	c.RiskThreshold = floatEnv("RISK_THRESHOLD", 60.0)

	targets := strings.TrimSpace(getEnv("WEBHOOK_TARGET_URLS", ""))
	if targets != "" {
		c.WebhookTargets = strings.Split(targets, ",")
	}
	c.WebhookSecret = []byte(getEnv("WEBHOOK_SECRET", ""))

	c.SlackWebhookURL = getEnv("SLACK_WEBHOOK_URL", "")

	// Optional enrichment fields for webhooks
	c.ClusterName = getEnv("CLUSTER_NAME", "LIONS-DEN")
	c.Region = getEnv("REGION", "LK")
	c.Environment = getEnv("ENVIRONMENT", "DEBUG")

	if c.GraphBaseURL == "" {
		return Config{}, errors.New("GRAPH_BASE_URL required")
	}
	return c, nil
}

func mustEnv(k string) string {
	return strings.TrimSpace(os.Getenv(k))
}

func getEnv(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}

func intEnv(k string, def int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func floatEnv(k string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func durMs(v string) time.Duration {
	i, err := strconv.Atoi(v)
	if err != nil {
		i = 2000
	}
	return time.Duration(i) * time.Millisecond
}
