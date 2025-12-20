package domain

import "time"

type RiskScore struct {
	Service    ServiceNode
	Score      float64 // 0-100 normalized
	Severity   Severity
	Components RiskComponents
	Metrics    RiskMetrics
	Trends     RiskTrends
	Meta       RiskMetadata
	Timestamp  time.Time
}

type RiskComponents struct {
	LatencyScore float64
	ErrorScore   float64
	GraphScore   float64
	PeerScore    float64
}

type RiskMetrics struct {
	LatencyP95   float64
	LatencyP99   float64
	ErrorRate1m  float64
	ErrorRate5m  float64
	Throughput   float64
	PageRank     float64
	Connectivity float64
	Upstream     int
	Downstream   int
	Neighborhood int
}

type RiskTrends struct {
	ScoreAvg30s  float64
	ScoreAvg2m   float64
	ScoreDelta1s float64 // Derivative
}

type RiskMetadata struct {
	ModelVersion            string
	ThresholdVersion        string
	CalculationID           string
	GraphStale              bool
	GraphWindowMinutes      int
	GraphLastUpdatedSeconds *int64
}
