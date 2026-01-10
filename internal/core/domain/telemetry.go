package domain

import "time"

type Telemetry struct {
	Service       ServiceNode
	LatencyP95Ms  float64
	LatencyP99Ms  float64
	ErrorRate     float64 // instant/short-term
	ErrorRate1m   float64
	ErrorRate5m   float64
	ThroughputRPS float64
	Timestamp     time.Time
}
