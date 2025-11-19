package domain

import "time"

type Telemetry struct {
	Service      ServiceNode
	LatencyP95Ms float64
	ErrorRate    float64 // 0..1
	Timestamp    time.Time
}
