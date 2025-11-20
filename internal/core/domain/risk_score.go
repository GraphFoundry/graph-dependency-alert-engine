package domain

import "time"

type RiskScore struct {
	Service   ServiceNode
	Score     float64
	Factors   map[string]float64
	Timestamp time.Time
}
