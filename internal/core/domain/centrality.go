package domain

import "time"

type Centrality struct {
	Service     ServiceNode
	PageRank    float64 // expected 0..1 (normalize in adapter if needed)
	Betweenness float64
	UpdatedAt   time.Time
}

type Direction string

const (
	DirectionIn   Direction = "in"
	DirectionOut  Direction = "out"
	DirectionBoth Direction = "both"
)

type GraphHealth struct {
	Stale                 bool
	LastUpdatedSecondsAgo *int64
}
