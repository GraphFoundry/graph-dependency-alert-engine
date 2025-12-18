package domain

import "time"

type Centrality struct {
	Service          ServiceNode
	PageRank         float64 // expected 0..1 (normalize in adapter if needed)
	Betweenness      float64
	BlastRadius      float64 // normalized score 0-100 indicating downstream impact
	DownstreamCount  int
	ErrorPropagation float64 // likelihood of propagating errors 0-1
	UpdatedAt        time.Time
}

type Direction string

const (
	DirectionIn   Direction = "in"
	DirectionOut  Direction = "out"
	DirectionBoth Direction = "both"
)

type GraphHealth struct {
	Status                string
	Stale                 bool
	LastUpdatedSecondsAgo *int64
	WindowMinutes         int
}
