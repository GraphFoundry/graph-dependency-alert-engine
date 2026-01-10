package domain

import "errors"

var (
	ErrGraphStale       = errors.New("graph data is stale")
	ErrGraphUnavailable = errors.New("graph service unavailable")
	ErrGraphBadResponse = errors.New("graph service bad response")
)
