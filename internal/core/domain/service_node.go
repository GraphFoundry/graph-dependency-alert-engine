package domain

import (
	"net/url"
)

type ServiceNode struct {
	Name         string
	Namespace    string
	PodCount     int
	Availability float64

	// SLO context (optional, for meaningful threshold interpretation)
	SLOTarget *float64 // e.g., 0.9900
	SLOWindow *string  // e.g., "5m", "1h"
}

func (s ServiceNode) ID() string {
	if s.Namespace == "" {
		return s.Name
	}
	return s.Namespace + "/" + s.Name
}

func (s ServiceNode) PathEscapeID() string {
	return url.PathEscape(s.ID())
}
