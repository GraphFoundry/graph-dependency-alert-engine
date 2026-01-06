package graphservice

import "encoding/json"

type servicesResponse struct {
	Services []serviceDTO `json:"services"`
}

type healthResponse struct {
	Status                string  `json:"status"`
	Stale                 bool    `json:"stale"`
	LastUpdatedSecondsAgo *int64  `json:"lastUpdatedSecondsAgo"`
	WindowMinutes         int     `json:"windowMinutes"`
	Message               *string `json:"message,omitempty"`
}

type serviceDTO struct {
	Name         string          `json:"name"`
	Namespace    string          `json:"namespace"`
	PodCount     intOrObject     `json:"podCount"`
	Availability float64         `json:"availability"`
}

// intOrObject handles cases where the API returns either an int or an object/null
type intOrObject int

func (i *intOrObject) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as int first
	var v int
	if err := json.Unmarshal(data, &v); err == nil {
		*i = intOrObject(v)
		return nil
	}
	
	// If it's an object or null, default to 0
	*i = 0
	return nil
}

type centralityResponse struct {
	Scores []centralityScoreDTO `json:"scores"`
}

type centralityScoreDTO struct {
	Service          string  `json:"service"`
	PageRank         float64 `json:"pagerank"`
	Betweenness      float64 `json:"betweenness"`
	BlastRadius      float64 `json:"blast_radius"`
	DownstreamCount  int     `json:"downstream_count"`
	ErrorPropagation float64 `json:"error_propagation"`
	PodCount         int     `json:"podCount"`
	Availability     float64 `json:"availability"`
}

type peersResponse struct {
	Peers []peerDTO `json:"peers"`
}

type peerDTO struct {
	Service string `json:"service"`
}

type neighborhoodResponse struct {
	Nodes []string `json:"nodes"`
}
