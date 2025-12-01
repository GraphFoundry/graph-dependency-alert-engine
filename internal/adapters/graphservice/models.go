package graphservice

type servicesResponse struct {
	Services []serviceDTO `json:"services"`
}

type serviceDTO struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type centralityResponse struct {
	Scores []centralityScoreDTO `json:"scores"`
}

type centralityScoreDTO struct {
	Service     string  `json:"service"`
	PageRank    float64 `json:"pagerank"`
	Betweenness float64 `json:"betweenness"`
}

type peersResponse struct {
	Peers []peerDTO `json:"peers"`
}

type peerDTO struct {
	Service string `json:"service"`
}
