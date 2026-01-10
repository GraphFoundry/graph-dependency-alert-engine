package domain

import "time"

type AlertState string

const (
	AlertStateFiring       AlertState = "firing"
	AlertStateResolved     AlertState = "resolved"
	AlertStateAcknowledged AlertState = "acknowledged"
)

type AlertType string

const (
	AlertTypeAvailabilityDegraded AlertType = "service_availability_degraded"
	AlertTypeErrorRateHigh        AlertType = "service_error_rate_high"
	AlertTypeLatencyHigh          AlertType = "service_latency_high"
	AlertTypeSingleReplica        AlertType = "service_single_replica"
	AlertTypeGraphCentralityHigh  AlertType = "service_centrality_high"
)

type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

type Alert struct {
	Service     ServiceNode
	Severity    Severity
	State       AlertState // firing | resolved | acknowledged
	Type        AlertType  // Machine-readable classification
	Risk        RiskScore
	Explanation string

	// Actionability
	RecommendedAction MitigationAction
	ImpactScope       map[string]int // e.g., "downstream_services": 5
	AutoMitigatable   bool
	Priority          string   // P0, P1, P2, P3, P4
	ReasonCodes       []string // Machine-readable reason codes

	// Identity
	DedupeKey string // Stable fingerprint for idempotency

	CreatedAt time.Time
}
