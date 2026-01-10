package webhooks

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
)

// SchemaVersion is the current webhook payload schema version
const SchemaVersion = "alerts.v1"

// AlertEvent is the decision-first webhook payload envelope.
// It contains only what's needed for routing, decision-making, and action.
// Full diagnostics are available via links.details_ref if needed.
type AlertEvent struct {
	// Identity
	SchemaVersion string    `json:"schema_version"`
	EventID       string    `json:"event_id"`        // ULID for uniqueness
	DedupeKey     string    `json:"dedupe_key"`      // Stable fingerprint for idempotency
	ObservedAt    time.Time `json:"observed_at"`     // When the issue was detected
	SentAt        time.Time `json:"sent_at"`         // When the webhook was dispatched

	// Target
	Service ServiceInfo `json:"service"`

	// Classification
	Alert AlertClassification `json:"alert"`

	// Evidence (only what drove the decision)
	Evidence Evidence `json:"evidence"`

	// Blast radius
	Impact Impact `json:"impact"`

	// Decision output
	Decision Decision `json:"decision"`

	// Optional: ownership & routing (enriched from service catalog)
	Ownership *Ownership `json:"ownership,omitempty"`

	// Pointers
	Links Links `json:"links,omitempty"`

	// Metadata for debugging/auditing
	Meta Meta `json:"meta"`
}

type ServiceInfo struct {
	Name      string  `json:"name"`
	Namespace string  `json:"namespace"`
	Cluster   *string `json:"cluster,omitempty"`   // Optional: if multi-cluster
	Region    *string `json:"region,omitempty"`    // Optional: cloud region
	Env       *string `json:"env,omitempty"`       // Optional: prod/stage/dev
}

type AlertClassification struct {
	Type     string `json:"type"`     // Machine-readable: service_availability_degraded, service_error_rate_high, etc.
	State    string `json:"state"`    // firing | resolved | acknowledged
	Severity string `json:"severity"` // info | warning | critical
}

// Evidence contains only the signals that triggered this alert plus their thresholds.
// Zero values are omitted to avoid misleading consumers.
type Evidence struct {
	// SLI/SLO for availability/error rate/latency
	SLI *SLI `json:"sli,omitempty"`
	SLO *SLO `json:"slo,omitempty"`

	// Infrastructure signals
	PodCount     *int     `json:"pod_count,omitempty"`
	Availability *float64 `json:"availability,omitempty"` // 0.0-1.0, only if not covered by SLI

	// Observability signals (only if they drove the alert)
	ErrorRate *float64 `json:"error_rate,omitempty"` // 0.0-1.0
	LatencyP99 *float64 `json:"latency_p99_ms,omitempty"`

	// Graph signals (only if they drove the alert)
	PageRank    *float64 `json:"pagerank,omitempty"`
	Betweenness *float64 `json:"betweenness,omitempty"`
}

type SLI struct {
	Name  string  `json:"name"`  // "availability", "error_rate", "latency_p99"
	Value float64 `json:"value"` // Current measured value
}

type SLO struct {
	Target string `json:"target"` // "0.9900", "0.9990"
	Window string `json:"window"` // "5m", "1h"
	Breach bool   `json:"breach"` // true if SLI < SLO target
}

type Impact struct {
	DownstreamCount int     `json:"downstream_count"`
	UserImpact      *string `json:"user_impact,omitempty"`     // Optional: "high", "medium", "low"
	CustomerImpact  *bool   `json:"customer_impact,omitempty"` // Optional: true if customer-facing
}

type Decision struct {
	Action      string   `json:"action"`                // scale_up, page, ticket, noop, rollback, throttle, etc.
	Auto        bool     `json:"auto"`                  // Whether this can be auto-mitigated
	Priority    *string  `json:"priority,omitempty"`    // P0, P1, P2, P3, P4
	RiskScore   *float64 `json:"risk_score,omitempty"`  // 0-100, only if used by policies
	ReasonCodes []string `json:"reason_codes"`          // Machine-readable, stable: AVAILABILITY_LOW, SINGLE_REPLICA, etc.
	Scenario    *string  `json:"scenario,omitempty"`    // Optional: PROD_USER_FACING_SLO_BREACH, etc.
	Route       []string `json:"route,omitempty"`       // Optional: pager, slack, ticket, etc.
}

type Ownership struct {
	Team       *string `json:"team,omitempty"`        // Owner team
	RoutingKey *string `json:"routing_key,omitempty"` // For paging systems
	Tier       *int    `json:"tier,omitempty"`        // 0, 1, 2 (criticality)
}

type Links struct {
	DetailsRef string  `json:"details_ref,omitempty"` // riskcalc:ID or URL to full diagnostics
	Runbook    *string `json:"runbook,omitempty"`
	Dashboard  *string `json:"dashboard,omitempty"`
	Logs       *string `json:"logs,omitempty"`
	Traces     *string `json:"traces,omitempty"`
}

type Meta struct {
	ModelVersion     string `json:"model_version"`
	ThresholdVersion string `json:"threshold_version,omitempty"`
}

// GenerateEventID creates a new ULID for the event.
func GenerateEventID() string {
	return ulid.Make().String()
}

// ComputeDedupeKey creates a stable fingerprint for deduplication.
// Format: sha256(service_name:namespace:alert_type:dimension)
func ComputeDedupeKey(serviceName, namespace, alertType string) string {
	raw := fmt.Sprintf("%s:%s:%s", serviceName, namespace, alertType)
	hash := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(hash[:])[:16] // First 16 hex chars
}
