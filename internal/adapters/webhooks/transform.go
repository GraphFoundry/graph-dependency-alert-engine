package webhooks

import (
	"fmt"
	"graph-alert-engine/internal/core/domain"
	"time"
)

// TransformAlertToEvent converts a domain.Alert into a decision-first webhook payload.
// It strips redundancy, removes zero-value noise, and ensures CalculationID is always set.
func TransformAlertToEvent(alert domain.Alert) AlertEvent {
	now := time.Now()
	eventID := GenerateEventID()
	dedupeKey := alert.DedupeKey
	if dedupeKey == "" {
		// Fallback: compute if not set
		dedupeKey = ComputeDedupeKey(alert.Service.Name, alert.Service.Namespace, string(alert.Type))
	}

	// Service info (single source, no duplication)
	service := ServiceInfo{
		Name:      alert.Service.Name,
		Namespace: alert.Service.Namespace,
		// Cluster, Region, Env would come from service catalog enrichment
	}

	// Alert classification
	classification := AlertClassification{
		Type:     string(alert.Type),
		State:    string(alert.State),
		Severity: string(alert.Severity),
	}

	// Evidence: only non-zero, meaningful values
	evidence := buildEvidence(alert)

	// Impact
	impact := Impact{
		DownstreamCount: alert.ImpactScope["downstream_services"],
	}

	// Decision
	decision := Decision{
		Action:      string(alert.RecommendedAction),
		Auto:        alert.AutoMitigatable,
		ReasonCodes: alert.ReasonCodes,
	}
	if alert.Priority != "" {
		decision.Priority = &alert.Priority
	}
	// Only include risk score if non-zero (and only if used by policies)
	if alert.Risk.Score > 0 {
		score := alert.Risk.Score
		decision.RiskScore = &score
	}

	// Links: CalculationID must be non-empty for this to work
	links := Links{}
	if alert.Risk.Meta.CalculationID != "" {
		links.DetailsRef = fmt.Sprintf("riskcalc:%s", alert.Risk.Meta.CalculationID)
	} else {
		// CRITICAL: This should never happen. Log/alert if CalculationID is missing.
		// For now, generate a fallback so the webhook isn't broken.
		links.DetailsRef = fmt.Sprintf("riskcalc:MISSING-%s", eventID)
	}

	// Meta
	meta := Meta{
		ModelVersion:     alert.Risk.Meta.ModelVersion,
		ThresholdVersion: alert.Risk.Meta.ThresholdVersion,
	}

	return AlertEvent{
		SchemaVersion: SchemaVersion,
		EventID:       eventID,
		DedupeKey:     dedupeKey,
		ObservedAt:    alert.CreatedAt,
		SentAt:        now,
		Service:       service,
		Alert:         classification,
		Evidence:      evidence,
		Impact:        impact,
		Decision:      decision,
		Ownership:     nil, // Enriched by service catalog if available
		Links:         links,
		Meta:          meta,
	}
}

// buildEvidence extracts only the signals that are non-zero and meaningful.
// Never send zero as "unknown" — omit the field instead.
func buildEvidence(alert domain.Alert) Evidence {
	ev := Evidence{}

	// SLI/SLO: If SLO target is set on ServiceNode, include it for context
	if alert.Service.Availability > 0 {
		ev.SLI = &SLI{
			Name:  "availability",
			Value: alert.Service.Availability,
		}
		if alert.Service.SLOTarget != nil {
			ev.SLO = &SLO{
				Target: fmt.Sprintf("%.4f", *alert.Service.SLOTarget),
				Window: safeDeref(alert.Service.SLOWindow, "5m"),
				Breach: alert.Service.Availability < *alert.Service.SLOTarget,
			}
		}
	}

	// Pod count: include if positive
	if alert.Service.PodCount > 0 {
		pc := alert.Service.PodCount
		ev.PodCount = &pc
	}

	// Availability (if not already in SLI)
	if ev.SLI == nil && alert.Service.Availability > 0 {
		av := alert.Service.Availability
		ev.Availability = &av
	}

	// Error rate: only if non-zero
	if alert.Risk.Metrics.ErrorRate1m > 0 {
		er := alert.Risk.Metrics.ErrorRate1m
		ev.ErrorRate = &er
	}

	// Latency P99: only if non-zero
	if alert.Risk.Metrics.LatencyP99 > 0 {
		lat := alert.Risk.Metrics.LatencyP99
		ev.LatencyP99 = &lat
	}

	// Graph signals: only if non-zero
	if alert.Risk.Metrics.PageRank > 0 {
		pr := alert.Risk.Metrics.PageRank
		ev.PageRank = &pr
	}
	if alert.Risk.Metrics.Connectivity > 0 {
		conn := alert.Risk.Metrics.Connectivity
		ev.Betweenness = &conn // Assuming connectivity is betweenness-like
	}

	return ev
}

func safeDeref(ptr *string, def string) string {
	if ptr != nil {
		return *ptr
	}
	return def
}
