package services

import (
	"fmt"
	"graph-alert-engine/internal/adapters/webhooks"
	"graph-alert-engine/internal/core/domain"
)

// enrichAlert populates the new Alert fields required for decision-first webhooks.
// It ensures:
// - State is always set (defaults to firing)
// - Type is determined from context
// - DedupeKey is computed
// - ReasonCodes are generated from context
// - Priority is set based on severity
// - CalculationID is always non-empty
func enrichAlert(alert domain.Alert, alertType domain.AlertType, reasonCodes []string) domain.Alert {
	// State: default to firing (resolved alerts are handled separately)
	if alert.State == "" {
		alert.State = domain.AlertStateFiring
	}

	// Type: set from parameter
	alert.Type = alertType

	// DedupeKey: compute stable fingerprint
	alert.DedupeKey = webhooks.ComputeDedupeKey(
		alert.Service.Name,
		alert.Service.Namespace,
		string(alertType),
	)

	// ReasonCodes: set from parameter
	alert.ReasonCodes = reasonCodes

	// Priority: map from severity
	alert.Priority = mapSeverityToPriority(alert.Severity)

	// CalculationID: ensure always set (critical for details_ref)
	if alert.Risk.Meta.CalculationID == "" {
		// Generate fallback ID if not set by model
		alert.Risk.Meta.CalculationID = fmt.Sprintf("calc-%s", webhooks.GenerateEventID())
	}

	return alert
}

func mapSeverityToPriority(sev domain.Severity) string {
	switch sev {
	case domain.SeverityCritical:
		return "P1"
	case domain.SeverityWarning:
		return "P2"
	case domain.SeverityInfo:
		return "P3"
	default:
		return "P3"
	}
}

// determineReasonCodes generates machine-readable reason codes from alert context.
func determineAvailabilityReasonCodes(podCount int, availability float64) []string {
	codes := []string{}
	
	if podCount == 0 {
		codes = append(codes, "POD_COUNT_ZERO")
	} else if podCount == 1 {
		codes = append(codes, "SINGLE_REPLICA")
	}
	
	if availability < 0.5 {
		codes = append(codes, "AVAILABILITY_CRITICAL")
	} else if availability < 0.8 {
		codes = append(codes, "AVAILABILITY_LOW")
	}
	
	return codes
}

func determineRiskReasonCodes(latScore, errScore, graphImpact float64, health domain.GraphHealth) []string {
	codes := []string{}
	
	if latScore > 70 {
		codes = append(codes, "LATENCY_HIGH")
	}
	if errScore > 70 {
		codes = append(codes, "ERROR_RATE_HIGH")
	}
	if graphImpact > 70 {
		codes = append(codes, "GRAPH_CENTRALITY_HIGH")
	}
	if health.Stale {
		codes = append(codes, "GRAPH_STALE")
	}
	
	return codes
}
