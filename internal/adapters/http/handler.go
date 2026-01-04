package api

import (
	"encoding/json"
	"graph-alert-engine/internal/adapters/graphservice"
	"graph-alert-engine/internal/core/services"
	"net/http"
	"time"
)

type Handler struct {
	riskService *services.RiskService
	poller      *graphservice.GraphPoller
}

func NewHandler(rs *services.RiskService, gp *graphservice.GraphPoller, _ interface{}) *Handler {
	return &Handler{
		riskService: rs,
		poller:      gp,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/alerts/enriched", h.handleEnrichedAlerts)
	mux.HandleFunc("/api/services/", h.handleServiceRiskProfile) // Matches /api/services/{id}/risk-profile
	mux.HandleFunc("/api/risk/dashboard", h.handleRiskDashboard)
	mux.HandleFunc("/api/forecasts/risk-horizon", h.handleRiskHorizon)
}

func (h *Handler) handleEnrichedAlerts(w http.ResponseWriter, r *http.Request) {
	alerts := h.riskService.GetRecentAlerts()

	// Wrap in response structure
	resp := map[string]interface{}{
		"alerts": alerts,
		"metadata": map[string]interface{}{
			"totalAlerts": len(alerts),
			"timestamp":   time.Now(),
		},
	}

	writeJSON(w, resp)
}

func (h *Handler) handleServiceRiskProfile(w http.ResponseWriter, r *http.Request) {
	// Simple path parsing /api/services/{serviceID}/risk-profile
	// Current implementation assumes serviceID might be 'service' or 'namespace/service'
	// URL pattern: /api/services/{parts...}
	// Let's strip prefix
	path := r.URL.Path[len("/api/services/"):]
	// expect {id}/risk-profile
	if len(path) < len("/risk-profile") {
		http.NotFound(w, r)
		return
	}
	id := path[:len(path)-len("/risk-profile")]

	rs, ok := h.riskService.GetRiskProfile(id)
	if !ok {
		// Try decoding if it was URL encoded?
		// For now simple match.
		http.Error(w, "Service not found or no risk score yet", http.StatusNotFound)
		return
	}

	writeJSON(w, rs)
}

func (h *Handler) handleRiskDashboard(w http.ResponseWriter, r *http.Request) {
	riskScores := h.riskService.GetAllServicesRisk()
	alerts := h.riskService.GetRecentAlerts()
	health, _ := h.poller.GetHealth(r.Context())

	// Simple aggregation
	highRisk := 0
	for _, s := range riskScores {
		if s.Score > 50 { // arbitrary threshold for dashboard count
			highRisk++
		}
	}

	resp := map[string]interface{}{
		"timestamp": time.Now(),
		"systemHealth": map[string]interface{}{
			"graphFreshness": health,
			"totalServices":  len(riskScores),
			"servicesAtRisk": highRisk,
			"activeAlerts":   len(alerts),
		},
		"topRiskyServices": riskScores, // simplified for now, UI can sort
	}
	writeJSON(w, resp)
}

func (h *Handler) handleRiskHorizon(w http.ResponseWriter, r *http.Request) {
	// As discussed, RiskService calculates score based on prediction.
	// We return the current risk map as the horizon forecast.
	riskScores := h.riskService.GetAllServicesRisk()

	health, _ := h.poller.GetHealth(r.Context())

	resp := map[string]interface{}{
		"forecastWindow": map[string]interface{}{
			"horizonMinutes": 5, // Default for our model
			"generatedAt":    time.Now(),
		},
		"predictions": riskScores,
		"confidence": map[string]interface{}{
			"graphFreshness": !health.Stale,
		},
	}
	writeJSON(w, resp)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
