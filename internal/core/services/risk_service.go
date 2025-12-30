package services

import (
	"context"
	"fmt"
	"graph-alert-engine/internal/core/domain"
	"graph-alert-engine/internal/core/ports"
	"log/slog"
	"math"
	"sync"
	"time"
)

type Weights struct {
	W1PageRank float64
	W2Latency  float64
	W3Error    float64
}

type RiskService struct {
	logger     *slog.Logger
	bus        ports.EventBus
	graph      ports.GraphProvider
	forecaster ports.Forecaster
	clock      ports.Clock

	weights   Weights
	threshold float64

	mu             sync.Mutex
	history        map[string][]domain.Telemetry // key = ServiceNode.ID()
	scoreHistory   map[string][]scoreRecord      // Internal struct for rolling avg
	recentAlerts   []domain.Alert
	lastRiskScores map[string]domain.RiskScore // Cache latest risk score per service

	cooldowns           map[string]time.Time
	consecutiveBreaches map[string]int
	healthyStreak       map[string]int

	knownServices   map[string]domain.ServiceNode
	seenTelemetry   map[string]bool
	lastSeen        map[string]time.Time
	offlineActive   map[string]bool
	offlineCooldown map[string]time.Time
	recoveryTracker map[string]int
	seenByName      map[string]bool
	lastSeenByName  map[string]time.Time
	knownByName     map[string]domain.ServiceNode
	allowedNS       map[string]struct{}

	servicePollInterval time.Duration
	livenessInterval    time.Duration
	livenessTimeout     time.Duration
	recoverySamples     int
}

type scoreRecord struct {
	Timestamp time.Time
	Score     float64
}

func NewRiskService(logger *slog.Logger, bus ports.EventBus, graph ports.GraphProvider, forecaster ports.Forecaster, clock ports.Clock, w Weights, threshold float64) *RiskService {
	return &RiskService{
		logger:              logger,
		bus:                 bus,
		graph:               graph,
		forecaster:          forecaster,
		clock:               clock,
		weights:             w,
		threshold:           threshold,
		history:             make(map[string][]domain.Telemetry),
		scoreHistory:        make(map[string][]scoreRecord),
		recentAlerts:        make([]domain.Alert, 0),
		lastRiskScores:      make(map[string]domain.RiskScore),
		cooldowns:           make(map[string]time.Time),
		consecutiveBreaches: make(map[string]int),
		healthyStreak:       make(map[string]int),
		knownServices:       make(map[string]domain.ServiceNode),
		seenTelemetry:       make(map[string]bool),
		lastSeen:            make(map[string]time.Time),
		offlineActive:       make(map[string]bool),
		offlineCooldown:     make(map[string]time.Time),
		recoveryTracker:     make(map[string]int),
		seenByName:          make(map[string]bool),
		lastSeenByName:      make(map[string]time.Time),
		knownByName:         make(map[string]domain.ServiceNode),
		allowedNS:           map[string]struct{}{"default": {}},
		servicePollInterval: 30 * time.Second,
		livenessInterval:    10 * time.Second,
		livenessTimeout:     45 * time.Second,
		recoverySamples:     3,
	}
}

// Public Accessors for API

func (r *RiskService) GetRecentAlerts() []domain.Alert {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Return copy
	out := make([]domain.Alert, len(r.recentAlerts))
	copy(out, r.recentAlerts)
	return out
}

func (r *RiskService) GetRiskProfile(serviceID string) (domain.RiskScore, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rs, ok := r.lastRiskScores[serviceID]
	return rs, ok
}

func (r *RiskService) GetAllServicesRisk() []domain.RiskScore {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.RiskScore, 0, len(r.lastRiskScores))
	for _, rs := range r.lastRiskScores {
		out = append(out, rs)
	}
	return out
}

func (r *RiskService) Start(ctx context.Context) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)

	unsub := r.bus.Subscribe(domain.TopicTelemetryUpdated, func(hctx context.Context, payload any) {
		t, ok := payload.(domain.Telemetry)
		if !ok {
			r.logger.Warn("received invalid payload on telemetry channel", "payload_type", fmt.Sprintf("%T", payload))
			return
		}
		if err := r.handleTelemetry(hctx, t); err != nil {
			r.logger.Error("failed to handle telemetry", "error", err, "service", t.Service.ID())
		}
	})

	go r.watchServices(ctx)

	return func() {
		cancel()
		unsub()
	}
}

func (r *RiskService) handleTelemetry(ctx context.Context, t domain.Telemetry) error {
	// Normalize service to known namespace (fallback to same-name match).
	t.Service = r.canonicalizeService(t.Service)
	svcID := t.Service.ID()

	r.mu.Lock()
	r.history[svcID] = append(r.history[svcID], t)
	hist := r.history[svcID]
	r.lastSeen[svcID] = t.Timestamp
	r.knownServices[svcID] = t.Service
	r.seenTelemetry[svcID] = true
	r.seenByName[t.Service.Name] = true
	if prev, ok := r.lastSeenByName[t.Service.Name]; !ok || t.Timestamp.After(prev) {
		r.lastSeenByName[t.Service.Name] = t.Timestamp
	}
	if r.offlineActive[svcID] {
		r.recoveryTracker[svcID]++
		if r.recoveryTracker[svcID] >= r.recoverySamples {
			delete(r.offlineActive, svcID)
			r.recoveryTracker[svcID] = 0
			r.logger.Info("service recovered from silence", "service", svcID)
		}
	} else {
		r.recoveryTracker[svcID] = 0
	}

	// Prune history (keep last ~5 minutes assuming 1s interval, safe upper bound 600)
	if len(hist) > 300 {
		hist = hist[len(hist)-300:]
		r.history[svcID] = hist
	}
	r.mu.Unlock()

	// 1. Get Graph Signals
	var health domain.GraphHealth
	if h, err := r.graph.GetHealth(ctx); err == nil {
		health = h
	} else {
		r.logger.Warn("graph health fetch failed; treating as stale", "error", err)
		health = domain.GraphHealth{Stale: true}
	}

	centralityMap, err := r.graph.GetCentrality(ctx)
	// If centrality fails, we can proceed with defaults, but log it.
	if err != nil {
		r.logger.Warn("getting centrality failed", "error", err)
		centralityMap = make(map[string]domain.Centrality)
	}

	c, ok := centralityMap[svcID]
	if !ok {
		// Try fallback lookups
		if alt, ok := centralityMap[t.Service.Name]; ok {
			c = alt
		} else if alt, ok := centralityMap["default/"+t.Service.Name]; ok {
			c = alt
		}
	}
	if !ok {
		// Default to low centrality
		c = domain.Centrality{Service: t.Service, PageRank: 0.001}
	}

	// Strictly verify service existence in known roster before querying detailed graph (peers/neighborhood).
	// This ensures we follow the API "Chain": List Services -> Get Details.
	// We check r.knownServices (or by name) which is populated by refreshServiceRoster loop.
	r.mu.Lock()
	_, knownID := r.knownServices[svcID]
	_, knownName := r.knownByName[t.Service.Name]
	r.mu.Unlock()

	var upPeers, downPeers []domain.ServiceNode
	var neighborhoodSize int

	if knownID || knownName {
		// Service is confirmed in list, safe to query details
		if up, err := r.graph.GetPeers(ctx, t.Service, domain.DirectionIn, 15); err == nil {
			upPeers = up
		} else {
			r.logger.Warn("failed to fetch upstream peers", "service", svcID, "error", err)
		}

		if down, err := r.graph.GetPeers(ctx, t.Service, domain.DirectionOut, 15); err == nil {
			downPeers = down
		} else {
			r.logger.Warn("failed to fetch downstream peers", "service", svcID, "error", err)
		}
	} else {
		// Unknown service - skip graph details to prevent 404 floods or invalid queries
		r.logger.Debug("skipping graph details for unknown service", "service", svcID)
	}

	upCount := len(upPeers)
	downCount := len(downPeers)

	neighborhoodSize = 0
	// Only ask for neighborhood when the service is structurally important to avoid hammering graph service (and only if confirmed known)
	if (knownID || knownName) && (c.PageRank >= 0.2 || downCount >= 5 || c.DownstreamCount >= 5) {
		if neigh, err := r.graph.GetNeighborhood(ctx, t.Service, 2); err == nil {
			neighborhoodSize = len(neigh)
		} else {
			r.logger.Debug("neighborhood fetch failed", "service", svcID, "error", err)
		}
	}

	// 2. Forecasting
	pred, err := r.forecaster.Predict(ctx, t.Service, hist)
	if err != nil {
		return fmt.Errorf("forecasting: %w", err)
	}

	// 3. Components Calculation (Normalization)
	// Latency: 0-100 score. Assume >2000ms is failure (100 risk).
	latScore := (pred.PredictedLatencyP95Ms / 2000.0) * 100
	if latScore > 100 {
		latScore = 100
	}

	// Error: 0-100 score. Assume >5% error rate is failure (100 risk).
	errScore := (t.ErrorRate5m / 0.05) * 100
	if errScore > 100 {
		errScore = 100
	}

	// Graph: Composite Centrality Score
	// Formula: Risk = PageRank*0.6 + Betweenness*0.3 + PeerRisk*0.1
	// Peer Risk = downstream * ln(upstream + 1)
	peerRisk := float64(downCount) * math.Log(float64(upCount)+1)

	// Normalize PeerRisk roughly to 0-1 range for the formula mixing.
	// Assuming max reasonable peer interaction is e.g. 10 downstream, 10 upstream.
	// 10 * ln(11) =~ 24.  24 * 0.1 = 2.4. This creates a strong signal.
	// We'll clamp the component to avoid it overwhelming PR/Betweenness completely if strictly 0-1 is needed,
	// but the formula implies direct linear combination. We will clamp the *result* to ensure 0-1.

	// Using the provided weights:
	term1 := c.PageRank * 0.6
	term2 := c.Betweenness * 0.3
	term3 := peerRisk * 0.1

	// Define peerNormalized for visibility in RiskComponents later
	peerNormalized := clamp01(term3)

	graphComposite := clamp01(term1 + term2 + term3)
	graphImpact := graphComposite * 100

	// Neighborhood amplification (Bonus risk, not part of the base weighted formula but originally requested)
	if neighborhoodSize > 0 {
		// Cap bonus to 20 points
		bonus := math.Min(float64(neighborhoodSize), 20.0)
		graphImpact += bonus
	}

	// Apply freshness penalty if graph is stale
	if health.Stale {
		graphImpact *= 0.85
	}

	// Weighted Sum
	rawScore := (latScore * r.weights.W2Latency) + (errScore * r.weights.W3Error) + (graphImpact * r.weights.W1PageRank)
	// Cap at 100
	if rawScore > 100 {
		rawScore = 100
	}

	// 4. Update Score History & Calculate Trends
	r.mu.Lock()
	r.scoreHistory[svcID] = append(r.scoreHistory[svcID], scoreRecord{Timestamp: r.clock.Now(), Score: rawScore})
	// Prune score history
	if len(r.scoreHistory[svcID]) > 300 {
		r.scoreHistory[svcID] = r.scoreHistory[svcID][len(r.scoreHistory[svcID])-300:]
	}
	// Make a copy for calculation to avoid holding lock during heavy calc (though calc is cheap)
	// Just holding lock is fine for iteration.
	sHist := r.scoreHistory[svcID]

	avg30s := r.calculateRollingAverage(sHist, 30*time.Second)
	avg2m := r.calculateRollingAverage(sHist, 2*time.Second)

	delta1s := 0.0
	if len(sHist) >= 2 {
		delta1s = sHist[len(sHist)-1].Score - sHist[len(sHist)-2].Score
	}

	// 5. Construct Risk Object
	rs := domain.RiskScore{
		Service:   t.Service,
		Score:     rawScore,
		Timestamp: r.clock.Now(),
		Components: domain.RiskComponents{
			LatencyScore: latScore,
			ErrorScore:   errScore,
			GraphScore:   graphImpact,
			PeerScore:    peerNormalized * 100,
		},
		Metrics: domain.RiskMetrics{
			LatencyP95:   t.LatencyP95Ms,
			LatencyP99:   t.LatencyP99Ms,
			ErrorRate1m:  t.ErrorRate1m,
			ErrorRate5m:  t.ErrorRate5m,
			Throughput:   t.ThroughputRPS,
			PageRank:     c.PageRank,
			Connectivity: c.BlastRadius,
			Upstream:     upCount,
			Downstream:   downCount,
			Neighborhood: neighborhoodSize,
		},
		Trends: domain.RiskTrends{
			ScoreAvg30s:  avg30s,
			ScoreAvg2m:   avg2m,
			ScoreDelta1s: delta1s,
		},
		Meta: domain.RiskMetadata{
			ModelVersion:            "v2.0-hybrid",
			ThresholdVersion:        "2026-01-02",
			GraphStale:              health.Stale,
			GraphWindowMinutes:      health.WindowMinutes,
			GraphLastUpdatedSeconds: health.LastUpdatedSecondsAgo,
		},
	}

	// Determine Severity
	rs.Severity = severityFrom(rawScore, r.threshold)

	// Store latest risk score
	r.lastRiskScores[svcID] = rs

	r.mu.Unlock()

	// Publish Score
	if err := r.bus.Publish(ctx, domain.TopicRiskScoreComputed, rs); err != nil {
		r.logger.Error("failed to publish risk score", "error", err)
	}

	// 6. Alerting Logic (Suppression & Breach Counting)
	r.mu.Lock()
	defer r.mu.Unlock()

	if rawScore > r.threshold {
		r.consecutiveBreaches[svcID]++
		r.healthyStreak[svcID] = 0
	} else {
		r.healthyStreak[svcID]++
		if r.healthyStreak[svcID] >= r.recoverySamples {
			r.consecutiveBreaches[svcID] = 0
		}
	}

	// Needs N consecutive breaches (e.g., 3)
	if r.consecutiveBreaches[svcID] >= 3 {
		// Check cooldown
		if lastAlert, ok := r.cooldowns[svcID]; ok {
			if time.Since(lastAlert) < 5*time.Second {
				return nil // Suppressed
			}
		}

		// Determine Mitigation Action
		action := domain.ActionObserve
		auto := false
		if latScore > 80 {
			action = domain.ActionThrottle
			auto = true // Throttle can be auto-applied
		} else if errScore > 80 {
			action = domain.ActionFailover
			auto = false // Failover might be risky to auto-trigger without redundancy check
		} else if graphImpact > 80 {
			// High graph risk (e.g. key node)
			action = domain.ActionObserve // Can't easily auto-fix centrality
			auto = false
		}

		if health.Stale {
			// Avoid making automated moves if topology is stale
			auto = false
			if action != domain.ActionObserve {
				action = domain.ActionObserve
			}
		}

		alert := domain.Alert{
			Service:           t.Service,
			Severity:          rs.Severity,
			Risk:              rs,
			CreatedAt:         r.clock.Now(),
			Explanation:       fmt.Sprintf("Risk %.2f exceeds threshold %.2f (lat=%.0f err=%.0f graph=%.0f peers=%d/%d stale=%t)", rawScore, r.threshold, latScore, errScore, graphImpact, downCount, upCount, health.Stale),
			RecommendedAction: action,
			ImpactScope: map[string]int{
				"downstream_count": downCount,
				"upstream_count":   upCount,
				"neighborhood":     neighborhoodSize,
			},
			AutoMitigatable: auto,
		}

		// Store alert
		r.recentAlerts = append(r.recentAlerts, alert)
		if len(r.recentAlerts) > 50 { // Keep only the 50 most recent alerts
			r.recentAlerts = r.recentAlerts[len(r.recentAlerts)-50:]
		}

		if err := r.bus.Publish(ctx, domain.TopicRiskAlertRaised, alert); err != nil {
			r.logger.Error("failed to publish alert", "error", err)
		} else {
			r.logger.Warn("risk alert raised", "service", svcID, "score", rawScore)
			r.cooldowns[svcID] = r.clock.Now() // Reset cooldown
		}
	}

	return nil
}

func (r *RiskService) calculateRollingAverage(hist []scoreRecord, window time.Duration) float64 {
	if len(hist) == 0 {
		return 0
	}
	sum := 0.0
	count := 0.0
	now := hist[len(hist)-1].Timestamp

	// Iterate backwards
	for i := len(hist) - 1; i >= 0; i-- {
		rec := hist[i]
		if now.Sub(rec.Timestamp) > window {
			break
		}
		sum += rec.Score
		count++
	}
	if count == 0 {
		return 0
	}
	return sum / count
}

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

func severityFrom(score, threshold float64) domain.Severity {
	if score > threshold*1.5 {
		return domain.SeverityCritical
	}
	return domain.SeverityWarning
}

func (r *RiskService) watchServices(ctx context.Context) {
	r.refreshServiceRoster(ctx)
	poll := time.NewTicker(r.servicePollInterval)
	liveness := time.NewTicker(r.livenessInterval)
	defer poll.Stop()
	defer liveness.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
			r.refreshServiceRoster(ctx)
		case <-liveness.C:
			r.checkSilentServices(ctx)
		}
	}
}

func (r *RiskService) refreshServiceRoster(ctx context.Context) {
	services, err := r.graph.GetServices(ctx)
	if err != nil {
		r.logger.Warn("failed to fetch service roster", "error", err)
		return
	}

	r.mu.Lock()
	for _, svc := range services {
		r.knownServices[svc.ID()] = svc
		r.knownByName[svc.Name] = svc
		if ns := svc.Namespace; ns != "" {
			r.allowedNS[ns] = struct{}{}
		}
		// Initialize lastSeen if we have never seen telemetry to start liveness timer.
		if _, ok := r.lastSeen[svc.ID()]; !ok {
			r.lastSeen[svc.ID()] = time.Time{}
		}
	}
	r.mu.Unlock()
}

func (r *RiskService) checkSilentServices(ctx context.Context) {
	now := r.clock.Now()

	r.mu.Lock()
	snapshot := make(map[string]domain.ServiceNode, len(r.knownServices))
	for k, v := range r.knownServices {
		snapshot[k] = v
	}
	lastSeen := make(map[string]time.Time, len(r.lastSeen))
	for k, v := range r.lastSeen {
		lastSeen[k] = v
	}
	seen := make(map[string]bool, len(r.seenTelemetry))
	for k, v := range r.seenTelemetry {
		seen[k] = v
	}
	seenByName := make(map[string]bool, len(r.seenByName))
	for k, v := range r.seenByName {
		seenByName[k] = v
	}
	lastSeenByName := make(map[string]time.Time, len(r.lastSeenByName))
	for k, v := range r.lastSeenByName {
		lastSeenByName[k] = v
	}
	r.mu.Unlock()

	for id, svc := range snapshot {
		ts := lastSeen[id]
		svcSeen := seen[id]
		if !svcSeen {
			// Fallback: treat same service name across namespaces as equivalent for liveness.
			if seenByName[svc.Name] {
				ts = lastSeenByName[svc.Name]
				svcSeen = true
			}
		}
		if !svcSeen || ts.IsZero() {
			continue // no telemetry yet
		}
		// Allow a buffer before declaring the service silent.
		if now.Sub(ts) < r.livenessTimeout {
			continue
		}
		r.raiseSilentAlert(ctx, svc, ts, now)
	}
}

func (r *RiskService) raiseSilentAlert(ctx context.Context, svc domain.ServiceNode, last time.Time, now time.Time) {
	r.mu.Lock()
	if r.offlineActive[svc.ID()] {
		r.mu.Unlock()
		return
	}
	if lastAlert, ok := r.offlineCooldown[svc.ID()]; ok {
		if now.Sub(lastAlert) < r.livenessTimeout {
			r.mu.Unlock()
			return
		}
	}
	r.offlineActive[svc.ID()] = true
	r.offlineCooldown[svc.ID()] = now
	r.mu.Unlock()

	gap := r.livenessTimeout
	if !last.IsZero() {
		gap = now.Sub(last)
	}

	alert := domain.Alert{
		Service:           svc,
		Severity:          domain.SeverityCritical,
		RecommendedAction: domain.ActionObserve,
		ImpactScope: map[string]int{
			"downstream_count": 0,
			"upstream_count":   0,
			"neighborhood":     0,
		},
		AutoMitigatable: false,
		CreatedAt:       now,
		Explanation:     fmt.Sprintf("No telemetry observed for %s for %s", svc.ID(), gap.Truncate(time.Second)),
		Risk: domain.RiskScore{
			Service:   svc,
			Score:     100,
			Severity:  domain.SeverityCritical,
			Timestamp: now,
			Meta: domain.RiskMetadata{
				ModelVersion:     "v2.0-hybrid",
				ThresholdVersion: "2026-01-02",
			},
		},
	}

	if err := r.bus.Publish(ctx, domain.TopicRiskAlertRaised, alert); err != nil {
		r.logger.Error("failed to publish silent-service alert", "error", err, "service", svc.ID())
		return
	}
	r.logger.Warn("service telemetry silent", "service", svc.ID(), "gap", gap)
}

// canonicalizeService attempts to align telemetry services with the graph roster so we don't emit alerts for unknown namespaces.
func (r *RiskService) canonicalizeService(svc domain.ServiceNode) domain.ServiceNode {
	r.mu.Lock()
	defer r.mu.Unlock()

	if s, ok := r.knownServices[svc.ID()]; ok {
		return s
	}
	if s, ok := r.knownByName[svc.Name]; ok {
		return s
	}
	// Fallback: if namespace is unknown or unrecognized, prefer default.
	if svc.Namespace != "" {
		if _, ok := r.allowedNS[svc.Namespace]; !ok {
			return domain.ServiceNode{Name: svc.Name, Namespace: "default"}
		}
		return svc
	}
	return svc
}
