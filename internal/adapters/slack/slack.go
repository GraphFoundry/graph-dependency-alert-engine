package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"graph-alert-engine/config"
	"graph-alert-engine/internal/core/domain"
	"graph-alert-engine/internal/core/ports"
)

// Notifier sends formatted alert messages to a Slack incoming webhook.
// The webhook URL is read from config.Get() at send time so that runtime
// config reloads take effect without a pod restart. If the URL is empty
// when an alert fires, the notification is silently skipped.
type Notifier struct {
	bus    ports.EventBus
	hc     *http.Client
	logger *slog.Logger
}

// slackPayload is the Slack incoming-webhook request body.
type slackPayload struct {
	Text        string            `json:"text"`                  // Fallback plain-text
	Attachments []slackAttachment `json:"attachments,omitempty"` // Rich attachment with color sidebar
}

type slackAttachment struct {
	Color   string       `json:"color"`
	Blocks  []slackBlock `json:"blocks"`
	Fallback string      `json:"fallback,omitempty"`
}

type slackBlock struct {
	Type     string        `json:"type"`
	Text     *slackText    `json:"text,omitempty"`
	Fields   []slackText   `json:"fields,omitempty"`
	Elements []interface{} `json:"elements,omitempty"`
}

type slackText struct {
	Type string `json:"type"` // "mrkdwn" or "plain_text"
	Text string `json:"text"`
}

type slackButton struct {
	Type string    `json:"type"` // "button"
	Text slackText `json:"text"`
	URL  string    `json:"url"`
}

func New(logger *slog.Logger, bus ports.EventBus) *Notifier {
	return &Notifier{
		bus:    bus,
		logger: logger,
		hc:     &http.Client{Timeout: 5 * time.Second},
	}
}

// Start subscribes to risk alerts and returns a stop function.
func (n *Notifier) Start(ctx context.Context) (stop func()) {
	return n.bus.Subscribe(domain.TopicRiskAlertRaised, func(hctx context.Context, payload any) {
		alert, ok := payload.(domain.Alert)
		if !ok {
			return
		}
		if err := n.send(hctx, alert); err != nil {
			n.logger.Error("slack notification failed", "service", alert.Service.Name, "error", err)
		}
	})
}

func (n *Notifier) send(ctx context.Context, alert domain.Alert) error {
	webhookURL := config.Get().SlackWebhookURL
	if webhookURL == "" {
		return nil // Slack not configured — skip silently
	}

	// Filter out info-level alerts (restorations, etc.) to reduce noise.
	// Only actionable warning/critical alerts are sent to Slack.
	if alert.Severity == domain.SeverityInfo {
		n.logger.Debug("skipping info-level slack notification", "service", alert.Service.Name)
		return nil
	}

	p := buildPayload(alert)

	body, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.hc.Do(req)
	if err != nil {
		return fmt.Errorf("post to slack: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("slack returned status %d", resp.StatusCode)
	}
	n.logger.Info("slack alert sent", "service", alert.Service.Name, "severity", alert.Severity)
	return nil
}

// buildPayload creates a professional Slack message using attachments for the
// colored sidebar, compact field layout, and an action button.
func buildPayload(alert domain.Alert) slackPayload {
	cfg := config.Get()
	sev := strings.ToUpper(string(alert.Severity))
	svcID := alert.Service.Name
	if alert.Service.Namespace != "" {
		svcID = alert.Service.Namespace + "/" + alert.Service.Name
	}

	fallback := fmt.Sprintf("[%s] %s — %s", sev, svcID, alert.Explanation)

	// ── Blocks inside the colored attachment ──

	// Title line
	title := fmt.Sprintf("*%s*", svcID)
	blocks := []slackBlock{
		{
			Type: "section",
			Text: &slackText{Type: "mrkdwn", Text: title},
		},
	}

	// Summary line
	summary := alert.Explanation
	if summary != "" {
		blocks = append(blocks, slackBlock{
			Type: "section",
			Text: &slackText{Type: "mrkdwn", Text: summary},
		})
	}

	// Detail fields — compact two-column layout, only include meaningful data
	fields := []slackText{}
	fields = append(fields, slackText{Type: "mrkdwn", Text: fmt.Sprintf("*Status:* %s", humanState(alert.State))})
	fields = append(fields, slackText{Type: "mrkdwn", Text: fmt.Sprintf("*Severity:* %s", sev)})
	fields = append(fields, slackText{Type: "mrkdwn", Text: fmt.Sprintf("*Type:* %s", humanAlertType(alert.Type))})
	fields = append(fields, slackText{Type: "mrkdwn", Text: fmt.Sprintf("*Priority:* %s", priorityLabel(alert.Priority))})

	if alert.Risk.Score > 0 {
		fields = append(fields, slackText{Type: "mrkdwn", Text: fmt.Sprintf("*Risk Score:* %.0f / 100", alert.Risk.Score)})
	}
	if alert.Service.Availability > 0 {
		fields = append(fields, slackText{Type: "mrkdwn", Text: fmt.Sprintf("*Availability:* %.1f%%", alert.Service.Availability*100)})
	}

	m := alert.Risk.Metrics
	if m.LatencyP95 > 0 {
		fields = append(fields, slackText{Type: "mrkdwn", Text: fmt.Sprintf("*Latency P95:* %.0f ms", m.LatencyP95)})
	}
	if m.ErrorRate5m > 0 {
		fields = append(fields, slackText{Type: "mrkdwn", Text: fmt.Sprintf("*Error Rate:* %.2f%%", m.ErrorRate5m*100)})
	}

	if ds, ok := alert.ImpactScope["downstream_count"]; ok && ds > 0 {
		fields = append(fields, slackText{Type: "mrkdwn", Text: fmt.Sprintf("*Downstream:* %d services", ds)})
	}

	fields = append(fields, slackText{Type: "mrkdwn", Text: fmt.Sprintf("*Action:* %s", humanAction(alert.RecommendedAction))})

	blocks = append(blocks, slackBlock{
		Type:   "section",
		Fields: fields,
	})

	// Reason codes as a single context line
	if len(alert.ReasonCodes) > 0 {
		blocks = append(blocks, slackBlock{
			Type: "context",
			Elements: []interface{}{
				slackText{Type: "mrkdwn", Text: strings.Join(alert.ReasonCodes, "  ·  ")},
			},
		})
	}

	// Actions — View Alert button
	dashboardURL := cfg.DashboardURL
	if dashboardURL != "" {
		alertPath := "/alerts"
		if alert.DedupeKey != "" {
			alertPath = fmt.Sprintf("/alerts/%s", alert.DedupeKey)
		}
		alertURL := strings.TrimRight(dashboardURL, "/") + alertPath
		// Append namespace and service as query parameters so the
		// dashboard can resolve context without a DedupeKey lookup.
		qv := url.Values{}
		if alert.Service.Namespace != "" {
			qv.Set("namespace", alert.Service.Namespace)
		}
		if alert.Service.Name != "" {
			qv.Set("service", alert.Service.Name)
		}
		if encoded := qv.Encode(); encoded != "" {
			alertURL += "?" + encoded
		}
		blocks = append(blocks, slackBlock{
			Type: "actions",
			Elements: []interface{}{
				slackButton{
					Type: "button",
					Text: slackText{Type: "plain_text", Text: "View Alert"},
					URL:  alertURL,
				},
			},
		})
	}

	// Footer context
	ctxParts := []string{cfg.ClusterName, cfg.Region, cfg.Environment}
	ctxParts = append(ctxParts, alert.CreatedAt.UTC().Format(time.RFC3339))
	blocks = append(blocks, slackBlock{
		Type: "context",
		Elements: []interface{}{
			slackText{Type: "mrkdwn", Text: strings.Join(ctxParts, "  ·  ")},
		},
	})

	return slackPayload{
		Text: fallback,
		Attachments: []slackAttachment{
			{
				Color:    severityColor(alert.Severity),
				Blocks:   blocks,
				Fallback: fallback,
			},
		},
	}
}

func severityColor(s domain.Severity) string {
	switch s {
	case domain.SeverityCritical:
		return "#E01E5A" // red
	case domain.SeverityWarning:
		return "#ECB22E" // amber
	default:
		return "#2EB67D" // green/info
	}
}

func humanState(s domain.AlertState) string {
	switch s {
	case domain.AlertStateFiring:
		return "Firing"
	case domain.AlertStateResolved:
		return "Resolved"
	case domain.AlertStateAcknowledged:
		return "Acknowledged"
	default:
		return string(s)
	}
}

func humanAlertType(t domain.AlertType) string {
	switch t {
	case domain.AlertTypeAvailabilityDegraded:
		return "Availability Degraded"
	case domain.AlertTypeErrorRateHigh:
		return "Error Rate High"
	case domain.AlertTypeLatencyHigh:
		return "Latency High"
	case domain.AlertTypeSingleReplica:
		return "Single Replica"
	case domain.AlertTypeGraphCentralityHigh:
		return "High Centrality"
	default:
		return string(t)
	}
}

func humanAction(a domain.MitigationAction) string {
	switch a {
	case domain.ActionThrottle:
		return "Throttle"
	case domain.ActionFailover:
		return "Failover"
	case domain.ActionObserve:
		return "Observe"
	case domain.ActionScaleUp:
		return "Scale Up"
	case domain.ActionDegrade:
		return "Degrade"
	default:
		return string(a)
	}
}

func priorityLabel(p string) string {
	if p == "" {
		return "—"
	}
	return p
}
