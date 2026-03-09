package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
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
	Text   string       `json:"text"`             // Fallback plain-text
	Blocks []slackBlock `json:"blocks,omitempty"` // Rich Block Kit UI
}

type slackBlock struct {
	Type     string      `json:"type"`
	Text     *slackText  `json:"text,omitempty"`
	Fields   []slackText `json:"fields,omitempty"`
	Elements []slackText `json:"elements,omitempty"`
}

type slackText struct {
	Type string `json:"type"` // "mrkdwn" or "plain_text"
	Text string `json:"text"`
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

// buildPayload creates a rich Slack Block Kit message from a domain alert.
func buildPayload(alert domain.Alert) slackPayload {
	sev := strings.ToUpper(string(alert.Severity))
	icon := severityIcon(alert.Severity)
	cfg := config.Get()

	headline := fmt.Sprintf("%s *[%s] %s/%s*",
		icon, sev, alert.Service.Namespace, alert.Service.Name)

	fallback := fmt.Sprintf("[%s] %s %s/%s — %s",
		sev, string(alert.Type), alert.Service.Namespace, alert.Service.Name, alert.Explanation)

	blocks := []slackBlock{
		// ── Header ──
		{
			Type: "header",
			Text: &slackText{Type: "plain_text", Text: fmt.Sprintf("%s %s Alert — %s/%s",
				icon, sev, alert.Service.Namespace, alert.Service.Name)},
		},
		// ── Classification ──
		{
			Type: "section",
			Text: &slackText{Type: "mrkdwn", Text: fmt.Sprintf(
				"%s\n\n*Type:* `%s`  •  *State:* `%s`  •  *Priority:* `%s`\n\n> %s",
				headline,
				string(alert.Type), string(alert.State),
				priorityLabel(alert.Priority),
				alert.Explanation,
			)},
		},
		{Type: "divider"},
		// ── Risk & Metrics ──
		{
			Type:   "section",
			Fields: buildRiskFields(alert),
		},
		// ── Latency & Error Metrics ──
		{
			Type:   "section",
			Fields: buildObservabilityFields(alert),
		},
		// ── Graph Topology ──
		{
			Type:   "section",
			Fields: buildGraphFields(alert),
		},
		{Type: "divider"},
		// ── Decision ──
		{
			Type: "section",
			Text: &slackText{Type: "mrkdwn", Text: fmt.Sprintf(
				"*:zap: Recommended Action:* `%s`\n*Auto-mitigatable:* %s",
				string(alert.RecommendedAction), boolEmoji(alert.AutoMitigatable),
			)},
		},
	}

	// ── Reason Codes ──
	if len(alert.ReasonCodes) > 0 {
		codes := make([]string, len(alert.ReasonCodes))
		for i, c := range alert.ReasonCodes {
			codes[i] = "`" + c + "`"
		}
		blocks = append(blocks, slackBlock{
			Type: "section",
			Text: &slackText{Type: "mrkdwn", Text: fmt.Sprintf("*Reason Codes:*  %s", strings.Join(codes, "  "))},
		})
	}

	// ── Trend ──
	if delta := alert.Risk.Trends.ScoreDelta1s; delta != 0 {
		arrow := ":chart_with_upwards_trend:"
		if delta < 0 {
			arrow = ":chart_with_downwards_trend:"
		}
		blocks = append(blocks, slackBlock{
			Type: "section",
			Text: &slackText{Type: "mrkdwn", Text: fmt.Sprintf(
				"%s *Trend:* Δ%+.1f/s  |  30s avg: %.1f  |  2m avg: %.1f",
				arrow, delta, alert.Risk.Trends.ScoreAvg30s, alert.Risk.Trends.ScoreAvg2m,
			)},
		})
	}

	// ── Footer Context ──
	ctxParts := []string{
		fmt.Sprintf("Cluster: %s", cfg.ClusterName),
		fmt.Sprintf("Region: %s", cfg.Region),
		fmt.Sprintf("Env: %s", cfg.Environment),
		fmt.Sprintf("Alert at %s", alert.CreatedAt.UTC().Format(time.RFC3339)),
	}
	if alert.Risk.Meta.CalculationID != "" {
		ctxParts = append(ctxParts, fmt.Sprintf("CalcID: %s", alert.Risk.Meta.CalculationID))
	}

	blocks = append(blocks, slackBlock{
		Type: "context",
		Elements: []slackText{
			{Type: "mrkdwn", Text: strings.Join(ctxParts, "  •  ")},
		},
	})

	return slackPayload{
		Text:   fallback,
		Blocks: blocks,
	}
}

func buildRiskFields(alert domain.Alert) []slackText {
	scoreBar := scoreProgressBar(alert.Risk.Score)
	fields := []slackText{
		{Type: "mrkdwn", Text: fmt.Sprintf("*Risk Score:*\n%s  *%.1f* / 100", scoreBar, alert.Risk.Score)},
		{Type: "mrkdwn", Text: fmt.Sprintf("*Severity:*\n%s %s",
			severityIcon(alert.Severity), strings.ToUpper(string(alert.Severity)))},
	}
	return fields
}

func buildObservabilityFields(alert domain.Alert) []slackText {
	m := alert.Risk.Metrics
	fields := []slackText{}

	if m.LatencyP95 > 0 || m.LatencyP99 > 0 {
		fields = append(fields, slackText{
			Type: "mrkdwn",
			Text: fmt.Sprintf("*Latency:*\nP95: `%.1f ms`  P99: `%.1f ms`", m.LatencyP95, m.LatencyP99),
		})
	}

	if m.ErrorRate1m > 0 || m.ErrorRate5m > 0 {
		fields = append(fields, slackText{
			Type: "mrkdwn",
			Text: fmt.Sprintf("*Error Rate:*\n1m: `%.2f%%`  5m: `%.2f%%`", m.ErrorRate1m*100, m.ErrorRate5m*100),
		})
	}

	if m.Throughput > 0 {
		fields = append(fields, slackText{
			Type: "mrkdwn",
			Text: fmt.Sprintf("*Throughput:*\n`%.0f rps`", m.Throughput),
		})
	}

	if alert.Service.Availability > 0 {
		fields = append(fields, slackText{
			Type: "mrkdwn",
			Text: fmt.Sprintf("*Availability:*\n`%.2f%%`", alert.Service.Availability*100),
		})
	}

	if len(fields) == 0 {
		fields = append(fields, slackText{Type: "mrkdwn", Text: "_No observability metrics available_"})
	}
	return fields
}

func buildGraphFields(alert domain.Alert) []slackText {
	m := alert.Risk.Metrics
	fields := []slackText{}

	if m.PageRank > 0 {
		fields = append(fields, slackText{
			Type: "mrkdwn",
			Text: fmt.Sprintf("*PageRank:*\n`%.4f`", m.PageRank),
		})
	}

	impact := []string{}
	if ds, ok := alert.ImpactScope["downstream_count"]; ok && ds > 0 {
		impact = append(impact, fmt.Sprintf("%d downstream", ds))
	}
	if us, ok := alert.ImpactScope["upstream_count"]; ok && us > 0 {
		impact = append(impact, fmt.Sprintf("%d upstream", us))
	}
	if nb, ok := alert.ImpactScope["neighborhood"]; ok && nb > 0 {
		impact = append(impact, fmt.Sprintf("%d neighborhood", nb))
	}
	if len(impact) > 0 {
		fields = append(fields, slackText{
			Type: "mrkdwn",
			Text: fmt.Sprintf("*Blast Radius:*\n%s", strings.Join(impact, " • ")),
		})
	}

	if alert.Risk.Meta.GraphStale {
		fields = append(fields, slackText{
			Type: "mrkdwn",
			Text: ":warning: *Graph data is stale*",
		})
	}

	if len(fields) == 0 {
		fields = append(fields, slackText{Type: "mrkdwn", Text: "_No graph topology data_"})
	}
	return fields
}

func scoreProgressBar(score float64) string {
	filled := int(score / 10)
	if filled > 10 {
		filled = 10
	}
	empty := 10 - filled
	if score >= 80 {
		return strings.Repeat(":red_square:", filled) + strings.Repeat(":white_large_square:", empty)
	}
	if score >= 50 {
		return strings.Repeat(":orange_square:", filled) + strings.Repeat(":white_large_square:", empty)
	}
	return strings.Repeat(":large_green_square:", filled) + strings.Repeat(":white_large_square:", empty)
}

func severityIcon(s domain.Severity) string {
	switch s {
	case domain.SeverityCritical:
		return ":red_circle:"
	case domain.SeverityWarning:
		return ":large_yellow_circle:"
	default:
		return ":large_blue_circle:"
	}
}

func priorityLabel(p string) string {
	if p == "" {
		return "unset"
	}
	return p
}

func boolEmoji(v bool) string {
	if v {
		return ":white_check_mark: Yes"
	}
	return ":x: No"
}
