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

	"graph-alert-engine/internal/core/domain"
	"graph-alert-engine/internal/core/ports"
)

// Notifier sends formatted alert messages to a Slack incoming webhook.
type Notifier struct {
	bus        ports.EventBus
	webhookURL string
	hc         *http.Client
	logger     *slog.Logger
}

// slackPayload is the Slack incoming-webhook request body.
type slackPayload struct {
	Text   string       `json:"text"`             // Fallback plain-text
	Blocks []slackBlock `json:"blocks,omitempty"` // Rich Block Kit UI
}

type slackBlock struct {
	Type   string      `json:"type"`
	Text   *slackText  `json:"text,omitempty"`
	Fields []slackText `json:"fields,omitempty"`
}

type slackText struct {
	Type string `json:"type"` // "mrkdwn" or "plain_text"
	Text string `json:"text"`
}

func New(logger *slog.Logger, bus ports.EventBus, webhookURL string) *Notifier {
	return &Notifier{
		bus:        bus,
		webhookURL: webhookURL,
		logger:     logger,
		hc:         &http.Client{Timeout: 5 * time.Second},
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
	p := buildPayload(alert)

	body, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.webhookURL, bytes.NewReader(body))
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

	headline := fmt.Sprintf("%s *%s* — %s/%s",
		icon, sev, alert.Service.Namespace, alert.Service.Name)

	fallback := fmt.Sprintf("[%s] %s %s/%s — %s",
		sev, string(alert.Type), alert.Service.Namespace, alert.Service.Name, alert.Explanation)

	blocks := []slackBlock{
		// Header
		{
			Type: "section",
			Text: &slackText{Type: "mrkdwn", Text: headline},
		},
		// Alert type & explanation
		{
			Type: "section",
			Text: &slackText{Type: "mrkdwn", Text: fmt.Sprintf("*Type:* `%s`\n*State:* %s\n\n%s",
				string(alert.Type), string(alert.State), alert.Explanation)},
		},
		// Key metrics
		{
			Type: "section",
			Fields: buildFields(alert),
		},
		// Divider
		{Type: "divider"},
		// Action recommendation
		{
			Type: "section",
			Text: &slackText{Type: "mrkdwn", Text: fmt.Sprintf("*Recommended Action:* `%s`  |  *Priority:* %s  |  *Auto-mitigatable:* %v",
				string(alert.RecommendedAction), priorityLabel(alert.Priority), alert.AutoMitigatable)},
		},
	}

	if len(alert.ReasonCodes) > 0 {
		blocks = append(blocks, slackBlock{
			Type: "context",
			Fields: []slackText{
				{Type: "mrkdwn", Text: fmt.Sprintf("Reason codes: %s", strings.Join(alert.ReasonCodes, ", "))},
			},
		})
	}

	return slackPayload{
		Text:   fallback,
		Blocks: blocks,
	}
}

func buildFields(alert domain.Alert) []slackText {
	fields := []slackText{
		{Type: "mrkdwn", Text: fmt.Sprintf("*Risk Score:*\n%.1f / 100", alert.Risk.Score)},
	}

	if ds, ok := alert.ImpactScope["downstream_services"]; ok && ds > 0 {
		fields = append(fields, slackText{
			Type: "mrkdwn", Text: fmt.Sprintf("*Downstream Impact:*\n%d services", ds),
		})
	}

	if alert.Service.Availability > 0 {
		fields = append(fields, slackText{
			Type: "mrkdwn", Text: fmt.Sprintf("*Availability:*\n%.2f%%", alert.Service.Availability*100),
		})
	}

	return fields
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
