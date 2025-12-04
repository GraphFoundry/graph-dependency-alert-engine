package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"graph-alert-engine/internal/core/domain"
	"graph-alert-engine/internal/core/ports"
	"net/http"
	"time"
)

type Outbound struct {
	bus     ports.EventBus
	targets []string
	secret  []byte
	hc      *http.Client
}

func NewOutbound(bus ports.EventBus, targets []string, secret []byte) *Outbound {
	return &Outbound{
		bus:     bus,
		targets: targets,
		secret:  secret,
		hc:      &http.Client{Timeout: 2 * time.Second},
	}
}

func (o *Outbound) Start(ctx context.Context) (stop func()) {
	return o.bus.Subscribe(domain.TopicRiskAlertRaised, func(hctx context.Context, payload any) {
		alert, ok := payload.(domain.Alert)
		if !ok {
			return
		}
		_ = o.dispatch(hctx, alert)
	})
}

func (o *Outbound) dispatch(ctx context.Context, alert domain.Alert) error {
	body, err := json.Marshal(alert)
	if err != nil {
		return err
	}
	sig := ""
	if len(o.secret) > 0 {
		sig = SignHMACSHA256(o.secret, body)
	}

	for _, url := range o.targets {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		if sig != "" {
			req.Header.Set("X-Signature", sig)
		}
		resp, err := o.hc.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		// In v1 keep it simple: best-effort send; add retries/backoff later.
		_ = err
	}
	return nil
}
