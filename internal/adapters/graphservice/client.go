package graphservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"graph-alert-engine/internal/core/domain"
	"graph-alert-engine/internal/core/ports"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Client struct {
	baseURL string
	hc      *http.Client
	cb      *CircuitBreaker
	retries int

	cacheMu         sync.RWMutex
	cacheTTL        time.Duration
	cachedAt        time.Time
	centralityCache map[string]domain.Centrality
}

func New(baseURL string, timeout time.Duration, retries int, cb *CircuitBreaker, cacheTTL time.Duration) *Client {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	if retries < 0 {
		retries = 0
	}
	if cb == nil {
		cb = NewCircuitBreaker(3, 5*time.Second)
	}
	if cacheTTL < 0 {
		cacheTTL = 0
	}
	return &Client{
		baseURL:         strings.TrimRight(baseURL, "/"),
		hc:              &http.Client{Timeout: timeout},
		cb:              cb,
		retries:         retries,
		cacheTTL:        cacheTTL,
		centralityCache: make(map[string]domain.Centrality),
	}
}

var _ ports.GraphProvider = (*Client)(nil)

func (c *Client) GetServices(ctx context.Context) ([]domain.ServiceNode, error) {
	var out servicesResponse
	if err := c.getJSON(ctx, c.path("/services"), &out); err != nil {
		if isNotFound(err) {
			if err := c.getJSON(ctx, c.path("/graph/services"), &out); err != nil {
				return nil, classify(err)
			}
		} else {
			return nil, classify(err)
		}
	}
	res := make([]domain.ServiceNode, 0, len(out.Services))
	for _, s := range out.Services {
		res = append(res, domain.ServiceNode{Name: s.Name, Namespace: s.Namespace})
	}
	return res, nil
}

func (c *Client) GetCentrality(ctx context.Context) (map[string]domain.Centrality, error) {
	// Cache
	if c.cacheTTL > 0 {
		c.cacheMu.RLock()
		if time.Since(c.cachedAt) <= c.cacheTTL && len(c.centralityCache) > 0 {
			cp := make(map[string]domain.Centrality, len(c.centralityCache))
			for k, v := range c.centralityCache {
				cp[k] = v
			}
			c.cacheMu.RUnlock()
			return cp, nil
		}
		c.cacheMu.RUnlock()
	}

	var out centralityResponse
	paths := []string{"/centrality", "/graph/centrality", "/graph/centrality/scores"}
	var lastErr error
	for _, p := range paths {
		if err := c.getJSON(ctx, c.path(p), &out); err != nil {
			lastErr = err
			if isNotFound(err) {
				continue
			}
			return nil, classify(err)
		}
		lastErr = nil
		break
	}
	if lastErr != nil {
		return nil, classify(lastErr)
	}

	m := make(map[string]domain.Centrality, len(out.Scores))
	now := time.Now()
	for _, it := range out.Scores {
		// API returns simple service name. Assume default namespace or treat as canonical ID.
		// Since we don't have namespace from API, and simulator uses "prod", this is tricky.
		// However, standard graph service usually returns just names. We'll map to "default"
		// or whatever makes sense. Ideally, the 'Service' string IS the ID.
		// But domain.ServiceNode splits it.
		// Let's assume "default" for now if just a name, or parse it if it looks like "ns/name".
		name, ns := parseServiceString(it.Service)
		svc := domain.ServiceNode{Name: name, Namespace: ns}

		// If API returns unbounded pagerank, normalize in adapter (do NOT leak weirdness into core).
		pr := it.PageRank
		if pr < 0 {
			pr = 0
		}
		m[svc.ID()] = domain.Centrality{
			Service:          svc,
			PageRank:         pr,
			Betweenness:      it.Betweenness,
			BlastRadius:      it.BlastRadius,
			DownstreamCount:  it.DownstreamCount,
			ErrorPropagation: it.ErrorPropagation,
			UpdatedAt:        now,
		}
	}

	if c.cacheTTL > 0 {
		c.cacheMu.Lock()
		c.centralityCache = m
		c.cachedAt = time.Now()
		c.cacheMu.Unlock()
	}

	return m, nil
}

func (c *Client) GetHealth(ctx context.Context) (domain.GraphHealth, error) {
	paths := []string{"/health", "/graph/health"} // try both since deployments may expose either
	var out healthResponse
	var lastErr error
	for _, p := range paths {
		if err := c.getJSON(ctx, c.path(p), &out); err != nil {
			lastErr = err
			if isNotFound(err) {
				continue // try next path
			}
			return domain.GraphHealth{}, classify(err)
		}
		return domain.GraphHealth{
			Status:                out.Status,
			Stale:                 out.Stale,
			LastUpdatedSecondsAgo: out.LastUpdatedSecondsAgo,
			WindowMinutes:         out.WindowMinutes,
		}, nil
	}
	return domain.GraphHealth{}, classify(lastErr)
}

func (c *Client) GetPeers(ctx context.Context, svc domain.ServiceNode, direction domain.Direction, limit int) ([]domain.ServiceNode, error) {
	// User requested endpoint pattern: /services/{name}/peers
	// NOT /services/{namespace}/{name}/peers
	if limit <= 0 {
		limit = 10
	}
	path := fmt.Sprintf("%s?direction=%s&limit=%d", c.path(fmt.Sprintf("/services/%s/peers", svc.Name)), direction, limit)
	var out peersResponse
	if err := c.getJSON(ctx, path, &out); err != nil {
		if isNotFound(err) {
			graphPath := fmt.Sprintf("%s?direction=%s&limit=%d", c.path(fmt.Sprintf("/graph/services/%s/peers", svc.Name)), direction, limit)
			if err := c.getJSON(ctx, graphPath, &out); err != nil {
				return nil, classify(err)
			}
		} else {
			return nil, classify(err)
		}
	}
	res := make([]domain.ServiceNode, 0, len(out.Peers))
	for _, p := range out.Peers {
		name, ns := parseServiceString(p.Service)
		res = append(res, domain.ServiceNode{Name: name, Namespace: ns})
	}
	return res, nil
}

func (c *Client) GetNeighborhood(ctx context.Context, svc domain.ServiceNode, k int) ([]domain.ServiceNode, error) {
	if k <= 0 {
		k = 1
	}
	path := fmt.Sprintf("%s?k=%d", c.path(fmt.Sprintf("/services/%s/neighborhood", svc.Name)), k)
	var out neighborhoodResponse
	if err := c.getJSON(ctx, path, &out); err != nil {
		if isNotFound(err) {
			graphPath := fmt.Sprintf("%s?k=%d", c.path(fmt.Sprintf("/graph/services/%s/neighborhood", svc.Name)), k)
			if err := c.getJSON(ctx, graphPath, &out); err != nil {
				return nil, classify(err)
			}
		} else {
			return nil, classify(err)
		}
	}
	res := make([]domain.ServiceNode, 0, len(out.Nodes))
	for _, n := range out.Nodes {
		name, ns := parseServiceString(n)
		res = append(res, domain.ServiceNode{Name: name, Namespace: ns})
	}
	return res, nil
}

func parseServiceString(s string) (name, namespace string) {
	if strings.Contains(s, "/") {
		parts := strings.SplitN(s, "/", 2)
		return parts[1], parts[0]
	}
	return s, "default"
}

func (c *Client) getJSON(ctx context.Context, path string, dst any) error {
	now := time.Now()
	if !c.cb.Allow(now) {
		return domain.ErrGraphUnavailable
	}

	url := c.baseURL + path

	var lastErr error
	for attempt := 0; attempt <= c.retries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := c.hc.Do(req)
		if err == nil && resp != nil {
			defer resp.Body.Close()
		}

		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			dec := json.NewDecoder(resp.Body)
			if derr := dec.Decode(dst); derr != nil {
				lastErr = derr
			} else {
				c.cb.OnSuccess()
				return nil
			}
		} else {
			// Drain body to reuse connections (best effort)
			if resp != nil && resp.Body != nil {
				_, _ = io.Copy(io.Discard, resp.Body)
			}
			if err != nil {
				lastErr = err
			} else if resp != nil {
				lastErr = fmt.Errorf("http %d", resp.StatusCode)
			} else {
				lastErr = errors.New("nil response")
			}
		}

		if attempt < c.retries && shouldRetry(resp, err) {
			_ = sleepCtx(ctx, backoff(attempt))
			continue
		}
		break
	}

	c.cb.OnFailure(time.Now())
	return lastErr
}

func classify(err error) error {
	// Keep it simple: treat unknown failures as unavailable.
	if errors.Is(err, domain.ErrGraphStale) {
		return err
	}
	return fmt.Errorf("%w: %v", domain.ErrGraphUnavailable, err)
}

// path builds a slash-prefixed path relative to the graph-service base URL.
func (c *Client) path(p string) string {
	p = strings.TrimPrefix(p, "/")
	return "/" + p
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "404")
}
