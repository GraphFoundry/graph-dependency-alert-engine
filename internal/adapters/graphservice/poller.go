package graphservice

import (
	"context"
	"graph-alert-engine/internal/core/domain"
	"graph-alert-engine/internal/core/ports"
	"log/slog"
	"sync"
	"time"
)

type GraphPoller struct {
	logger   *slog.Logger
	client   ports.GraphProvider
	interval time.Duration

	mu             sync.RWMutex
	health         domain.GraphHealth
	centrality     map[string]domain.Centrality
	services       map[string]domain.ServiceNode
	lastHealthPoll time.Time
	lastCentPoll   time.Time
	lastSvcPoll    time.Time

	peerCache    map[string]peerCacheEntry
	peerCacheTTL time.Duration
}

type peerCacheEntry struct {
	Peers     []domain.ServiceNode
	Timestamp time.Time
}

func NewGraphPoller(logger *slog.Logger, client ports.GraphProvider, interval time.Duration) *GraphPoller {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &GraphPoller{
		logger:       logger,
		client:       client,
		interval:     interval,
		centrality:   make(map[string]domain.Centrality),
		services:     make(map[string]domain.ServiceNode),
		peerCache:    make(map[string]peerCacheEntry),
		peerCacheTTL: 5 * time.Second, // Default TTL
	}
}

func (p *GraphPoller) Start(ctx context.Context) func() {
	// Initial poll
	p.poll(ctx)

	ticker := time.NewTicker(p.interval)
	done := make(chan struct{})

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				p.poll(ctx)
			}
		}
	}()

	return func() {
		close(done)
	}
}

// ... helper to generate key
func peerKey(svc domain.ServiceNode, dir domain.Direction) string {
	return svc.ID() + "|" + string(dir)
}

func (p *GraphPoller) GetPeers(ctx context.Context, svc domain.ServiceNode, direction domain.Direction, limit int) ([]domain.ServiceNode, error) {
	key := peerKey(svc, direction)

	p.mu.RLock()
	entry, ok := p.peerCache[key]
	p.mu.RUnlock()

	if ok && time.Since(entry.Timestamp) < p.peerCacheTTL {
		// Return copy
		out := make([]domain.ServiceNode, len(entry.Peers))
		copy(out, entry.Peers)
		return out, nil
	}

	// Cache miss or stale
	peers, err := p.client.GetPeers(ctx, svc, direction, limit)
	if err != nil {
		// If stale data exists, return it with warning instead of error?
		if ok {
			p.logger.Warn("poller: failed to refresh peers, using stale cache", "service", svc.ID(), "error", err)
			out := make([]domain.ServiceNode, len(entry.Peers))
			copy(out, entry.Peers)
			return out, nil
		}
		return nil, err
	}

	p.mu.Lock()
	p.peerCache[key] = peerCacheEntry{
		Peers:     peers,
		Timestamp: time.Now(),
	}
	p.mu.Unlock()

	return peers, nil
}

func (p *GraphPoller) poll(ctx context.Context) {
	// 1. Poll Health
	h, err := p.client.GetHealth(ctx)
	if err != nil {
		p.logger.Warn("poller: failed to get graph health", "error", err)
	} else {
		p.mu.Lock()
		p.health = h
		p.lastHealthPoll = time.Now()
		p.mu.Unlock()
	}

	// 2. Poll Services (The "Chain" Start)
	svcs, err := p.client.GetServices(ctx)
	if err != nil {
		p.logger.Warn("poller: failed to get services list", "error", err)
	} else {
		p.mu.Lock()
		p.services = make(map[string]domain.ServiceNode, len(svcs))
		for _, s := range svcs {
			p.services[s.ID()] = s
		}
		p.lastSvcPoll = time.Now()
		p.mu.Unlock()
	}

	// 3. Poll Centrality
	c, err := p.client.GetCentrality(ctx)
	if err != nil {
		p.logger.Warn("poller: failed to get centrality", "error", err)
	} else {
		p.mu.Lock()
		p.centrality = c
		p.lastCentPoll = time.Now()
		p.mu.Unlock()
	}
}

func (p *GraphPoller) GetHealth(ctx context.Context) (domain.GraphHealth, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.health, nil
}

func (p *GraphPoller) GetCentrality(ctx context.Context) (map[string]domain.Centrality, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	// Return copy to avoid race conditions
	out := make(map[string]domain.Centrality, len(p.centrality))
	for k, v := range p.centrality {
		out[k] = v
	}
	return out, nil
}

// Pass-through for other methods, or implement specific caching if needed
func (p *GraphPoller) GetServices(ctx context.Context) ([]domain.ServiceNode, error) {
	p.mu.RLock()
	// Removed defer p.mu.RUnlock() to avoid double unlock on fallback path

	if len(p.services) == 0 {
		// Cold start fallback
		p.mu.RUnlock()
		return p.client.GetServices(ctx)
	}

	out := make([]domain.ServiceNode, 0, len(p.services))
	for _, s := range p.services {
		out = append(out, s)
	}
	p.mu.RUnlock()
	return out, nil
}

func (p *GraphPoller) GetNeighborhood(ctx context.Context, svc domain.ServiceNode, k int) ([]domain.ServiceNode, error) {
	return p.client.GetNeighborhood(ctx, svc, k)
}
