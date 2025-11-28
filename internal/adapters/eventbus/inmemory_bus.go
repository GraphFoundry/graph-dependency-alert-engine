package eventbus

import (
	"context"
	"sync"

	"graph-alert-engine/internal/core/ports"
)

type InMemoryBus struct {
	mu   sync.RWMutex
	subs map[string]map[int]ports.EventHandler
	next int
}

func New() *InMemoryBus {
	return &InMemoryBus{
		subs: make(map[string]map[int]ports.EventHandler),
	}
}

func (b *InMemoryBus) Publish(ctx context.Context, topic string, payload any) error {
	b.mu.RLock()
	handlers := make([]ports.EventHandler, 0)
	if m, ok := b.subs[topic]; ok {
		for _, h := range m {
			handlers = append(handlers, h)
		}
	}
	b.mu.RUnlock()

	// Fire handlers async so publishers never block forever.
	for _, h := range handlers {
		hh := h
		go hh(ctx, payload)
	}
	return nil
}

func (b *InMemoryBus) Subscribe(topic string, handler ports.EventHandler) (unsubscribe func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.subs[topic] == nil {
		b.subs[topic] = make(map[int]ports.EventHandler)
	}
	id := b.next
	b.next++
	b.subs[topic][id] = handler

	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.subs[topic] != nil {
			delete(b.subs[topic], id)
		}
	}
}
