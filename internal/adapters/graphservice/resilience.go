package graphservice

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"
)

type breakerState int

const (
	stateClosed breakerState = iota
	stateOpen
	stateHalfOpen
)

type CircuitBreaker struct {
	mu         sync.Mutex
	state      breakerState
	failCount  int
	openedAt   time.Time
	failThresh int
	openFor    time.Duration
}

func NewCircuitBreaker(failThresh int, openFor time.Duration) *CircuitBreaker {
	if failThresh < 1 {
		failThresh = 3
	}
	if openFor <= 0 {
		openFor = 5 * time.Second
	}
	return &CircuitBreaker{state: stateClosed, failThresh: failThresh, openFor: openFor}
}

func (cb *CircuitBreaker) Allow(now time.Time) bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case stateOpen:
		if now.Sub(cb.openedAt) >= cb.openFor {
			cb.state = stateHalfOpen
			return true
		}
		return false
	case stateHalfOpen, stateClosed:
		return true
	default:
		return true
	}
}

func (cb *CircuitBreaker) OnSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failCount = 0
	cb.state = stateClosed
}

func (cb *CircuitBreaker) OnFailure(now time.Time) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failCount++
	if cb.failCount >= cb.failThresh {
		cb.state = stateOpen
		cb.openedAt = now
	}
}

func shouldRetry(resp *http.Response, err error) bool {
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return true
		}
		// basic transient net errors
		return true
	}
	if resp == nil {
		return true
	}
	if resp.StatusCode >= 500 {
		return true
	}
	if resp.StatusCode == 429 {
		return true
	}
	return false
}

func backoff(attempt int) time.Duration {
	// exponential-ish, capped
	d := time.Duration(50*(1<<attempt)) * time.Millisecond
	if d > 800*time.Millisecond {
		return 800 * time.Millisecond
	}
	return d
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
