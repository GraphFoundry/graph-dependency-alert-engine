package eventbus

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestBus_ConcurrentPublish_NoRace(t *testing.T) {
	b := New()
	ctx := context.Background()

	var gotMu sync.Mutex
	got := 0

	unsub := b.Subscribe("T", func(ctx context.Context, payload any) {
		gotMu.Lock()
		got++
		gotMu.Unlock()
	})
	defer unsub()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.Publish(ctx, "T", 1)
		}()
	}
	wg.Wait()

	time.Sleep(50 * time.Millisecond) // allow async handlers to run

	gotMu.Lock()
	defer gotMu.Unlock()
	if got == 0 {
		t.Fatalf("expected some events processed, got=%d", got)
	}
}
