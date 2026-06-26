package squads

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSignalCompletionIsIdempotent(t *testing.T) {
	pipeline := NewSquadsPipeline(nil, nil, 1)
	var callbackCount int32
	pipeline.completionCallback = func(threadID string, squadID string) {
		atomic.AddInt32(&callbackCount, 1)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	pipeline.completionEvents["root-thread"] = &wg

	pipeline.signalCompletion("root-thread", "squad-1")
	waitForCompletion(t, &wg)

	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("second completion signal panicked: %v", recovered)
			}
		}()
		pipeline.signalCompletion("root-thread", "squad-1")
	}()

	if got := atomic.LoadInt32(&callbackCount); got != 1 {
		t.Fatalf("completion callback count = %d, want 1", got)
	}
}

func waitForCompletion(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("completion signal was not delivered")
	}
}
