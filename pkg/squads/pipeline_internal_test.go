package squads

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockingSynthesizer struct {
	starts  chan struct{}
	release chan struct{}
}

func (s *blockingSynthesizer) Synthesize(context.Context, string, string) error {
	s.starts <- struct{}{}
	<-s.release
	return nil
}

func (s *blockingSynthesizer) LastSynthesizedContent() string {
	return ""
}

func TestObservedSummaryPreservesUTF8(t *testing.T) {
	summary := observedSummary(strings.Repeat("á", 121))
	if !strings.HasSuffix(summary, "...") {
		t.Fatalf("summary %q does not include truncation suffix", summary)
	}
	if strings.ToValidUTF8(summary, "") != summary {
		t.Fatalf("summary %q is not valid UTF-8", summary)
	}
}

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

func TestResolveRootThreadStopsAtParentCycle(t *testing.T) {
	bus := newConnectedBus(t)
	pipeline := NewSquadsPipeline(bus, nil, 1)
	bus.ParentThreads().Set("thread-a", "thread-b")
	bus.ParentThreads().Set("thread-b", "thread-a")

	resolved := make(chan string, 1)
	go func() {
		resolved <- pipeline.resolveRootThread("thread-a")
	}()

	select {
	case rootThreadID := <-resolved:
		if rootThreadID != "thread-a" {
			t.Fatalf("root thread ID = %q, want thread-a", rootThreadID)
		}
	case <-time.After(time.Second):
		t.Fatal("root thread resolution did not terminate for a parent cycle")
	}
}

func TestGlobalQuiescenceStartsFinalSynthesisOnce(t *testing.T) {
	bus := newConnectedBus(t)
	synthesizer := &blockingSynthesizer{
		starts:  make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	pipeline := NewSquadsPipeline(bus, synthesizer, 1)
	pipeline.threadToSquadMap["root-thread"] = "squad-1"
	defer close(synthesizer.release)

	go pipeline.checkGlobalQuiescence(context.Background(), "root-thread")
	go pipeline.checkGlobalQuiescence(context.Background(), "root-thread")

	select {
	case <-synthesizer.starts:
	case <-time.After(time.Second):
		t.Fatal("final synthesis did not start")
	}

	select {
	case <-synthesizer.starts:
		t.Fatal("final synthesis started more than once for the same root thread")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestQueryTimeoutReleasesExecutionState(t *testing.T) {
	bus := newConnectedBus(t)
	pipeline := NewSquadsPipeline(bus, nil, 1)
	squad := NewSquad("squad-1", "Squad", "", bus)
	agent := NewSubAgent("agent-1", "Agent", "", "", bus, squad.SquadID)
	agent.LLMCall = func(ctx context.Context, _ string, _ string, _ []map[string]any) (LLMResponse, error) {
		<-ctx.Done()
		return LLMResponse{}, ctx.Err()
	}
	squad.RegisterSubAgent(agent)
	pipeline.RegisterSquad(squad)

	_, err := pipeline.Query(context.Background(), "timeout-root", []string{squad.SquadID}, "wait", 10*time.Millisecond)
	if err == nil {
		t.Fatal("Query returned nil error after its timeout elapsed")
	}

	if parentThreads := bus.ParentThreads().Keys(); len(parentThreads) != 0 {
		t.Fatalf("parent threads = %v, want none after timeout cleanup", parentThreads)
	}
	if _, exists := pipeline.threadToSquadMap["timeout-root"]; exists {
		t.Fatal("pipeline retained the timed-out root thread")
	}
	if len(pipeline.iterationCounter) != 0 || len(pipeline.exceptions) != 0 {
		t.Fatal("pipeline retained timed-out execution counters or exceptions")
	}
}

func TestSquadCompletionStopsAtMaximumIterations(t *testing.T) {
	pipeline := NewSquadsPipeline(newConnectedBus(t), nil, 1)
	pipeline.threadToSquadMap["root-thread"] = "squad-1"

	var wg sync.WaitGroup
	wg.Add(1)
	pipeline.completionEvents["root-thread"] = &wg

	pipeline.onSquadExecutionComplete(context.Background(), "root-thread")
	pipeline.onSquadExecutionComplete(context.Background(), "root-thread")
	waitForCompletion(t, &wg)

	if got := pipeline.exceptions["root-thread"]; got == nil || !strings.Contains(got.Error(), "maximum iteration limit") {
		t.Fatalf("iteration exception = %v, want maximum iteration limit error", got)
	}
}

func TestReleaseExecutionStateRemovesCompletedThreadTree(t *testing.T) {
	rootThread := "thread-root"
	squadThread := "thread-squad"
	agentThread := "thread-agent"
	otherRootThread := "thread-other-root"
	otherSquadThread := "thread-other-squad"
	parents := NewParentThreadMap()
	parents.Set(squadThread, rootThread)
	parents.Set(agentThread, squadThread)
	parents.Set(otherSquadThread, otherRootThread)

	pipeline := NewSquadsPipeline(nil, nil, 1)
	pipeline.threadToSquadMap[rootThread] = "squad-1"
	pipeline.threadToSquadMap[squadThread] = "squad-1"
	pipeline.iterationCounter[rootThread] = 2
	pipeline.exceptions[rootThread] = context.DeadlineExceeded
	pipeline.threadToSquadMap[otherSquadThread] = "squad-2"
	pipeline.iterationCounter[otherRootThread] = 1

	squad := NewSquad("squad-1", "Squad", "", nil)
	squad.RegisterSquadRun(squadThread, rootThread)
	squad.threadToSquadMap[rootThread] = "squad-1"
	squad.threadToSquadMap[squadThread] = "squad-1"
	squad.incrementActive(squadThread)
	squad.decrementActive(squadThread)
	squad.RegisterSquadRun(otherSquadThread, otherRootThread)
	squad.threadToSquadMap[otherSquadThread] = "squad-1"
	squad.incrementActive(otherSquadThread)

	completedThreadIDs := parents.DeleteTree(rootThread)
	pipeline.releaseExecutionState(completedThreadIDs)
	squad.releaseThreads(completedThreadIDs)

	if got := parents.Keys(); len(got) != 1 || got[0] != otherSquadThread {
		t.Fatalf("unexpected parent thread relationships after cleanup: %v", got)
	}
	if len(pipeline.threadToSquadMap) != 1 || len(pipeline.iterationCounter) != 1 || len(pipeline.exceptions) != 0 {
		t.Fatal("pipeline execution state was not released without affecting another query")
	}
	if len(squad.squadRuns) != 1 || len(squad.threadToSquadMap) != 1 || len(squad.activeExecutions) != 1 {
		t.Fatal("squad execution state was not released without affecting another query")
	}
}

func TestActiveTransversalsManifestoWaitsForRunnerLock(t *testing.T) {
	pipeline := NewSquadsPipeline(nil, nil, 1)
	pipeline.Runner.mu.Lock()

	manifestoReady := make(chan struct{})
	go func() {
		_ = pipeline.getActiveTransversalsManifesto()
		close(manifestoReady)
	}()

	select {
	case <-manifestoReady:
		t.Fatal("manifesto snapshot did not wait for the runner lock")
	case <-time.After(10 * time.Millisecond):
	}

	pipeline.Runner.mu.Unlock()

	select {
	case <-manifestoReady:
	case <-time.After(time.Second):
		t.Fatal("manifesto snapshot did not complete after the runner lock was released")
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
