package observability_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/embention/agent-squad-go/pkg/observability"
)

func TestJSONFileLoaderWaitsForCompleteLinesAndReportsMalformedInput(t *testing.T) {
	tracePath := filepath.Join(t.TempDir(), "trace.jsonl")
	step := observability.AgentStep{
		StepID:        "step-partial",
		CorrelationID: "thread-partial",
		TraceID:       "trace-partial",
		SpanID:        "span-partial",
		Kind:          observability.StepResponded,
		ThreadID:      "thread-partial",
		StartedAt:     time.Now(),
	}
	encoded, err := json.Marshal(step)
	if err != nil {
		t.Fatalf("marshal step: %v", err)
	}
	if err := os.WriteFile(tracePath, encoded[:len(encoded)/2], 0o600); err != nil {
		t.Fatalf("write partial trace: %v", err)
	}

	loader := &observability.JSONFileLoader{Path: tracePath}
	ledger := observability.NewStepLedger(nil)
	if err := loader.Sync(ledger); err != nil {
		t.Fatalf("sync partial trace: %v", err)
	}
	if got := ledger.Timeline(step.CorrelationID); len(got) != 0 {
		t.Fatalf("partial line was replayed prematurely: %d steps", len(got))
	}

	file, err := os.OpenFile(tracePath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open trace for completion: %v", err)
	}
	if _, err := file.Write(append(encoded[len(encoded)/2:], '\n')); err != nil {
		_ = file.Close()
		t.Fatalf("complete trace line: %v", err)
	}
	_ = file.Close()

	if err := loader.Sync(ledger); err != nil {
		t.Fatalf("sync completed trace: %v", err)
	}
	if got := ledger.Timeline(step.CorrelationID); len(got) != 1 {
		t.Fatalf("expected one replayed step, got %d", len(got))
	}
	if err := loader.Sync(ledger); err != nil {
		t.Fatalf("repeat sync: %v", err)
	}
	if got := ledger.Timeline(step.CorrelationID); len(got) != 1 {
		t.Fatalf("repeat sync duplicated step, got %d", len(got))
	}

	file, err = os.OpenFile(tracePath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open trace for malformed line: %v", err)
	}
	if _, err := file.WriteString("not-json\n"); err != nil {
		_ = file.Close()
		t.Fatalf("write malformed line: %v", err)
	}
	_ = file.Close()
	if err := loader.Sync(ledger); err != nil {
		t.Fatalf("sync malformed trace: %v", err)
	}
	if got := loader.Diagnostics().MalformedLines; got != 1 {
		t.Fatalf("malformed lines = %d, want 1", got)
	}
}
