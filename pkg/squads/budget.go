package squads

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/embention/agent-squad-go/pkg/observability"
)

// ExecutionBudget limits token and USD consumption for one pipeline execution.
// Zero limits disable their respective constraints.
type ExecutionBudget struct {
	MaxTotalTokens int
	MaxCostUSD     float64
}

// Validate rejects limits that cannot represent a meaningful execution budget.
func (b ExecutionBudget) Validate() error {
	if b.MaxTotalTokens < 0 {
		return fmt.Errorf("execution budget max total tokens must not be negative")
	}
	if b.MaxCostUSD < 0 {
		return fmt.Errorf("execution budget max cost USD must not be negative")
	}
	if math.IsNaN(b.MaxCostUSD) || math.IsInf(b.MaxCostUSD, 0) {
		return fmt.Errorf("execution budget max cost USD must be finite")
	}
	return nil
}

func (b ExecutionBudget) enabled() bool {
	return b.MaxTotalTokens > 0 || b.MaxCostUSD > 0
}

func normalizeLLMResponseUsage(response LLMResponse) (LLMResponse, error) {
	if response.PromptTokens < 0 {
		return LLMResponse{}, fmt.Errorf("LLM response prompt tokens must not be negative")
	}
	if response.CompletionTokens < 0 {
		return LLMResponse{}, fmt.Errorf("LLM response completion tokens must not be negative")
	}
	if response.TotalTokens < 0 {
		return LLMResponse{}, fmt.Errorf("LLM response total tokens must not be negative")
	}
	if response.CostUSD < 0 || math.IsNaN(response.CostUSD) || math.IsInf(response.CostUSD, 0) {
		return LLMResponse{}, fmt.Errorf("LLM response cost USD must be finite and non-negative")
	}
	knownTokens := response.PromptTokens + response.CompletionTokens
	if response.TotalTokens < knownTokens {
		response.TotalTokens = knownTokens
	}
	return response, nil
}

// BudgetStatus describes the current state of an execution budget.
type BudgetStatus string

const (
	BudgetStatusDisabled  BudgetStatus = observability.BudgetStatusDisabled
	BudgetStatusAvailable BudgetStatus = observability.BudgetStatusAvailable
	BudgetStatusExhausted BudgetStatus = observability.BudgetStatusExhausted
	BudgetStatusExceeded  BudgetStatus = observability.BudgetStatusExceeded
)

// ErrExecutionBudgetExceeded is the sentinel for calls that cannot proceed
// because an execution budget was exhausted or exceeded.
var ErrExecutionBudgetExceeded = errors.New("execution budget exceeded")

// BudgetExceededError carries the exact usage and limits that rejected an LLM call.
type BudgetExceededError struct {
	Snapshot observability.ExecutionBudgetSnapshot
}

func (e *BudgetExceededError) Error() string {
	status := "exhausted"
	if e.Snapshot.Status == string(BudgetStatusExceeded) {
		status = "exceeded"
	}
	return fmt.Sprintf(
		"execution budget %s: total tokens %d/%d, cost USD %.6f/%.6f",
		status,
		e.Snapshot.TotalTokens,
		e.Snapshot.MaxTotalTokens,
		e.Snapshot.CostUSD,
		e.Snapshot.MaxCostUSD,
	)
}

func (e *BudgetExceededError) Unwrap() error {
	return ErrExecutionBudgetExceeded
}

// budgetAwarePipelineMetrics is optional so external PipelineMetrics
// implementations remain source-compatible with the existing public contract.
type budgetAwarePipelineMetrics interface {
	acquireLLMBudget(ctx context.Context) (observability.ExecutionBudgetSnapshot, func(), error)
	recordSubagentLLMUsageWithCost(squadID, agentID string, prompt, completion, total int, costUSD float64, elapsed time.Duration) (observability.ExecutionBudgetSnapshot, error)
	recordCoordinatorLLMUsageWithCost(squadID string, prompt, completion, total int, costUSD float64, elapsed time.Duration) (observability.ExecutionBudgetSnapshot, error)
	budgetFailure() error
}

func beginBudgetedLLMCall(ctx context.Context, metrics PipelineMetrics) (observability.ExecutionBudgetSnapshot, bool, func(), error) {
	budgetMetrics, ok := metrics.(budgetAwarePipelineMetrics)
	if !ok {
		return observability.ExecutionBudgetSnapshot{}, false, nil, nil
	}
	snapshot, release, err := budgetMetrics.acquireLLMBudget(ctx)
	return snapshot, true, release, err
}

func recordBudgetedSubagentLLMUsage(metrics PipelineMetrics, squadID, agentID string, response LLMResponse, elapsed time.Duration) (observability.ExecutionBudgetSnapshot, bool, error) {
	budgetMetrics, ok := metrics.(budgetAwarePipelineMetrics)
	if !ok {
		if metrics != nil {
			metrics.RecordLLMUsage(squadID, agentID, response.PromptTokens, response.CompletionTokens, response.TotalTokens, elapsed)
		}
		return observability.ExecutionBudgetSnapshot{}, false, nil
	}
	snapshot, err := budgetMetrics.recordSubagentLLMUsageWithCost(
		squadID,
		agentID,
		response.PromptTokens,
		response.CompletionTokens,
		response.TotalTokens,
		response.CostUSD,
		elapsed,
	)
	return snapshot, true, err
}

func recordBudgetedCoordinatorLLMUsage(metrics PipelineMetrics, squadID string, response LLMResponse, elapsed time.Duration) (observability.ExecutionBudgetSnapshot, bool, error) {
	budgetMetrics, ok := metrics.(budgetAwarePipelineMetrics)
	if !ok {
		if metrics != nil {
			metrics.RecordCoordinatorUsage(squadID, response.PromptTokens, response.CompletionTokens, response.TotalTokens, elapsed)
		}
		return observability.ExecutionBudgetSnapshot{}, false, nil
	}
	snapshot, err := budgetMetrics.recordCoordinatorLLMUsageWithCost(
		squadID,
		response.PromptTokens,
		response.CompletionTokens,
		response.TotalTokens,
		response.CostUSD,
		elapsed,
	)
	return snapshot, true, err
}

func budgetFailureFromMetrics(metrics PipelineMetrics) error {
	budgetMetrics, ok := metrics.(budgetAwarePipelineMetrics)
	if !ok {
		return nil
	}
	return budgetMetrics.budgetFailure()
}
