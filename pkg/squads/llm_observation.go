package squads

import (
	"context"
	"errors"
	"time"

	"github.com/embention/agent-squad-go/pkg/observability"
)

type observedLLMCall struct {
	SpanName     string
	AgentID      string
	AgentType    string
	SquadID      string
	ThreadID     string
	Model        string
	Summary      string
	SystemPrompt string
	Messages     []map[string]any
	Clock        func() time.Time
}

type llmUsageRecorder func(PipelineMetrics, LLMResponse, time.Duration) (observability.ExecutionBudgetSnapshot, bool, error)

func invokeObservedLLMCall(ctx context.Context, bb BlackboardBus, llmCall LLMCall, call observedLLMCall, recordUsage llmUsageRecorder) (LLMResponse, error) {
	if llmCall == nil {
		return LLMResponse{}, nil
	}
	if err := ctx.Err(); err != nil {
		return LLMResponse{}, err
	}

	llmCtx, span := startObservedSpan(ctx, bb, call.SpanName,
		observability.Attr{Key: observability.AttrAgentID, Value: call.AgentID},
		observability.Attr{Key: observability.AttrAgentType, Value: call.AgentType},
		observability.Attr{Key: observability.AttrSquadID, Value: call.SquadID},
		observability.Attr{Key: observability.AttrThreadID, Value: call.ThreadID},
		observability.Attr{Key: observability.AttrModel, Value: call.Model},
	)
	defer span.End()

	var metrics PipelineMetrics
	if bb != nil {
		metrics = bb.GetMetrics(call.ThreadID)
	}

	startedAt := time.Now()
	budget, budgetTracked, releaseBudget, budgetErr := beginBudgetedLLMCall(llmCtx, metrics)
	if budgetErr != nil {
		span.RecordError(budgetErr)
		recordObservedLLMCallStep(llmCtx, bb, call, LLMResponse{}, startedAt, time.Now(), budget, budgetTracked, budgetErr, false, false)
		return LLMResponse{}, budgetErr
	}
	if releaseBudget != nil {
		defer releaseBudget()
	}
	if err := llmCtx.Err(); err != nil {
		return LLMResponse{}, err
	}

	clock := call.Clock
	if clock == nil {
		clock = time.Now
	}
	clockStart := clock()
	response, callErr := llmCall(llmCtx, call.Model, call.SystemPrompt, call.Messages)
	elapsed := clock().Sub(clockStart)
	finishedAt := time.Now()
	normalizedResponse, usageValidationErr := normalizeLLMResponseUsage(response)
	observedResponse := response
	if usageValidationErr == nil {
		response = normalizedResponse
		observedResponse = response
	} else {
		observedResponse.PromptTokens = 0
		observedResponse.CompletionTokens = 0
		observedResponse.TotalTokens = 0
		observedResponse.CostUSD = 0
	}

	if recordUsage != nil && usageValidationErr == nil {
		usageBudget, usageTracked, usageErr := recordUsage(metrics, response, elapsed)
		if usageTracked {
			budget = usageBudget
			budgetTracked = true
		}
		budgetErr = usageErr
	}
	err := errors.Join(callErr, usageValidationErr, budgetErr)
	if err != nil {
		span.RecordError(err)
	}
	recordObservedLLMCallStep(llmCtx, bb, call, observedResponse, startedAt, finishedAt, budget, budgetTracked, err, true, true)
	return response, err
}

func recordObservedLLMCallStep(ctx context.Context, bb BlackboardBus, call observedLLMCall, response LLMResponse, startedAt, finishedAt time.Time, budget observability.ExecutionBudgetSnapshot, budgetTracked bool, err error, captureTrace, providerInvoked bool) {
	var budgetSnapshot *observability.ExecutionBudgetSnapshot
	if budgetTracked {
		budgetCopy := budget
		budgetSnapshot = &budgetCopy
	}
	stepKind := observability.StepLLMCall
	summary := call.Summary
	if !providerInvoked {
		stepKind = observability.StepError
		summary = "LLM call blocked by execution budget"
	}
	step := observability.AgentStep{
		Kind:       stepKind,
		AgentID:    call.AgentID,
		AgentType:  call.AgentType,
		SquadID:    call.SquadID,
		ThreadID:   call.ThreadID,
		Summary:    summary,
		Model:      call.Model,
		TokensIn:   response.PromptTokens,
		TokensOut:  response.CompletionTokens,
		CostUSD:    response.CostUSD,
		Budget:     budgetSnapshot,
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
	}
	if captureTrace {
		step.LLMTrace = captureLLMTrace(bb, call.SystemPrompt, call.Messages, response)
	}
	if err != nil {
		step.Error = err.Error()
	}
	_, _ = recordObservedStep(ctx, bb, step)
}
