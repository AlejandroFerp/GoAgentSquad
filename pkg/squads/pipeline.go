package squads

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/embention/agent-squad-go/pkg/observability"
	"github.com/embention/agent-squad-go/pkg/synapse"
	"github.com/google/uuid"
)

// This file hosts the top-level pipeline coordinator that routes queries,
// monitors quiescence, enforces iteration/timeout limits, and returns results.

// FinalSynthesizer is the interface for the final compilation step that runs
// when the entire execution tree reaches quiescence.
type FinalSynthesizer interface {
	Synthesize(ctx context.Context, threadID, squadID string) error
	LastSynthesizedContent() string
}

// TransversalRunner manages transversal agents and dispatches TaskMessages to
// the first agent that matches the task type.
type TransversalRunner struct {
	Blackboard    BlackboardBus
	Agents        map[string]*TransversalAgent
	CapabilityMap map[string][]string
	mu            sync.Mutex
	subscribed    bool
}

// NewTransversalRunner builds an empty runner.
func NewTransversalRunner(bb BlackboardBus) *TransversalRunner {
	return &TransversalRunner{
		Blackboard:    bb,
		Agents:        make(map[string]*TransversalAgent),
		CapabilityMap: make(map[string][]string),
	}
}

// RegisterAgent maps a transversal agent's capabilities.
func (r *TransversalRunner) RegisterAgent(agent *TransversalAgent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Agents[agent.AgentID] = agent
	for _, cap := range agent.Capabilities {
		r.CapabilityMap[cap] = append(r.CapabilityMap[cap], agent.AgentID)
	}
}

// Start subscribes the runner to post_insert TaskMessage events.
func (r *TransversalRunner) Start() {
	if r.subscribed {
		return
	}
	r.Blackboard.Events().Subscribe("TaskMessage", synapse.PostInsertCallback(r.onPostInsert), "post_insert")
	r.subscribed = true
}

// Stop unsubscribes the runner.
func (r *TransversalRunner) Stop() {
	if !r.subscribed {
		return
	}
	r.Blackboard.Events().Unsubscribe(synapse.PostInsertCallback(r.onPostInsert))
	r.subscribed = false
}

func (r *TransversalRunner) onPostInsert(ctx context.Context, msg synapse.SynapseMessage) {
	ctx = contextWithMessageTrace(ctx, msg)
	if msg.MessageClass != synapse.ClassTaskMessage {
		return
	}
	taskType := msg.TaskType()
	r.mu.Lock()
	agentIDs, ok := r.CapabilityMap[taskType]
	r.mu.Unlock()
	if !ok || len(agentIDs) == 0 {
		return
	}

	consumed, err := r.Blackboard.ConsumeTask(ctx, msg.ThreadID, "", taskType, 1)
	if err != nil || len(consumed) == 0 {
		return
	}

	for _, task := range consumed {
		r.mu.Lock()
		agentIDs = r.CapabilityMap[task.TaskType()]
		r.mu.Unlock()
		if len(agentIDs) == 0 {
			continue
		}
		r.mu.Lock()
		agent := r.Agents[agentIDs[0]]
		r.mu.Unlock()
		if agent == nil {
			continue
		}
		taskCopy := task
		taskCtx := contextWithMessageTrace(ctx, taskCopy)
		go func(a *TransversalAgent, t synapse.SynapseMessage, taskCtx context.Context) {
			defer func() {
				if rec := recover(); rec != nil {
					observedLogger(taskCtx, r.Blackboard).Error("transversal agent panicked",
						"agent_id", a.AgentID,
						"thread_id", t.ThreadID,
						"task_type", t.TaskType(),
						"panic", rec,
					)
				}
			}()
			a.ProcessTask(taskCtx, &t)
		}(agent, taskCopy, taskCtx)
	}
}

// SquadsPipeline is the top-level orchestrator that wires squads, transversals,
// observers, and the final synthesizer into a single concurrent execution engine.
type SquadsPipeline struct {
	Blackboard       BlackboardBus
	Runner           *TransversalRunner
	FinalSynthesizer FinalSynthesizer
	Squads           map[string]*Squad
	Observers        []ObserverStarter
	RouteQueryFn     func(ctx context.Context, content string) (any, error)
	MaxIterations    int

	mu                 sync.Mutex
	executions         *BoundedMap
	activeQueries      int32
	threadToSquadMap   map[string]string
	completionEvents   map[string]*sync.WaitGroup
	completionCallback func(threadID string, squadID string)
	exceptions         map[string]error
	iterationCounter   map[string]int
}

// ObserverStarter is the minimal interface for observer lifecycle management.
type ObserverStarter interface {
	Start()
	Stop()
}

// NewSquadsPipeline builds a pipeline with the given blackboard.
func NewSquadsPipeline(bb BlackboardBus, finalSynth FinalSynthesizer, maxIterations int) *SquadsPipeline {
	if maxIterations <= 0 {
		maxIterations = 15
	}
	return &SquadsPipeline{
		Blackboard:       bb,
		Runner:           NewTransversalRunner(bb),
		FinalSynthesizer: finalSynth,
		Squads:           make(map[string]*Squad),
		Observers:        []ObserverStarter{},
		executions:       NewBoundedMap(100),
		threadToSquadMap: make(map[string]string),
		completionEvents: make(map[string]*sync.WaitGroup),
		exceptions:       make(map[string]error),
		iterationCounter: make(map[string]int),
		MaxIterations:    maxIterations,
	}
}

// Start starts observers, transversals, and squads.
func (p *SquadsPipeline) Start() {
	for _, obs := range p.Observers {
		obs.Start()
	}
	p.Runner.Start()
	for _, squad := range p.Squads {
		squad.Start()
	}
}

// Stop stops observers, transversals, and squads.
func (p *SquadsPipeline) Stop() {
	for _, obs := range p.Observers {
		obs.Stop()
	}
	for _, squad := range p.Squads {
		squad.Stop()
	}
	p.Runner.Stop()
}

// RegisterSquad registers an instantiated Squad.
func (p *SquadsPipeline) RegisterSquad(squad *Squad) {
	if squad.Blackboard == nil {
		squad.Blackboard = p.Blackboard
	}
	for _, agent := range squad.SubAgents {
		if agent.Blackboard == nil {
			agent.Blackboard = p.Blackboard
		}
	}
	squad.RegisterCompletionCallback(p.onSquadExecutionComplete)
	p.Squads[squad.SquadID] = squad
	p.broadcastGlobalTopology()
}

// RegisterTransversal registers a transversal agent into the runner.
func (p *SquadsPipeline) RegisterTransversal(agent *TransversalAgent) {
	if agent.Blackboard == nil {
		agent.Blackboard = p.Blackboard
	}
	p.Runner.RegisterAgent(agent)
	manifesto := p.getActiveTransversalsManifesto()
	for _, squad := range p.Squads {
		squad.SetTransversals(manifesto)
	}
	p.broadcastGlobalTopology()
}

// RegisterObserver registers a middleware observer.
func (p *SquadsPipeline) RegisterObserver(obs ObserverStarter) {
	p.Observers = append(p.Observers, obs)
}

// GetMetrics retrieves the metrics report for a thread.
func (p *SquadsPipeline) GetMetrics(threadID string) map[string]any {
	if v, ok := p.executions.Get(threadID); ok {
		if pm, ok := v.(PipelineMetrics); ok {
			return pm.ToDict()
		}
	}
	return nil
}

func (p *SquadsPipeline) broadcastGlobalTopology() {
	squadsMeta := make([]map[string]any, 0, len(p.Squads))
	for _, squad := range p.Squads {
		squadsMeta = append(squadsMeta, map[string]any{
			"squad_id":    squad.SquadID,
			"name":        squad.Name,
			"description": squad.Description,
		})
	}
	transversalsMeta := p.getActiveTransversalsManifesto()
	for _, squad := range p.Squads {
		for _, agent := range squad.SubAgents {
			otherSquads := make([]map[string]any, 0, len(squadsMeta))
			for _, sm := range squadsMeta {
				if sm["squad_id"] != squad.SquadID {
					otherSquads = append(otherSquads, sm)
				}
			}
			agent.UpdateGlobalTopology(otherSquads, transversalsMeta)
		}
	}
}

func (p *SquadsPipeline) getActiveTransversalsManifesto() []map[string]any {
	out := make([]map[string]any, 0, len(p.Runner.Agents))
	for _, agent := range p.Runner.Agents {
		caps := make([]any, 0, len(agent.Capabilities))
		for _, c := range agent.Capabilities {
			caps = append(caps, c)
		}
		out = append(out, map[string]any{
			"agent_id":     agent.AgentID,
			"agent_type":   agent.AgentType,
			"description":  agent.Description,
			"capabilities": caps,
		})
	}
	return out
}

func (p *SquadsPipeline) getTotalPendingReplies(threadID string) int {
	total := 0
	for _, squad := range p.Squads {
		for _, agent := range squad.SubAgents {
			agent.mu.Lock()
			for _, meta := range agent.PendingReplies {
				if rt, ok := meta["respond_thread_id"].(string); ok && rt == threadID {
					total++
					continue
				}
				if ot, ok := meta["original_thread"].(string); ok && ot == threadID {
					total++
				}
			}
			agent.mu.Unlock()
		}
	}
	return total
}

func (p *SquadsPipeline) resolveRootThread(threadID string) string {
	parents := p.Blackboard.ParentThreads()
	curr := threadID
	for {
		parent := parents.Get(curr)
		if parent == "" {
			break
		}
		curr = parent
	}
	return curr
}

func (p *SquadsPipeline) isTreeQuiescent(rootThread string) bool {
	treeThreads := map[string]bool{rootThread: true}
	for _, child := range p.Blackboard.ParentThreads().Keys() {
		if p.resolveRootThread(child) == rootThread {
			treeThreads[child] = true
		}
	}
	for t := range treeThreads {
		activeCount := int32(0)
		for _, squad := range p.Squads {
			activeCount += squad.ActiveExecutions(t)
		}
		pendingCount := p.getTotalPendingReplies(t)
		if activeCount > 0 || pendingCount > 0 {
			return false
		}
	}
	return true
}

func (p *SquadsPipeline) onSquadExecutionComplete(ctx context.Context, threadID string) {
	rootThread := p.resolveRootThread(threadID)

	p.mu.Lock()
	p.iterationCounter[rootThread]++
	if p.iterationCounter[rootThread] > p.MaxIterations {
		observedLogger(ctx, p.Blackboard).Error("pipeline exceeded maximum iterations",
			"thread_id", rootThread,
			"max_iterations", p.MaxIterations,
		)
		p.exceptions[rootThread] = fmt.Errorf("execution aborted: exceeded maximum iteration limit of %d steps", p.MaxIterations)
		if wg, ok := p.completionEvents[rootThread]; ok {
			wg.Done()
		}
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	activeCount := int32(0)
	for _, squad := range p.Squads {
		activeCount += squad.ActiveExecutions(threadID)
	}
	pendingCount := p.getTotalPendingReplies(threadID)

	if activeCount == 0 && pendingCount == 0 {
		var targetSquad *Squad
		var parentThread string
		for _, squad := range p.Squads {
			if squad.IsSquadThread(threadID) {
				targetSquad = squad
				parentThread = squad.GetParentThread(threadID)
				break
			}
		}
		if targetSquad != nil && parentThread != "" {
			go func(sq *Squad, coordCtx context.Context, stID, ptID string) {
				if err := sq.DoCoordination(coordCtx, stID, ptID); err != nil {
					observedLogger(coordCtx, p.Blackboard).Error("squad coordination failed",
						"squad_id", sq.SquadID,
						"thread_id", stID,
						"parent_thread_id", ptID,
						"error", err,
					)
				}
				p.checkGlobalQuiescence(coordCtx, rootThread)
			}(targetSquad, ctx, threadID, parentThread)
			return
		}
	}
	p.checkGlobalQuiescence(ctx, rootThread)
}

func (p *SquadsPipeline) checkGlobalQuiescence(ctx context.Context, rootThread string) {
	if !p.isTreeQuiescent(rootThread) {
		return
	}
	p.mu.Lock()
	squadID := p.threadToSquadMap[rootThread]
	if p.FinalSynthesizer != nil {
		go func(synthCtx context.Context) {
			if err := p.FinalSynthesizer.Synthesize(synthCtx, rootThread, squadID); err != nil {
				observedLogger(synthCtx, p.Blackboard).Error("final synthesis failed",
					"thread_id", rootThread,
					"squad_id", squadID,
					"error", err,
				)
			}
			p.signalCompletion(rootThread, squadID)
		}(ctx)
		p.mu.Unlock()
		return
	}
	p.signalCompletionLocked(rootThread, squadID)
	p.mu.Unlock()
}

func (p *SquadsPipeline) signalCompletion(rootThread, squadID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.signalCompletionLocked(rootThread, squadID)
}

func (p *SquadsPipeline) signalCompletionLocked(rootThread, squadID string) {
	if p.completionCallback != nil {
		p.completionCallback(rootThread, squadID)
	}
	if wg, ok := p.completionEvents[rootThread]; ok {
		wg.Done()
	}
}

// RouteQuery determines the initial squad ID(s) based on query content.
func (p *SquadsPipeline) RouteQuery(ctx context.Context, content string) (any, error) {
	if p.RouteQueryFn == nil {
		return nil, fmt.Errorf("route_query_fn was not provided to SquadsPipeline")
	}
	result, err := p.RouteQueryFn(ctx, content)
	if err != nil {
		return nil, err
	}
	var squadIDs []string
	switch v := result.(type) {
	case string:
		squadIDs = []string{v}
	case []string:
		squadIDs = v
	default:
		return nil, fmt.Errorf("route_query_fn returned unsupported type %T", result)
	}
	for _, sID := range squadIDs {
		if _, ok := p.Squads[sID]; !ok {
			return nil, fmt.Errorf("route_query_fn returned unregistered squad ID '%s'", sID)
		}
	}
	return result, nil
}

// QueryResult holds the output of a pipeline query.
type QueryResult struct {
	Response     string
	History      []synapse.SynapseMessage
	SquadThreads map[string]string
	Metrics      map[string]any
	Timeline     []observability.AgentStep
	Metadata     QueryMetadata
}

// QueryMetadata carries the high-level traceable metadata of a Query result.
type QueryMetadata struct {
	CorrelationID    string
	ThreadID         string
	RespondingSquads []string
	TraceID          string
	DurationMS       int64
	TotalSteps       int
}

// Query runs the full concurrent agent squads pipeline.
func (p *SquadsPipeline) Query(ctx context.Context, threadID string, initialSquadID any, content string, timeout float64) (*QueryResult, error) {
	queryStartedAt := time.Now()
	if threadID == "" {
		threadID = "thread-" + uuid.NewString()
	}
	if timeout <= 0 {
		timeout = 10.0
	}
	ctx = observability.WithTraceContext(ctx, observability.TraceContext{CorrelationID: threadID})
	ctx, rootSpan := startObservedSpan(ctx, p.Blackboard, "pipeline.query",
		observability.Attr{Key: observability.AttrCorrelationID, Value: threadID},
		observability.Attr{Key: observability.AttrThreadID, Value: threadID},
	)
	defer rootSpan.End()
	ctx, rootStep := recordObservedStep(ctx, p.Blackboard, observability.AgentStep{
		Kind:       observability.StepQueryReceived,
		ThreadID:   threadID,
		Summary:    observedSummary(content),
		StartedAt:  queryStartedAt,
		FinishedAt: queryStartedAt,
	})

	if initialSquadID == nil {
		resolved, err := p.RouteQuery(ctx, content)
		if err != nil {
			rootSpan.RecordError(err)
			now := time.Now()
			_, _ = recordObservedStep(ctx, p.Blackboard, observability.AgentStep{
				Kind:       observability.StepError,
				ThreadID:   threadID,
				Summary:    observedSummary(err.Error()),
				Error:      err.Error(),
				StartedAt:  now,
				FinishedAt: now,
			})
			return nil, err
		}
		initialSquadID = resolved
	}

	metrics := NewExecutionMetrics(threadID)
	p.Blackboard.SetMetrics(threadID, metrics)
	p.executions.Set(threadID, metrics)

	p.broadcastGlobalTopology()

	for agentID, agent := range p.Runner.Agents {
		metrics.RegisterTransversal(agentID, agent.AgentType)
	}
	for squadID, squad := range p.Squads {
		metrics.RegisterSquad(squadID, squad)
	}

	p.mu.Lock()
	p.activeQueries++
	status := "Completed"
	p.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(1)
	p.mu.Lock()
	p.completionEvents[threadID] = &wg
	p.mu.Unlock()

	defer func() {
		metrics.Finalize(status)
		obsRuntime := observedRuntime(p.Blackboard)
		if obsRuntime.Exporter != nil {
			if err := obsRuntime.Exporter.Export(ctx, obsRuntime.Ledger.Timeline(threadID)); err != nil {
				observedLogger(ctx, p.Blackboard).Error("observability export failed",
					"thread_id", threadID,
					"error", err,
				)
			}
		}
		p.mu.Lock()
		delete(p.completionEvents, threadID)
		p.activeQueries--
		if p.activeQueries == 0 {
			for _, obs := range p.Observers {
				obs.Stop()
			}
			for _, squad := range p.Squads {
				squad.Stop()
			}
			p.Runner.Stop()
		}
		p.mu.Unlock()
	}()

	p.mu.Lock()
	if p.activeQueries == 1 {
		for _, obs := range p.Observers {
			obs.Start()
		}
		p.Runner.Start()
		for _, squad := range p.Squads {
			squad.Start()
		}
	}
	p.mu.Unlock()

	var squadIDs []string
	switch v := initialSquadID.(type) {
	case string:
		squadIDs = []string{v}
	case []string:
		squadIDs = v
	default:
		return nil, fmt.Errorf("invalid initial_squad_id type %T", initialSquadID)
	}
	routeTime := time.Now()
	ctx, _ = recordObservedStep(ctx, p.Blackboard, observability.AgentStep{
		Kind:       observability.StepRouted,
		ThreadID:   threadID,
		Summary:    fmt.Sprintf("routed to squads: %s", strings.Join(squadIDs, ", ")),
		StartedAt:  routeTime,
		FinishedAt: routeTime,
	})

	synth := p.FinalSynthesizer
	if synth != nil {
		// Reset synthesis tracker if the synthesizer exposes it.
		_ = synth.LastSynthesizedContent()
	}

	for _, sID := range squadIDs {
		squadThreadID := "thread-squad-" + sID + "-" + uuid.NewString()
		p.Blackboard.ParentThreads().Set(squadThreadID, threadID)
		p.mu.Lock()
		p.threadToSquadMap[squadThreadID] = sID
		p.threadToSquadMap[threadID] = sID
		p.mu.Unlock()
		squad := p.Squads[sID]
		squad.RegisterSquadRun(squadThreadID, threadID)
		squad.mu.Lock()
		squad.threadToSquadMap[squadThreadID] = sID
		squad.threadToSquadMap[threadID] = sID
		squad.mu.Unlock()
		p.Blackboard.SetMetrics(squadThreadID, metrics)

		userMsg := synapse.NewContextMessage(squadThreadID, "user-client", synapse.RoleUser, content, sID, nil, 3600)
		_, _ = p.Blackboard.SendMessage(ctx, userMsg)
	}

	// Only short-circuit when there is nothing to dispatch. Routed queries rely
	// on asynchronous post-insert callbacks, so checking quiescence immediately
	// after enqueuing the first messages races and can finish the query before
	// any squad goroutine starts.
	if len(squadIDs) == 0 && p.isTreeQuiescent(threadID) {
		if synth == nil || synth.LastSynthesizedContent() != "" {
			wg.Done()
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Duration(timeout * float64(time.Second))):
		status = "Timeout"
		rootSpan.RecordError(fmt.Errorf("execution timed out waiting for squads pipeline to complete"))
		timeoutTime := time.Now()
		_, _ = recordObservedStep(ctx, p.Blackboard, observability.AgentStep{
			Kind:       observability.StepError,
			ThreadID:   threadID,
			Summary:    "execution timed out",
			Error:      "execution timed out waiting for squads pipeline to complete",
			StartedAt:  timeoutTime,
			FinishedAt: timeoutTime,
		})
		p.mu.Lock()
		if wg, ok := p.completionEvents[threadID]; ok {
			wg.Done()
		}
		p.mu.Unlock()
		return nil, fmt.Errorf("execution timed out waiting for squads pipeline to complete")
	}

	p.mu.Lock()
	if exc, ok := p.exceptions[threadID]; ok {
		p.mu.Unlock()
		status = "Failed"
		rootSpan.RecordError(exc)
		errTime := time.Now()
		_, _ = recordObservedStep(ctx, p.Blackboard, observability.AgentStep{
			Kind:       observability.StepError,
			ThreadID:   threadID,
			Summary:    observedSummary(exc.Error()),
			Error:      exc.Error(),
			StartedAt:  errTime,
			FinishedAt: errTime,
		})
		return nil, exc
	}
	p.mu.Unlock()

	history, _ := p.Blackboard.FetchContext(ctx, threadID, 100)

	var response string
	if synth != nil {
		response = synth.LastSynthesizedContent()
	}
	completeTime := time.Now()
	_, _ = recordObservedStep(ctx, p.Blackboard, observability.AgentStep{
		Kind:       observability.StepQuiesced,
		ThreadID:   threadID,
		Summary:    "execution tree reached quiescence",
		StartedAt:  completeTime,
		FinishedAt: completeTime,
	})
	ctx, _ = recordObservedStep(ctx, p.Blackboard, observability.AgentStep{
		Kind:       observability.StepResponded,
		ThreadID:   threadID,
		Summary:    observedSummary(response),
		StartedAt:  completeTime,
		FinishedAt: completeTime,
	})
	timeline := observedRuntime(p.Blackboard).Ledger.Timeline(threadID)

	p.mu.Lock()
	squadThreads := make(map[string]string, len(p.threadToSquadMap))
	for k, v := range p.threadToSquadMap {
		squadThreads[k] = v
	}
	p.mu.Unlock()

	return &QueryResult{
		Response:     response,
		History:      history,
		SquadThreads: squadThreads,
		Metrics:      metrics.ToDict(),
		Timeline:     timeline,
		Metadata: QueryMetadata{
			CorrelationID:    threadID,
			ThreadID:         threadID,
			RespondingSquads: append([]string(nil), squadIDs...),
			TraceID:          rootStep.TraceID,
			DurationMS:       time.Since(queryStartedAt).Milliseconds(),
			TotalSteps:       len(timeline),
		},
	}, nil
}
