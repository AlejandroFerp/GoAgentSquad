package squads

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/embention/agent-squad-go/pkg/observability"
	"github.com/embention/agent-squad-go/pkg/synapse"
	"github.com/google/uuid"
)

// This file contains squad-level orchestration: event subscriptions, subagent
// fan-out execution, and final intra-squad coordination.

// Squad groups a set of SubAgents that run concurrently to process a trigger
// message on isolated subagent threads.
type Squad struct {
	SquadID          string
	Name             string
	Description      string
	Blackboard       BlackboardBus
	LLMCall          LLMCall
	Model            string
	SubAgents        map[string]*SubAgent
	TransversalsList []map[string]any

	taskTypes           map[string]string // squad_thread_id -> delegated task type
	mu                  sync.Mutex
	squadRuns           map[string]string // squad_thread_id -> parent_thread_id
	threadToSquadMap    map[string]string
	activeExecutions    map[string]*int32
	completionCallback  func(ctx context.Context, threadID string)
	subscribed          bool
	contextSubscription synapse.SubscriptionID
	taskSubscription    synapse.SubscriptionID
}

// NewSquad builds a Squad with the given identity and blackboard.
func NewSquad(squadID, name, description string, bb BlackboardBus) *Squad {
	return &Squad{
		SquadID:          squadID,
		Name:             name,
		Description:      description,
		Blackboard:       bb,
		SubAgents:        make(map[string]*SubAgent),
		squadRuns:        make(map[string]string),
		taskTypes:        make(map[string]string),
		threadToSquadMap: make(map[string]string),
		activeExecutions: make(map[string]*int32),
		TransversalsList: []map[string]any{},
	}
}

// RegisterSubAgent links a SubAgent to this squad and broadcasts topology.
func (s *Squad) RegisterSubAgent(agent *SubAgent) {
	s.mu.Lock()
	agent.SquadID = s.SquadID
	agent.Blackboard = s.Blackboard
	s.SubAgents[agent.AgentID] = agent
	s.mu.Unlock()
	s.broadcastTopology()
}

// SetTransversals updates the available transversal agents manifest.
func (s *Squad) SetTransversals(transversals []map[string]any) {
	s.mu.Lock()
	s.TransversalsList = append([]map[string]any(nil), transversals...)
	s.mu.Unlock()
	s.broadcastTopology()
}

// GetTopologyManifesto generates the squad topology manifest.
func (s *Squad) GetTopologyManifesto() map[string]any {
	s.mu.Lock()
	agents := s.subAgentsSnapshotLocked()
	transversals := append([]map[string]any(nil), s.TransversalsList...)
	s.mu.Unlock()

	members := make([]map[string]any, 0, len(agents))
	for _, agent := range agents {
		agent.mu.Lock()
		tools := append([]string(nil), agent.Tools...)
		agent.mu.Unlock()
		members = append(members, map[string]any{
			"agent_id":    agent.AgentID,
			"agent_type":  agent.AgentType,
			"description": agent.Description,
			"tools":       tools,
		})
	}
	return map[string]any{
		"squad_id":               s.SquadID,
		"squad_name":             s.Name,
		"description":            s.Description,
		"members":                members,
		"available_transversals": transversals,
	}
}

func (s *Squad) broadcastTopology() {
	manifesto := s.GetTopologyManifesto()
	for _, agent := range s.subAgentsSnapshot() {
		agent.UpdateTopology(manifesto)
	}
}

func (s *Squad) subAgentsSnapshot() []*SubAgent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subAgentsSnapshotLocked()
}

func (s *Squad) subAgentsSnapshotLocked() []*SubAgent {
	agents := make([]*SubAgent, 0, len(s.SubAgents))
	for _, agent := range s.SubAgents {
		agents = append(agents, agent)
	}
	sort.Slice(agents, func(i, j int) bool {
		return agents[i].AgentID < agents[j].AgentID
	})
	return agents
}

// RegisterCompletionCallback sets the callback invoked when a squad run finishes.
func (s *Squad) RegisterCompletionCallback(cb func(ctx context.Context, threadID string)) {
	s.mu.Lock()
	s.completionCallback = cb
	s.mu.Unlock()
}

func (s *Squad) notifyCompletion(ctx context.Context, threadID string) {
	s.mu.Lock()
	callback := s.completionCallback
	s.mu.Unlock()
	if callback != nil {
		callback(ctx, threadID)
	}
}

// Start subscribes the squad to ContextMessage and TaskMessage events.
func (s *Squad) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subscribed || s.Blackboard == nil {
		return
	}
	s.contextSubscription = s.Blackboard.Events().SubscribePostInsertFilter(synapse.MessageClassEventFilter(synapse.ClassContextMessage), s.onContextMessage)
	s.taskSubscription = s.Blackboard.Events().SubscribePostInsertFilter(synapse.MessageClassEventFilter(synapse.ClassTaskMessage), s.onTaskMessage)
	s.subscribed = true
}

// Stop unsubscribes the squad.
func (s *Squad) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.subscribed {
		return
	}
	s.Blackboard.Events().Unsubscribe(s.contextSubscription)
	s.Blackboard.Events().Unsubscribe(s.taskSubscription)
	s.contextSubscription = 0
	s.taskSubscription = 0
	s.subscribed = false
}

// RegisterSquadRun maps a squad thread to its parent thread.
func (s *Squad) RegisterSquadRun(squadThreadID, parentThreadID string) {
	s.mu.Lock()
	s.squadRuns[squadThreadID] = parentThreadID
	s.mu.Unlock()
}

func (s *Squad) registerDelegatedTaskRun(squadThreadID, parentThreadID, taskType string) {
	s.mu.Lock()
	s.squadRuns[squadThreadID] = parentThreadID
	s.taskTypes[squadThreadID] = taskType
	s.mu.Unlock()
}

func (s *Squad) completeDelegatedTaskRun(threadID string, runErr error) {
	s.mu.Lock()
	taskType, delegated := s.taskTypes[threadID]
	if delegated {
		delete(s.taskTypes, threadID)
	}
	s.mu.Unlock()
	if !delegated {
		return
	}

	metrics := s.Blackboard.GetMetrics(threadID)
	if metrics == nil {
		return
	}
	if runErr != nil {
		metrics.RecordTaskFailed(taskType)
		return
	}
	metrics.RecordTaskCompleted(taskType)
}

func (s *Squad) releaseThreads(threadIDs []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, threadID := range threadIDs {
		if counter, exists := s.activeExecutions[threadID]; exists && atomic.LoadInt32(counter) > 0 {
			continue
		}
		delete(s.squadRuns, threadID)
		delete(s.taskTypes, threadID)
		delete(s.threadToSquadMap, threadID)
		delete(s.activeExecutions, threadID)
	}
}

// IsSquadThread reports whether the thread is a squad thread.
func (s *Squad) IsSquadThread(threadID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.squadRuns[threadID]
	return ok
}

// GetParentThread returns the parent thread for a squad thread.
func (s *Squad) GetParentThread(threadID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.squadRuns[threadID]
}

// ActiveExecutions returns the active execution count for a thread.
func (s *Squad) ActiveExecutions(threadID string) int32 {
	s.mu.Lock()
	counter, ok := s.activeExecutions[threadID]
	s.mu.Unlock()
	if !ok {
		return 0
	}
	return atomic.LoadInt32(counter)
}

func (s *Squad) incrementActive(threadID string) int32 {
	s.mu.Lock()
	counter, ok := s.activeExecutions[threadID]
	if !ok {
		var c int32
		counter = &c
		s.activeExecutions[threadID] = counter
	}
	s.mu.Unlock()
	return atomic.AddInt32(counter, 1)
}

func (s *Squad) decrementActive(threadID string) int32 {
	s.mu.Lock()
	counter, ok := s.activeExecutions[threadID]
	s.mu.Unlock()
	if !ok {
		return 0
	}
	val := atomic.AddInt32(counter, -1)
	if val < 0 {
		atomic.StoreInt32(counter, 0)
		val = 0
	}
	return val
}

func (s *Squad) onContextMessage(ctx context.Context, msg synapse.SynapseMessage) {
	ctx = contextWithMessageTrace(ctx, msg)
	if msg.MessageClass != synapse.ClassContextMessage {
		return
	}

	if msg.Role != synapse.RoleUser {
		// Check if this thread matches a pending reply for any sub-agent.
		for _, agent := range s.subAgentsSnapshot() {
			agent.mu.Lock()
			_, pending := agent.PendingReplies[msg.ThreadID]
			agent.mu.Unlock()
			if pending {
				squadThreadID := msg.ThreadID
				replyCtx := ctx
				go func() {
					s.incrementActive(squadThreadID)
					defer func() {
						s.decrementActive(squadThreadID)
						s.notifyCompletion(replyCtx, squadThreadID)
					}()
					agent.HandleReply(replyCtx, msg.ThreadID, &msg)
				}()
				return
			}
		}
		return
	}

	// Ignore user messages on private subagent threads.
	if len(msg.ThreadID) > 13 && msg.ThreadID[:13] == "thread-agent-" {
		return
	}

	squadID := msg.SquadID
	if squadID == "" {
		if s.IsSquadThread(msg.ThreadID) {
			squadID = s.SquadID
		} else {
			s.mu.Lock()
			squadID = s.threadToSquadMap[msg.ThreadID]
			s.mu.Unlock()
		}
	}

	if squadID == s.SquadID {
		s.mu.Lock()
		s.threadToSquadMap[msg.ThreadID] = squadID
		s.mu.Unlock()
		runCtx := ctx
		go func() {
			s.incrementActive(msg.ThreadID)
			defer func() {
				s.decrementActive(msg.ThreadID)
				s.notifyCompletion(runCtx, msg.ThreadID)
			}()
			runErr := s.Run(runCtx, msg.ThreadID, &msg)
			s.completeDelegatedTaskRun(msg.ThreadID, runErr)
			if runErr != nil {
				observedLogger(runCtx, s.Blackboard).Error("squad run failed",
					"squad_id", s.SquadID,
					"thread_id", msg.ThreadID,
					"error", runErr,
				)
			}
		}()
	}
}

func (s *Squad) onTaskMessage(ctx context.Context, msg synapse.SynapseMessage) {
	ctx = contextWithMessageTrace(ctx, msg)
	if msg.MessageClass != synapse.ClassTaskMessage {
		return
	}
	if msg.SquadID != s.SquadID {
		return
	}

	consumed, err := s.Blackboard.ConsumeTask(ctx, msg.ThreadID, s.SquadID, "", 1)
	if err != nil || len(consumed) == 0 {
		return
	}

	for _, task := range consumed {
		metrics := s.Blackboard.GetMetrics(task.ThreadID)
		if metrics != nil {
			metrics.RecordTaskStarted(task.TaskType())
		}
		squadThreadID := "thread-squad-" + s.SquadID + "-" + uuid.NewString()
		s.registerDelegatedTaskRun(squadThreadID, task.ReplyToThread(), task.TaskType())
		s.Blackboard.ParentThreads().Set(squadThreadID, task.ReplyToThread())

		parentMetrics := metrics
		if parentMetrics == nil {
			parentMetrics = s.Blackboard.GetMetrics(task.ReplyToThread())
		}
		if parentMetrics != nil {
			if _, ok := s.Blackboard.Metrics()[squadThreadID]; !ok {
				s.Blackboard.SetMetrics(squadThreadID, parentMetrics)
			}
		}

		query := ""
		if p := task.Parameters(); p != nil {
			if q, ok := p["query"].(string); ok {
				query = q
			}
		}
		userMsg := synapse.NewContextMessage(squadThreadID, "user-client", synapse.RoleUser, query, s.SquadID, nil, time.Hour)
		if err := deliverObserved(ctx, s.Blackboard, userMsg, "task_type", task.TaskType()); err != nil {
			s.completeDelegatedTaskRun(squadThreadID, err)
			continue
		}
	}
}

// Run executes all registered sub-agents concurrently on isolated threads.
func (s *Squad) Run(ctx context.Context, squadThreadID string, triggerMsg *synapse.SynapseMessage) error {
	ctx, span := startObservedSpan(ctx, s.Blackboard, "squad.run",
		observability.Attr{Key: observability.AttrSquadID, Value: s.SquadID},
		observability.Attr{Key: observability.AttrThreadID, Value: squadThreadID},
	)
	defer span.End()
	metrics := s.Blackboard.GetMetrics(squadThreadID)
	if metrics != nil {
		metrics.RecordSquadStart(s.SquadID)
	}

	var wg sync.WaitGroup
	var agentErrsMu sync.Mutex
	var agentErrs []error
	for _, agent := range s.subAgentsSnapshot() {
		wg.Add(1)
		go func(agent *SubAgent) {
			defer wg.Done()
			if err := s.runAgentFlow(ctx, agent, squadThreadID, triggerMsg); err != nil {
				agentErrsMu.Lock()
				agentErrs = append(agentErrs, err)
				agentErrsMu.Unlock()
			}
		}(agent)
	}
	wg.Wait()

	if metrics != nil {
		metrics.RecordSquadEnd(s.SquadID)
	}
	return errors.Join(agentErrs...)
}

// runAgentFlow prepares an isolated thread for agent and executes it. It fails
// when the agent's context message cannot be delivered, because the agent would
// otherwise run against a thread that never received its input.
func (s *Squad) runAgentFlow(ctx context.Context, agent *SubAgent, squadThreadID string, triggerMsg *synapse.SynapseMessage) error {
	subagentThreadID := "thread-agent-" + agent.AgentID + "-" + uuid.NewString()
	s.Blackboard.ParentThreads().Set(subagentThreadID, squadThreadID)

	parentMetrics := s.Blackboard.GetMetrics(squadThreadID)
	if parentMetrics != nil {
		if _, ok := s.Blackboard.Metrics()[subagentThreadID]; !ok {
			s.Blackboard.SetMetrics(subagentThreadID, parentMetrics)
		}
	}

	for _, hook := range agent.PreRunHooks {
		if err := hook.Execute(ctx, agent, subagentThreadID, triggerMsg); err != nil {
			observedLogger(ctx, s.Blackboard).Error("subagent pre-run hook failed",
				"agent_id", agent.AgentID,
				"thread_id", subagentThreadID,
				"error", err,
			)
		}
	}

	queryContent := ""
	if triggerMsg != nil {
		queryContent = triggerMsg.Content()
	}

	userMsg := synapse.NewContextMessage(subagentThreadID, "user-client", synapse.RoleUser, queryContent, s.SquadID, nil, time.Hour)
	if err := deliverObserved(ctx, s.Blackboard, userMsg, "target_agent_id", agent.AgentID); err != nil {
		return fmt.Errorf("deliver context to agent %s: %w", agent.AgentID, err)
	}

	return agent.ExecuteInstrumented(ctx, subagentThreadID, &userMsg, squadThreadID)
}

// DoCoordination compiles responses from subagents and forwards a cohesive
// response to the parent thread.
func (s *Squad) DoCoordination(ctx context.Context, squadThreadID, parentThreadID string) error {
	ctx, span := startObservedSpan(ctx, s.Blackboard, "squad.coordination",
		observability.Attr{Key: observability.AttrSquadID, Value: s.SquadID},
		observability.Attr{Key: observability.AttrThreadID, Value: squadThreadID},
	)
	defer span.End()
	messages, err := s.Blackboard.FetchContext(ctx, squadThreadID, 100)
	if err != nil {
		return err
	}

	agents := s.subAgentsSnapshot()
	registeredAgents := make(map[string]struct{}, len(agents))
	for _, agent := range agents {
		registeredAgents[agent.AgentID] = struct{}{}
	}

	var assistantMsgs []synapse.SynapseMessage
	for _, msg := range messages {
		if msg.Role == synapse.RoleAssistant {
			if _, ok := registeredAgents[msg.AgentID]; ok {
				assistantMsgs = append(assistantMsgs, msg)
			}
		}
	}

	if len(assistantMsgs) == 0 {
		observedLogger(ctx, s.Blackboard).Warn("squad coordinator found no assistant messages",
			"squad_id", s.SquadID,
			"thread_id", squadThreadID,
		)
		return nil
	}

	if len(agents) <= 1 {
		for _, msg := range assistantMsgs {
			out := synapse.NewContextMessage(parentThreadID, msg.AgentID, synapse.RoleAssistant, msg.Content(), s.SquadID, nil, time.Hour)
			if err := deliverObserved(ctx, s.Blackboard, out, "parent_thread_id", parentThreadID); err != nil {
				return fmt.Errorf("deliver coordinated response to %s: %w", parentThreadID, err)
			}
		}
		return nil
	}

	llm := s.LLMCall
	model := s.Model
	if llm == nil {
		observedLogger(ctx, s.Blackboard).Warn("squad coordinator LLM is not configured; returning ordered subagent responses",
			"squad_id", s.SquadID,
			"thread_id", squadThreadID,
		)
	}

	var joinedParts []string
	for _, msg := range assistantMsgs {
		joinedParts = append(joinedParts, fmt.Sprintf("[%s]: %s", msg.AgentID, msg.Content()))
	}
	joinedResponses := strings.Join(joinedParts, "\n\n")

	summary := joinedResponses
	if llm != nil {
		coordinatorPrompt := fmt.Sprintf(
			"You are the coordinator for squad '%s'.\n"+
				"Below are the final responses from the squad's subagents:\n"+
				"%s\n\n"+
				"Please compile and summarize these responses into a single, cohesive final answer. "+
				"Do not add any preamble or conversational filler. Output only the summarized final answer.",
			s.Name, joinedResponses)
		res, err := s.callCoordinatorLLMObserved(ctx, squadThreadID, coordinatorPrompt)
		if err != nil {
			observedLogger(ctx, s.Blackboard).Warn("squad coordinator llm call failed; falling back to joined responses",
				"squad_id", s.SquadID,
				"thread_id", squadThreadID,
				"model", model,
				"error", err,
			)
			if errors.Is(err, ErrExecutionBudgetExceeded) {
				return fmt.Errorf("coordinate squad %q: %w", s.SquadID, err)
			}
		} else if res.Content != "" {
			summary = strings.TrimSpace(res.Content)
		}
	}
	stepTime := time.Now()
	ctx, _ = recordObservedStep(ctx, s.Blackboard, observability.AgentStep{
		Kind:       observability.StepSynthesis,
		AgentID:    s.SquadID + "-coordinator",
		SquadID:    s.SquadID,
		ThreadID:   parentThreadID,
		Summary:    observedSummary(summary),
		StartedAt:  stepTime,
		FinishedAt: stepTime,
	})

	out := synapse.NewContextMessage(parentThreadID, s.SquadID+"-coordinator", synapse.RoleAssistant, summary, s.SquadID, nil, time.Hour)
	if err := deliverObserved(ctx, s.Blackboard, out, "parent_thread_id", parentThreadID); err != nil {
		return fmt.Errorf("deliver coordinated summary to %s: %w", parentThreadID, err)
	}
	return nil
}

func (s *Squad) callCoordinatorLLMObserved(ctx context.Context, threadID, systemPrompt string) (LLMResponse, error) {
	coordinatorID := s.SquadID + "-coordinator"
	return invokeObservedLLMCall(
		ctx,
		s.Blackboard,
		s.LLMCall,
		observedLLMCall{
			SpanName:     "squad.coordinator.llm",
			AgentID:      coordinatorID,
			AgentType:    "COORDINATOR",
			SquadID:      s.SquadID,
			ThreadID:     threadID,
			Model:        s.Model,
			Summary:      "squad coordinator llm call",
			SystemPrompt: systemPrompt,
			Clock:        time.Now,
		},
		func(metrics PipelineMetrics, response LLMResponse, elapsed time.Duration) (observability.ExecutionBudgetSnapshot, bool, error) {
			return recordBudgetedCoordinatorLLMUsage(metrics, s.SquadID, response, elapsed)
		},
	)
}
