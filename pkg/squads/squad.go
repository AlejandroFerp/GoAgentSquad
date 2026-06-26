package squads

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/embention/agent-squad-go/pkg/observability"
	"github.com/embention/agent-squad-go/pkg/synapse"
	"github.com/google/uuid"
)

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

	mu                 sync.Mutex
	squadRuns          map[string]string // squad_thread_id -> parent_thread_id
	threadToSquadMap   map[string]string
	activeExecutions   map[string]*int32
	completionCallback func(ctx context.Context, threadID string)
	subscribed         bool
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
		threadToSquadMap: make(map[string]string),
		activeExecutions: make(map[string]*int32),
		TransversalsList: []map[string]any{},
	}
}

// RegisterSubAgent links a SubAgent to this squad and broadcasts topology.
func (s *Squad) RegisterSubAgent(agent *SubAgent) {
	agent.SquadID = s.SquadID
	agent.Blackboard = s.Blackboard
	s.SubAgents[agent.AgentID] = agent
	s.broadcastTopology()
}

// SetTransversals updates the available transversal agents manifest.
func (s *Squad) SetTransversals(transversals []map[string]any) {
	s.TransversalsList = transversals
	s.broadcastTopology()
}

// GetTopologyManifesto generates the squad topology manifest.
func (s *Squad) GetTopologyManifesto() map[string]any {
	members := make([]map[string]any, 0, len(s.SubAgents))
	for _, agent := range s.SubAgents {
		members = append(members, map[string]any{
			"agent_id":    agent.AgentID,
			"agent_type":  agent.AgentType,
			"description": agent.Description,
			"tools":       agent.Tools,
		})
	}
	return map[string]any{
		"squad_id":               s.SquadID,
		"squad_name":             s.Name,
		"description":            s.Description,
		"members":                members,
		"available_transversals": s.TransversalsList,
	}
}

func (s *Squad) broadcastTopology() {
	manifesto := s.GetTopologyManifesto()
	for _, agent := range s.SubAgents {
		agent.UpdateTopology(manifesto)
	}
}

// RegisterCompletionCallback sets the callback invoked when a squad run finishes.
func (s *Squad) RegisterCompletionCallback(cb func(ctx context.Context, threadID string)) {
	s.completionCallback = cb
}

// Start subscribes the squad to ContextMessage and TaskMessage events.
func (s *Squad) Start() {
	if s.subscribed || s.Blackboard == nil {
		return
	}
	s.Blackboard.Events().Subscribe("ContextMessage", synapse.PostInsertCallback(s.onContextMessage), "post_insert")
	s.Blackboard.Events().Subscribe("TaskMessage", synapse.PostInsertCallback(s.onTaskMessage), "post_insert")
	s.subscribed = true
}

// Stop unsubscribes the squad.
func (s *Squad) Stop() {
	if !s.subscribed {
		return
	}
	s.Blackboard.Events().Unsubscribe(synapse.PostInsertCallback(s.onContextMessage))
	s.Blackboard.Events().Unsubscribe(synapse.PostInsertCallback(s.onTaskMessage))
	s.subscribed = false
}

// RegisterSquadRun maps a squad thread to its parent thread.
func (s *Squad) RegisterSquadRun(squadThreadID, parentThreadID string) {
	s.mu.Lock()
	s.squadRuns[squadThreadID] = parentThreadID
	s.mu.Unlock()
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
		for _, agent := range s.SubAgents {
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
						if s.completionCallback != nil {
							s.completionCallback(replyCtx, squadThreadID)
						}
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
				if s.completionCallback != nil {
					s.completionCallback(runCtx, msg.ThreadID)
				}
			}()
			if err := s.Run(runCtx, msg.ThreadID, &msg); err != nil {
				observedLogger(runCtx, s.Blackboard).Error("squad run failed",
					"squad_id", s.SquadID,
					"thread_id", msg.ThreadID,
					"error", err,
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
		squadThreadID := "thread-squad-" + s.SquadID + "-" + uuid.NewString()
		s.RegisterSquadRun(squadThreadID, task.ReplyToThread())
		s.Blackboard.ParentThreads().Set(squadThreadID, task.ReplyToThread())

		parentMetrics := s.Blackboard.GetMetrics(task.ReplyToThread())
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
		userMsg := synapse.NewContextMessage(squadThreadID, "user-client", synapse.RoleUser, query, s.SquadID, nil, 3600)
		_, _ = s.Blackboard.SendMessage(ctx, userMsg)
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
	for _, agent := range s.SubAgents {
		wg.Add(1)
		go func(agent *SubAgent) {
			defer wg.Done()
			s.runAgentFlow(ctx, agent, squadThreadID, triggerMsg)
		}(agent)
	}
	wg.Wait()

	if metrics != nil {
		metrics.RecordSquadEnd(s.SquadID)
	}
	return nil
}

func (s *Squad) runAgentFlow(ctx context.Context, agent *SubAgent, squadThreadID string, triggerMsg *synapse.SynapseMessage) {
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
		if c := triggerMsg.Content(); c != "" {
			queryContent = c
		} else {
			queryContent = ""
		}
	}

	userMsg := synapse.NewContextMessage(subagentThreadID, "user-client", synapse.RoleUser, queryContent, s.SquadID, nil, 3600)
	_, _ = s.Blackboard.SendMessage(ctx, userMsg)

	agent.ExecuteInstrumented(ctx, subagentThreadID, &userMsg, squadThreadID)
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

	var assistantMsgs []synapse.SynapseMessage
	for _, msg := range messages {
		if msg.Role == synapse.RoleAssistant {
			if _, ok := s.SubAgents[msg.AgentID]; ok {
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

	if len(s.SubAgents) <= 1 {
		for _, msg := range assistantMsgs {
			out := synapse.NewContextMessage(parentThreadID, msg.AgentID, synapse.RoleAssistant, msg.Content(), s.SquadID, nil, 3600)
			_, _ = s.Blackboard.SendMessage(ctx, out)
		}
		return nil
	}

	llm := s.LLMCall
	model := s.Model
	if llm == nil {
		for _, agent := range s.SubAgents {
			if agent.LLMCall != nil {
				llm = agent.LLMCall
				model = agent.Model
				break
			}
		}
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
		res, err := llm(ctx, model, coordinatorPrompt, nil)
		if err != nil {
			observedLogger(ctx, s.Blackboard).Warn("squad coordinator llm call failed; falling back to joined responses",
				"squad_id", s.SquadID,
				"thread_id", squadThreadID,
				"model", model,
				"error", err,
			)
		} else {
			if c, ok := res["content"].(string); ok && c != "" {
				summary = strings.TrimSpace(c)
			}
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

	out := synapse.NewContextMessage(parentThreadID, s.SquadID+"-coordinator", synapse.RoleAssistant, summary, s.SquadID, nil, 3600)
	_, _ = s.Blackboard.SendMessage(ctx, out)
	return nil
}
