// Package squads implements the declarative multi-agent collaboration framework
// Synapse blackboard designed for safe concurrent execution.
package squads

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/embention/agent-squad-go/pkg/observability"
	"github.com/embention/agent-squad-go/pkg/synapse"
)

// This file defines the blackboard abstraction and its Synapse-backed adapter,
// plus shared runtime state such as observability and parent-thread tracking.

// PipelineMetrics is the thread-safe metrics contract exposed by the blackboard.
// The concrete implementation lives in metrics.go.
type PipelineMetrics interface {
	RecordSquadStart(squadID string)
	RecordSquadEnd(squadID string)
	RecordSubagentStart(squadID, agentID string)
	RecordSubagentEnd(squadID, agentID string, elapsed time.Duration)
	RecordSubagentWaiting(squadID, agentID, replyToThread string)
	RecordSubagentResumed(squadID, agentID, replyThreadID string)
	AddSubagentElapsedTime(squadID, agentID string, elapsed time.Duration)
	RecordTransversalStart(agentID string)
	RecordTransversalSuccess(agentID string, elapsed time.Duration)
	RecordTransversalFailure(agentID string, elapsed time.Duration)
	RecordObserverInteraction(observerName string)
	RecordLLMUsage(squadID, agentID string, prompt, completion, total int, elapsed time.Duration)
	RecordCoordinatorUsage(squadID string, prompt, completion, total int, elapsed time.Duration)
	RecordCrossSquadMessage(fromAgent, toSquad, taskType string, parameters map[string]any)
	RecordDelegation(source, destination, delegationType string, parameters map[string]any)
	RecordTaskStarted(taskType string)
	RecordTaskCompleted(taskType string)
	RecordTaskFailed(taskType string)
	RecordRetry(squadID, agentID, operation string)
	RecordError(squadID, agentID, category string)
	RegisterSquad(squadID string, squad *Squad)
	RegisterTransversal(agentID, agentType string)
	Finalize(status string)
	ToDict() map[string]any
}

// ObservabilityRuntime groups the optional tracing and timeline components.
// The zero-value is normalized by ensureDefaults so callers can use it safely.
type ObservabilityRuntime struct {
	Tracer            observability.Tracer
	Hub               *observability.Hub
	Ledger            *observability.StepLedger
	Exporter          observability.TraceExporter
	Logger            *slog.Logger
	CaptureLLMContent bool
}

func NewObservabilityRuntime() *ObservabilityRuntime {
	return (&ObservabilityRuntime{}).ensureDefaults()
}

func (r *ObservabilityRuntime) ensureDefaults() *ObservabilityRuntime {
	if r == nil {
		r = &ObservabilityRuntime{}
	}
	if r.Tracer == nil {
		r.Tracer = observability.NoopTracer{}
	}
	if r.Hub == nil {
		r.Hub = observability.NewHub()
	}
	if r.Ledger == nil {
		r.Ledger = observability.NewStepLedger(r.Hub)
	}
	if r.Logger == nil {
		r.Logger = observability.NewTextLogger(nil, nil)
	}
	return r
}

// BlackboardBus is the abstract contract that decouples squads from the
// concrete SynapseService implementation.
type BlackboardBus interface {
	Events() *synapse.EventBus
	Metrics() map[string]PipelineMetrics
	GetMetrics(threadID string) PipelineMetrics
	SetMetrics(threadID string, metrics PipelineMetrics)
	SendMessage(ctx context.Context, msg synapse.SynapseMessage) (*synapse.SynapseMessage, error)
	FetchContext(ctx context.Context, threadID string, limit int) ([]synapse.SynapseMessage, error)
	ConsumeTask(ctx context.Context, threadID, squadID, taskType string, limit int) ([]synapse.SynapseMessage, error)
	ParentThreads() *ParentThreadMap
	Observability() *ObservabilityRuntime
}

// ParentThreadMap tracks the parent-child relationship between reply threads and
// their originating threads. It is used by the pipeline to resolve the root
// thread of an execution tree.
type ParentThreadMap struct {
	mu sync.RWMutex
	m  map[string]string
}

// NewParentThreadMap returns an empty ParentThreadMap.
func NewParentThreadMap() *ParentThreadMap {
	return &ParentThreadMap{m: make(map[string]string)}
}

// Set records that childThread was spawned from parentThread.
func (p *ParentThreadMap) Set(childThread, parentThread string) {
	p.mu.Lock()
	p.m[childThread] = parentThread
	p.mu.Unlock()
}

// Get returns the parent thread for the given child, or "" if none.
func (p *ParentThreadMap) Get(childThread string) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.m[childThread]
}

// Has reports whether childThread has a registered parent.
func (p *ParentThreadMap) Has(childThread string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.m[childThread]
	return ok
}

// Keys returns a snapshot of all child thread IDs.
func (p *ParentThreadMap) Keys() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]string, 0, len(p.m))
	for k := range p.m {
		out = append(out, k)
	}
	return out
}

// DeleteTree removes the descendant relationships for rootThread and returns
// every thread ID that belonged to the removed execution tree, including rootThread.
func (p *ParentThreadMap) DeleteTree(rootThread string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	threads := map[string]struct{}{rootThread: {}}
	for changed := true; changed; {
		changed = false
		for childThread, parentThread := range p.m {
			if _, exists := threads[parentThread]; !exists {
				continue
			}
			if _, exists := threads[childThread]; exists {
				continue
			}
			threads[childThread] = struct{}{}
			changed = true
		}
	}

	out := make([]string, 0, len(threads))
	for threadID := range threads {
		out = append(out, threadID)
		if threadID != rootThread {
			delete(p.m, threadID)
		}
	}
	return out
}

// SynapseBlackboardBus adapts a synapse.SynapseService to the BlackboardBus
// contract. It also carries the auxiliary state that squads attach to the
// blackboard at runtime (metrics registry and parent thread map).
type SynapseBlackboardBus struct {
	synapse      *synapse.SynapseService
	metricsStore *BoundedMap
	parents      *ParentThreadMap
	obs          *ObservabilityRuntime
}

// NewSynapseBlackboardBus wraps a SynapseService into a BlackboardBus.
func NewSynapseBlackboardBus(svc *synapse.SynapseService) *SynapseBlackboardBus {
	return &SynapseBlackboardBus{
		synapse:      svc,
		metricsStore: NewBoundedMap(100),
		parents:      NewParentThreadMap(),
		obs:          NewObservabilityRuntime(),
	}
}

func (b *SynapseBlackboardBus) Events() *synapse.EventBus { return b.synapse.Events }

func (b *SynapseBlackboardBus) Metrics() map[string]PipelineMetrics {
	raw := b.metricsStore.All()
	out := make(map[string]PipelineMetrics, len(raw))
	for k, v := range raw {
		if pm, ok := v.(PipelineMetrics); ok {
			out[k] = pm
		}
	}
	return out
}

func (b *SynapseBlackboardBus) GetMetrics(threadID string) PipelineMetrics {
	if v, ok := b.metricsStore.Get(threadID); ok {
		if pm, ok := v.(PipelineMetrics); ok {
			return pm
		}
	}
	return nil
}

func (b *SynapseBlackboardBus) SetMetrics(threadID string, metrics PipelineMetrics) {
	b.metricsStore.Set(threadID, metrics)
}

func (b *SynapseBlackboardBus) SendMessage(ctx context.Context, msg synapse.SynapseMessage) (*synapse.SynapseMessage, error) {
	if msg.ParentThreadID == "" {
		msg.ParentThreadID = b.parents.Get(msg.ThreadID)
	}
	return b.synapse.SendMessage(ctx, msg)
}

func (b *SynapseBlackboardBus) FetchContext(ctx context.Context, threadID string, limit int) ([]synapse.SynapseMessage, error) {
	return b.synapse.FetchContext(ctx, threadID, limit)
}

func (b *SynapseBlackboardBus) ConsumeTask(ctx context.Context, threadID, squadID, taskType string, limit int) ([]synapse.SynapseMessage, error) {
	return b.synapse.ConsumeTask(ctx, threadID, squadID, taskType, "", limit)
}

func (b *SynapseBlackboardBus) ParentThreads() *ParentThreadMap { return b.parents }

func (b *SynapseBlackboardBus) Observability() *ObservabilityRuntime {
	return b.obs
}

// BoundedMap is a thread-safe map with a maximum size. When full, the oldest
// key (by insertion order) is evicted.
type BoundedMap struct {
	mu      sync.Mutex
	m       map[string]any
	order   []string
	maxSize int
}

// NewBoundedMap returns a BoundedMap with the given capacity.
func NewBoundedMap(maxSize int) *BoundedMap {
	if maxSize <= 0 {
		maxSize = 100
	}
	return &BoundedMap{m: make(map[string]any), maxSize: maxSize}
}

// Set inserts or updates a key. If the map is full and the key is new, the
// oldest key is removed.
func (b *BoundedMap) Set(key string, value any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.m[key]; !exists {
		if len(b.order) >= b.maxSize {
			oldest := b.order[0]
			b.order = b.order[1:]
			delete(b.m, oldest)
		}
		b.order = append(b.order, key)
	}
	b.m[key] = value
}

// Get retrieves a value by key.
func (b *BoundedMap) Get(key string) (any, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.m[key]
	return v, ok
}

// All returns a snapshot copy of the underlying map.
func (b *BoundedMap) All() map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]any, len(b.m))
	for k, v := range b.m {
		out[k] = v
	}
	return out
}
