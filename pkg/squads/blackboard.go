// Package squads implements the declarative multi-agent collaboration framework
// on top of the synapse blackboard. It is the Go equivalent of the Python
// agent_squads package, redesigned for safe concurrent execution.
package squads

import (
	"context"
	"sync"

	"github.com/embention/agent-squad-go/pkg/synapse"
)

// PipelineMetrics is the thread-safe metrics contract exposed by the blackboard.
// The concrete implementation lives in metrics.go.
type PipelineMetrics interface {
	RecordSquadStart(squadID string)
	RecordSquadEnd(squadID string)
	RecordSubagentStart(squadID, agentID string)
	RecordSubagentEnd(squadID, agentID string, elapsed float64)
	RecordSubagentWaiting(squadID, agentID, replyToThread string)
	RecordSubagentResumed(squadID, agentID, replyThreadID string)
	AddSubagentElapsedTime(squadID, agentID string, elapsed float64)
	RecordTransversalStart(agentID string)
	RecordTransversalSuccess(agentID string, elapsed float64)
	RecordTransversalFailure(agentID string, elapsed float64)
	RecordObserverInteraction(observerName string)
	RecordLLMUsage(squadID, agentID string, prompt, completion, total int, elapsed float64)
	RecordCoordinatorUsage(squadID string, prompt, completion, total int, elapsed float64)
	RecordCrossSquadMessage(fromAgent, toSquad, taskType string, parameters map[string]any)
	RecordDelegation(source, destination, delegationType string, parameters map[string]any)
	RegisterSquad(squadID string, squad *Squad)
	RegisterTransversal(agentID, agentType string)
	Finalize(status string)
	ToDict() map[string]any
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

// SynapseBlackboardBus adapts a synapse.SynapseService to the BlackboardBus
// contract. It also carries the auxiliary state that squads attach to the
// blackboard at runtime (metrics registry and parent thread map).
type SynapseBlackboardBus struct {
	synapse      *synapse.SynapseService
	metricsStore *BoundedMap
	parents      *ParentThreadMap
}

// NewSynapseBlackboardBus wraps a SynapseService into a BlackboardBus.
func NewSynapseBlackboardBus(svc *synapse.SynapseService) *SynapseBlackboardBus {
	return &SynapseBlackboardBus{
		synapse:      svc,
		metricsStore: NewBoundedMap(100),
		parents:      NewParentThreadMap(),
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
	return b.synapse.SendMessage(ctx, msg)
}

func (b *SynapseBlackboardBus) FetchContext(ctx context.Context, threadID string, limit int) ([]synapse.SynapseMessage, error) {
	return b.synapse.FetchContext(ctx, threadID, limit)
}

func (b *SynapseBlackboardBus) ConsumeTask(ctx context.Context, threadID, squadID, taskType string, limit int) ([]synapse.SynapseMessage, error) {
	return b.synapse.ConsumeTask(ctx, threadID, squadID, taskType, "", limit)
}

func (b *SynapseBlackboardBus) ParentThreads() *ParentThreadMap { return b.parents }

// BoundedMap is a thread-safe map with a maximum size. When full, the oldest
// key (by insertion order) is evicted. It mirrors the Python BoundedDict.
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
