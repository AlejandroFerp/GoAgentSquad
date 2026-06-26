# Agent Squad Go

A Go port of the AGAPES Agent Squads framework and Synapse blackboard engine, redesigned for safe concurrent multi-threaded execution.

## Project Structure

```
agent-squad-go/
├── cmd/
│   └── squad-demo/           # End-to-end demonstration
│       └── main.go
├── pkg/
│   ├── synapse/              # Core messaging engine (port of agapes_synapse)
│   │   ├── models.go         # SynapseMessage, ContextMessage, TaskMessage, CommandMessage
│   │   ├── events.go         # EventBus with pre/post-insert hooks and regex matching
│   │   ├── storage.go        # BaseStorage interface + NoopStorage
│   │   └── engine.go         # SynapseService: in-memory blackboard, cache, GC, atomic consume
│   └── squads/               # Multi-agent collaboration framework (port of agent_squads)
│       ├── blackboard.go     # BlackboardBus interface, SynapseBlackboardBus adapter, BoundedMap
│       ├── metrics.go        # Thread-safe ExecutionMetrics with fine-grained mutexes
│       ├── agent.go          # BaseAgent, SubAgent, TransversalAgent, LLM reasoning loop, tool healing
│       ├── observer.go       # BaseObserver, ReferenceExpansionObserver
│       ├── synthesizer.go    # Context compaction with synthesis checkpoints
│       ├── squad.go          # Squad with concurrent sub-agent execution and coordination
│       └── pipeline.go       # SquadsPipeline with tree quiescence detection and timeout
├── tests/
│   ├── synapse/
│   │   └── synapse_test.go   # EventBus, SendMessage, ConsumeTask, GC tests
│   └── squads/
│       └── squads_test.go    # BoundedMap, SubAgent, Observer, Squad, Metrics, ToolCall tests
├── go.mod
├── plan.md                   # Engineering plan and architecture analysis
└── README.md
```

## Key Design Decisions

### Thread Safety
- All shared maps and counters are protected by `sync.RWMutex` or `sync.Mutex`.
- `ExecutionMetrics` uses a single top-level mutex with internal locked helpers to avoid reentrant locking (Go mutexes are NOT reentrant).
- `SynapseService` uses `sync.RWMutex` for read-heavy workloads (FetchContext) with exclusive locks for writes.

### Concurrency Model
- Python's `asyncio.gather` is replaced by `sync.WaitGroup` for concurrent sub-agent execution.
- Post-insert callbacks run in independent goroutines with `recover()` guards.
- The GC loop uses `time.Ticker` with `context.Context` cancellation.

### Composition over Inheritance
- Go has no inheritance. `BaseAgent` is embedded into `SubAgent` and `TransversalAgent`.
- `BlackboardBus` is a pure interface; `SynapseBlackboardBus` adapts `synapse.SynapseService`.

### Quiescence Detection
- The pipeline resolves the root thread by walking `ParentThreadMap`.
- It checks active executions (atomic counters) and pending replies across all squads.
- A `sync.WaitGroup` signals completion with timeout support.

## Running

```bash
go run ./cmd/squad-demo/
```

## Testing

```bash
go test ./... -v -count=1
```
