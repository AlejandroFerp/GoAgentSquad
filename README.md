# Agent Squad Go

A Go port of the AGAPES Agent Squads framework and Synapse blackboard engine, redesigned for safe concurrent multi-threaded execution.

## Project Structure

```
agent-squad-go/
├── cmd/
│   └── squad-demo/           # End-to-end demonstration
│       └── main.go
│   └── squad-dashboard/      # Standalone dashboard for live or persisted traces
│       └── main.go
├── pkg/
│   ├── dashboard/            # HTTP API, SSE stream, embedded UI, graph projections
│   ├── observability/        # Trace context, step ledger, hub, exporters, loaders
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

Optional runtime flags and environment variables:

- `SQUAD_DASHBOARD_ADDR` starts the embedded observability dashboard inside the demo process.
- `SQUAD_TRACE_JSONL` exports each completed query timeline to a JSONL file for later inspection.
- `SQUAD_OTEL_ENABLED` turns on the OpenTelemetry tracer runtime in the demo.
- `SQUAD_OTEL_ENDPOINT` points the OTLP gRPC exporter at a collector such as `127.0.0.1:4317`.
- `SQUAD_OTEL_INSECURE` disables TLS for local collectors.
- `SQUAD_OTEL_HEADERS` sets OTLP headers as a comma-separated `key=value` list.
- `SQUAD_OTEL_SERVICE_NAME`, `SQUAD_OTEL_SERVICE_VERSION`, and `SQUAD_OTEL_TRACER_NAME` override the default resource and tracer identity.
- `SQUAD_OTEL_BATCH_TIMEOUT` overrides the span batch timeout using Go duration syntax such as `250ms` or `2s`.

Example workflow:

1. Run the demo with a shared in-process dashboard.
2. Enable `SQUAD_TRACE_JSONL` when you want durable traces.
3. Enable `SQUAD_OTEL_ENABLED` plus an OTLP endpoint when you want the same spans exported to an external collector.
4. Open the standalone dashboard against the exported file when you want to inspect traces outside the demo process.

Example with both JSONL replay and OTLP export enabled:

```powershell
$env:SQUAD_TRACE_JSONL = ".\\traces\\agent-steps.jsonl"
$env:SQUAD_OTEL_ENABLED = "true"
$env:SQUAD_OTEL_ENDPOINT = "127.0.0.1:4317"
$env:SQUAD_OTEL_INSECURE = "true"
go run ./cmd/squad-demo/
```

Standalone dashboard:

```bash
go run ./cmd/squad-dashboard --addr 127.0.0.1:8080 --trace-file ./traces/agent-steps.jsonl
```

The standalone dashboard tails the JSONL file incrementally and deduplicates steps by `step_id`, so you can refresh or reopen the UI without duplicating the visual timeline.

## Testing

```bash
go test ./... -v -count=1
```
