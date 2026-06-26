// Package main provides a demonstration of the agent-squad-go framework,
// mirroring the theological squads example from the Python test suite.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/embention/agent-squad-go/pkg/dashboard"
	"github.com/embention/agent-squad-go/pkg/observability"
	"github.com/embention/agent-squad-go/pkg/squads"
	"github.com/embention/agent-squad-go/pkg/synapse"
)

// bibleDB is a tiny in-memory scripture lookup used by the reference expansion
// observer and the scripture study transversal agent.
var bibleDB = map[string]string{
	"Juan 3:16":   "Porque de tal manera amó Dios al mundo, que ha dado a su Hijo unigénito...",
	"Rom 8:28":    "Y sabemos que a los que aman a Dios, todas las cosas les ayudan a bien...",
	"Salmos 23:1": "Jehová es mi pastor; nada me faltará.",
}

// FinalSynthesizerImpl compiles the final response from assistant messages on
// the root thread.
type FinalSynthesizerImpl struct {
	Blackboard  squads.BlackboardBus
	LastContent string
}

// NewFinalSynthesizer builds a FinalSynthesizerImpl.
func NewFinalSynthesizer(bb squads.BlackboardBus) *FinalSynthesizerImpl {
	return &FinalSynthesizerImpl{Blackboard: bb}
}

// Synthesize compiles assistant messages into a single final response.
func (f *FinalSynthesizerImpl) Synthesize(ctx context.Context, threadID, squadID string) error {
	messages, err := squads.FetchSlicedContext(ctx, f.Blackboard, threadID, 100)
	if err != nil {
		return err
	}
	var insights []string
	for _, msg := range messages {
		if msg.Role == synapse.RoleAssistant && msg.Content() != "" && !strings.Contains(msg.Content(), "Synthesis") {
			insights = append(insights, fmt.Sprintf("- %s", msg.Content()))
		}
	}
	f.LastContent = fmt.Sprintf("### [Final Response Synthesis for Squad: %s]\n%s", squadID, strings.Join(insights, "\n"))
	finalMsg := synapse.NewContextMessage(threadID, "squad-leader-synthesizer", synapse.RoleAssistant, f.LastContent, "", nil, 3600)
	_, err = f.Blackboard.SendMessage(ctx, finalMsg)
	return err
}

// LastSynthesizedContent returns the last synthesized content.
func (f *FinalSynthesizerImpl) LastSynthesizedContent() string {
	return f.LastContent
}

// mockLLMCall is a deterministic LLM stub that returns canned responses.
func mockLLMCall(ctx context.Context, model, systemPrompt string, messages []map[string]any) (map[string]any, error) {
	// If the system prompt mentions "correction helper", return a healed argument.
	if strings.Contains(systemPrompt, "correction helper") {
		return map[string]any{
			"content":           `{"query": "healed"}`,
			"prompt_tokens":     10,
			"completion_tokens": 5,
			"total_tokens":      15,
		}, nil
	}
	// If the system prompt mentions "Integrate the tool result", produce a final answer.
	if strings.Contains(systemPrompt, "Integrate the tool result") {
		return map[string]any{
			"content":           "Based on the scripture analysis, the passage speaks of God's love.",
			"prompt_tokens":     20,
			"completion_tokens": 15,
			"total_tokens":      35,
		}, nil
	}
	// Default: respond with a plain answer.
	return map[string]any{
		"content":           "Theological analysis complete: the passage reveals divine love and grace.",
		"prompt_tokens":     30,
		"completion_tokens": 20,
		"total_tokens":      50,
	}, nil
}

func main() {
	ctx := context.Background()

	// 1. Initialize Synapse engine and blackboard.
	svc := synapse.NewSynapseService(50, nil)
	if err := svc.Connect(ctx); err != nil {
		log.Fatalf("synapse connect: %v", err)
	}
	defer svc.Close()

	blackboard := squads.NewSynapseBlackboardBus(svc)
	otelConfig, otelEnabled, err := observability.OTelRuntimeConfigFromEnv(os.LookupEnv, observability.OTelRuntimeConfig{
		ServiceName:    "agent-squad-demo",
		ServiceVersion: "dev",
		TracerName:     "agent-squad-demo",
	})
	if err != nil {
		log.Fatalf("opentelemetry config: %v", err)
	}
	if otelEnabled {
		otelRuntime, err := observability.NewOTelRuntime(ctx, otelConfig)
		if err != nil {
			log.Fatalf("opentelemetry init: %v", err)
		}
		blackboard.Observability().Tracer = otelRuntime.Tracer
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if shutdownErr := otelRuntime.Shutdown(shutdownCtx); shutdownErr != nil {
				log.Printf("opentelemetry shutdown error: %v", shutdownErr)
			}
		}()
		fmt.Printf("OpenTelemetry tracing enabled for service %s\n", otelConfig.ServiceName)
	}
	if traceFile := os.Getenv("SQUAD_TRACE_JSONL"); traceFile != "" {
		blackboard.Observability().Exporter = &observability.JSONFileExporter{Path: traceFile}
		fmt.Printf("Trace export enabled at %s\n", traceFile)
	}
	if addr := os.Getenv("SQUAD_DASHBOARD_ADDR"); addr != "" {
		server := dashboard.NewServer(blackboard.Observability())
		go func() {
			if err := http.ListenAndServe(addr, server); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("dashboard server error: %v", err)
			}
		}()
		fmt.Printf("Dashboard available at http://%s\n", addr)
	}

	// 2. Create the final synthesizer.
	finalSynth := NewFinalSynthesizer(blackboard)

	// 3. Build the pipeline.
	pipeline := squads.NewSquadsPipeline(blackboard, finalSynth, 15)

	// 4. Register a scripture study transversal agent.
	scriptureAgent := squads.NewTransversalAgent("trans-study", "StudyAgent", []string{"bible_study"}, blackboard)
	scriptureAgent.Description = "Provides scripture text lookups."
	scriptureAgent.ExecuteTask = func(ctx context.Context, taskMsg *synapse.SynapseMessage) (string, error) {
		passage := ""
		if p := taskMsg.Parameters(); p != nil {
			if v, ok := p["passage"].(string); ok {
				passage = v
			}
		}
		text := bibleDB[passage]
		if text == "" {
			text = fmt.Sprintf("[Scripture text for %s]", passage)
		}
		return text, nil
	}
	pipeline.RegisterTransversal(scriptureAgent)

	// 5. Build a pastoral care squad with a comfort agent.
	comfortAgent := squads.NewSubAgent(
		"comfort-agent",
		"ComfortAgent",
		"Provides pastoral comfort based on scripture.",
		"You are a pastoral comfort agent. Respond with empathy and biblical references.",
		blackboard,
		"squad-pastoral",
	)
	comfortAgent.Model = "mock-model"
	comfortAgent.LLMCall = mockLLMCall

	pastoralSquad := squads.NewSquad("squad-pastoral", "Pastoral Care Squad", "Provides pastoral care.", blackboard)
	pastoralSquad.LLMCall = mockLLMCall
	pastoralSquad.Model = "mock-model"
	pastoralSquad.RegisterSubAgent(comfortAgent)
	pipeline.RegisterSquad(pastoralSquad)

	// 6. Register a reference expansion observer.
	observer := squads.NewReferenceExpansionObserver("*", blackboard, func(ref string) string {
		return bibleDB[ref]
	})
	pipeline.RegisterObserver(observer)

	// 7. Set a simple route query function.
	pipeline.RouteQueryFn = func(ctx context.Context, content string) (any, error) {
		return "squad-pastoral", nil
	}

	// 8. Run a query.
	res, err := pipeline.Query(ctx, "session-thread-01", nil, "I need counseling and theology info about [Juan 3:16].", 10.0)
	if err != nil {
		log.Fatalf("pipeline query: %v", err)
	}

	fmt.Println("=== Pipeline Result ===")
	fmt.Println("Response:", res.Response)
	fmt.Println()
	fmt.Println("=== Metrics ===")
	fmt.Printf("Status: %v\n", res.Metrics["status"])
	fmt.Printf("Elapsed: %.4fs\n", res.Metrics["elapsed_time"])
	if squadsMap, ok := res.Metrics["squads"].(map[string]any); ok {
		fmt.Println("Squads Run:", keys(squadsMap))
	}
	if transversals, ok := res.Metrics["transversals"].(map[string]any); ok {
		fmt.Println("Transversals:", keys(transversals))
	}
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Ensure time import is used even if logic above evolves.
var _ = time.Second
