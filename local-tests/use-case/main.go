package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/embention/agent-squad-go/pkg/squads"
)

func main() {
	log.SetFlags(0)
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		log.Fatal(err)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	cfg, err := loadConfig(args)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var llm squads.LLMCall
	if cfg.Mock {
		llm = mockLLMCall
		fmt.Fprintln(stderr, "Mock mode enabled: OpenRouter will not be called.")
	} else {
		llm = newOpenRouterClient(cfg).call
		fmt.Fprintf(stderr, "OpenRouter model chain contains %d model(s).\n", len(cfg.Models))
	}

	experiment, err := newExperiment(ctx, cfg, llm)
	if err != nil {
		return err
	}
	defer experiment.Close()

	server, dashboardURL, err := startDashboard(cfg.DashboardAddr, experiment.Observability())
	if err != nil {
		return err
	}
	defer shutdownDashboard(server)
	fmt.Fprintf(stderr, "Dashboard: %s\n", dashboardURL)
	fmt.Fprintln(stderr, "Starting Squad 1: Manual Discovery and navigation-tree extraction...")
	fmt.Fprintln(stderr, "Squad 2 will start four scoped technical readers concurrently after discovery.")
	fmt.Fprintln(stderr, "Squad 3 will verify bounded candidate relationships; Squad 4 will format the evidence-gated report.")

	result, err := experiment.Run(ctx)
	if err != nil {
		return err
	}
	if err := os.WriteFile(cfg.OutputPath, []byte(result.Report), 0o644); err != nil {
		return fmt.Errorf("write audit report to %s: %w", cfg.OutputPath, err)
	}
	inventory, err := json.MarshalIndent(result.Inventory, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manual inventory: %w", err)
	}
	if err := os.WriteFile(cfg.InventoryOutput, append(inventory, '\n'), 0o644); err != nil {
		return fmt.Errorf("write manual inventory to %s: %w", cfg.InventoryOutput, err)
	}
	if err := os.WriteFile(cfg.MatrixOutput, []byte(formatCompatibilityMatrix(result.Matrix)), 0o644); err != nil {
		return fmt.Errorf("write compatibility matrix to %s: %w", cfg.MatrixOutput, err)
	}

	fmt.Fprintln(stdout, result.Report)
	fmt.Fprintf(stderr, "Audit report written to %s\n", cfg.OutputPath)
	fmt.Fprintf(stderr, "Manual map written to %s\n", cfg.InventoryOutput)
	fmt.Fprintf(stderr, "Compatibility matrix written to %s\n", cfg.MatrixOutput)
	fmt.Fprintf(stderr, "Dashboard phase queries: %s, %s, %s, %s\n", result.DiscoveryQuery, result.IngestionQuery, result.SynthesisQuery, result.ReportingQuery)
	fmt.Fprintf(stderr, "Scope %q: recorded %d mapped manuals, %d verification candidates, %d verified relationships, and %d blind spots.\n", cfg.Scope, len(result.Inventory.Manuals), len(result.Candidates), len(result.Matrix), len(result.BlindSpots))
	if cfg.ExitAfterRun {
		return nil
	}

	fmt.Fprintln(stderr, "Dashboard remains active. Press Ctrl+C to stop.")
	<-ctx.Done()
	return nil
}
