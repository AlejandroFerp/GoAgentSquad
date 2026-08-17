package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/embention/agent-squad-go/pkg/config"
	"github.com/embention/agent-squad-go/pkg/dashboard"
	"github.com/embention/agent-squad-go/pkg/squads"
)

func main() {
	settings, err := config.LoadDashboard(os.Args[1:])
	if err != nil {
		log.Fatalf("dashboard config: %v", err)
	}

	server := dashboard.NewServer(squads.NewObservabilityRuntime(), dashboard.WithTraceFile(settings.TraceFile))
	log.Printf("dashboard listening on http://%s", settings.Address)
	if settings.TraceFile != "" {
		log.Printf("loading traces from %s", settings.TraceFile)
	} else {
		log.Printf("standalone mode shows only in-process traces wired into the same runtime")
	}
	httpServer := &http.Server{
		Addr:              settings.Address,
		Handler:           server,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(shutdown)
	go func() {
		<-shutdown
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("dashboard shutdown error: %v", err)
		}
	}()
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
