package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/embention/agent-squad-go/pkg/dashboard"
	"github.com/embention/agent-squad-go/pkg/squads"
)

func startDashboard(address string, runtime *squads.ObservabilityRuntime) (*http.Server, string, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, "", fmt.Errorf("listen for dashboard on %s: %w", address, err)
	}

	server := &http.Server{
		Handler:           dashboard.NewServer(runtime),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("dashboard server stopped: %v", err)
		}
	}()
	return server, "http://" + listener.Addr().String(), nil
}

func shutdownDashboard(server *http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(ctx)
}
