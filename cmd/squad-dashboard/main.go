package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/embention/agent-squad-go/pkg/dashboard"
	"github.com/embention/agent-squad-go/pkg/squads"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "dashboard listen address")
	traceFile := flag.String("trace-file", "", "optional JSONL trace file to load and tail into the dashboard")
	flag.Parse()

	server := dashboard.NewServer(squads.NewObservabilityRuntime(), dashboard.WithTraceFile(*traceFile))
	log.Printf("dashboard listening on http://%s", *addr)
	if *traceFile != "" {
		log.Printf("loading traces from %s", *traceFile)
	} else {
		log.Printf("standalone mode shows only in-process traces wired into the same runtime")
	}
	log.Fatal(http.ListenAndServe(*addr, server))
}
