package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/embention/agent-squad-go/pkg/observability"
)

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	s.syncTraceFile()
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	if s.obs == nil || s.obs.Hub == nil {
		fmt.Fprint(w, ": no hub configured\n\n")
		flusher.Flush()
		return
	}

	id, ch := s.obs.Hub.Subscribe()
	defer s.obs.Hub.Unsubscribe(id)

	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case step, ok := <-ch:
			if !ok {
				return
			}
			payload, err := json.Marshal(step)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: %s\n", step.Kind)
			fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		}
	}
}

var _ = observability.AgentStep{}
