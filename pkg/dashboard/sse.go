package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/embention/agent-squad-go/pkg/observability"
)

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	// Keep replay-backed traces synchronized before opening the live stream.
	s.syncTraceFile()
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	// SSE headers: one long-lived HTTP response with incremental events.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	if s.obs == nil || s.obs.Hub == nil {
		// SSE comment frame used as a diagnostic marker for clients.
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, ": no hub configured\n\n")
		flusher.Flush()
		return
	}

	// Subscribe this HTTP connection to the in-memory event hub.
	id, ch, ok := s.obs.Hub.TrySubscribe()
	if !ok {
		http.Error(w, "too many live stream subscribers", http.StatusServiceUnavailable)
		return
	}
	defer s.obs.Hub.Unsubscribe(id)
	w.WriteHeader(http.StatusOK)

	// Initial comment frame confirms the stream is ready.
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
			// Each observability step is emitted as a named SSE event with JSON payload.
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
