package dashboard_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/embention/agent-squad-go/pkg/dashboard"
)

// The dashboard must render graphs from the binary alone, so its assets cannot
// depend on a remote CDN at runtime.
func TestDashboardServesMermaidFromEmbeddedAssets(t *testing.T) {
	server := dashboard.NewServer(newObservedRuntime())

	request := httptest.NewRequest(http.MethodGet, "/mermaid.min.js", nil)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body, err := io.ReadAll(recorder.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if len(body) == 0 {
		t.Fatal("mermaid.min.js served an empty body")
	}
	if !strings.Contains(string(body), "mermaid") {
		t.Error("served asset does not look like the Mermaid bundle")
	}
}

func TestDashboardIndexHasNoRemoteScripts(t *testing.T) {
	server := dashboard.NewServer(newObservedRuntime())

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	index := recorder.Body.String()
	if !strings.Contains(index, `<script src="/mermaid.min.js">`) {
		t.Error("index.html does not load Mermaid from the embedded assets")
	}
	for _, remote := range []string{"cdn.jsdelivr.net", "unpkg.com", "cdnjs.cloudflare.com"} {
		if strings.Contains(index, remote) {
			t.Errorf("index.html references remote host %q; assets must stay embedded", remote)
		}
	}
}
