package config

import (
	"testing"
)

func TestLoadDashboardDefaults(t *testing.T) {
	settings, err := LoadDashboard(nil)
	if err != nil {
		t.Fatalf("load dashboard defaults: %v", err)
	}
	if settings.Address != "127.0.0.1:8080" {
		t.Fatalf("dashboard address = %q, want default", settings.Address)
	}
	if settings.TraceFile != "" {
		t.Fatalf("trace file = %q, want empty", settings.TraceFile)
	}
}
