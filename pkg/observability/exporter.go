package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

type TraceExporter interface {
	Export(ctx context.Context, steps []AgentStep) error
	Shutdown(ctx context.Context) error
}

type StdoutExporter struct {
	Writer io.Writer
}

func (e StdoutExporter) Export(_ context.Context, steps []AgentStep) error {
	writer := e.Writer
	if writer == nil {
		writer = os.Stdout
	}
	encoder := json.NewEncoder(writer)
	for _, step := range steps {
		if err := encoder.Encode(step); err != nil {
			return fmt.Errorf("export stdout trace: %w", err)
		}
	}
	return nil
}

func (StdoutExporter) Shutdown(context.Context) error { return nil }

type JSONFileExporter struct {
	mu   sync.Mutex
	Path string
}

func (e *JSONFileExporter) Export(_ context.Context, steps []AgentStep) error {
	if e.Path == "" {
		return fmt.Errorf("json trace exporter path is empty")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	file, err := os.OpenFile(e.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open trace export file %s: %w", e.Path, err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	for _, step := range steps {
		if err := encoder.Encode(step); err != nil {
			return fmt.Errorf("write trace step to %s: %w", e.Path, err)
		}
	}
	return nil
}

func (*JSONFileExporter) Shutdown(context.Context) error { return nil }
