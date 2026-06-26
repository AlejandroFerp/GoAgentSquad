package observability

import (
	"bufio"
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

// JSONFileLoader incrementally loads step traces from a JSONL file. It keeps an
// internal byte offset so repeated Sync calls only ingest newly appended lines.
type JSONFileLoader struct {
	mu     sync.Mutex
	Path   string
	offset int64
}

// Sync reads newly appended JSONL steps from Path and records them into ledger.
func (l *JSONFileLoader) Sync(ledger *StepLedger) error {
	if l == nil || ledger == nil {
		return nil
	}
	if l.Path == "" {
		return fmt.Errorf("json trace loader path is empty")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	file, err := os.Open(l.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open trace input file %s: %w", l.Path, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat trace input file %s: %w", l.Path, err)
	}
	if info.Size() < l.offset {
		l.offset = 0
	}
	if _, err := file.Seek(l.offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek trace input file %s: %w", l.Path, err)
	}

	reader := bufio.NewReader(file)
	bytesRead := int64(0)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			bytesRead += int64(len(line))
			var step AgentStep
			if unmarshalErr := json.Unmarshal(line, &step); unmarshalErr == nil {
				ledger.Record(step)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read trace input file %s: %w", l.Path, err)
		}
	}
	l.offset += bytesRead
	return nil
}
