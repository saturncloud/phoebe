package emit

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/saturncloud/phoebe/internal/metering"
)

// wal is a simple append-only JSONL file used as the durability fallback when
// Valkey is unavailable. Each line is a JSON-encoded metering.Event.
// All exported methods are safe for concurrent use.
type wal struct {
	mu   sync.Mutex
	path string
	f    *os.File
}

// openWAL opens (or creates) the WAL file at path.
func openWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open wal %s: %w", path, err)
	}
	return &wal{path: path, f: f}, nil
}

// append writes one event to the WAL file and fsyncs.
// It is safe for concurrent callers.
func (w *wal) append(e metering.Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	data = append(data, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Write(data); err != nil {
		return fmt.Errorf("wal write: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal fsync: %w", err)
	}
	return nil
}

// readAll reads every event currently in the WAL, returning them in order.
// Does not modify the file.
func (w *wal) readAll() ([]metering.Event, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to start for reading.
	if _, err := w.f.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("wal seek: %w", err)
	}

	var events []metering.Event
	scanner := bufio.NewScanner(w.f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var e metering.Event
		if err := json.Unmarshal(line, &e); err != nil {
			// Corrupted line — skip but don't lose the rest.
			continue
		}
		events = append(events, e)
	}
	return events, scanner.Err()
}

// truncate empties the WAL after successful shipping.
func (w *wal) truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.f.Truncate(0); err != nil {
		return fmt.Errorf("wal truncate: %w", err)
	}
	if _, err := w.f.Seek(0, 0); err != nil {
		return fmt.Errorf("wal seek after truncate: %w", err)
	}
	return nil
}

// close closes the underlying file.
func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}
