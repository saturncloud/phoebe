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

// rotate atomically renames the current WAL aside to a temp path and opens a
// fresh, empty WAL in its place, then reads and returns every event from the
// rotated-aside snapshot. The caller ships those events and, on success, calls
// dropRotated(snapPath) to delete the snapshot.
//
// Why rotate instead of read-then-truncate: the drain ships over the network
// with no lock held, and append() runs concurrently on the hot path (Valkey
// down → channel full → append). A read-then-Truncate(0) would zero ANY event
// appended during the ship window — a silently lost metering event on a healthy
// pod. Rotation makes the drained set immutable: once rotated aside, no append
// can land in it (appends go to the fresh file), so deleting the snapshot after
// a successful ship can never destroy an unshipped event. The snapshot file is
// the unit of durability — it survives a crash mid-ship and is re-drained on
// restart (at-least-once; the consumer dedups on request_id).
func (w *wal) rotate() (events []metering.Event, snapPath string, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Nothing buffered → nothing to rotate. Avoids littering empty snapshots.
	if info, statErr := w.f.Stat(); statErr == nil && info.Size() == 0 {
		return nil, "", nil
	}

	snapPath = w.path + ".draining"
	// Rename the live file aside. On POSIX this is atomic; an in-flight append
	// holding mu cannot interleave, and the open fd keeps pointing at the same
	// inode (now at snapPath), so we can still read it below.
	if err := os.Rename(w.path, snapPath); err != nil {
		return nil, "", fmt.Errorf("wal rotate rename: %w", err)
	}
	// Open a fresh WAL at the original path for subsequent appends, and swap the
	// fd. The old fd (still pointing at the snapshot inode) is closed after read.
	old := w.f
	nf, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		// Roll back the rename so we don't lose the snapshot's events.
		_ = os.Rename(snapPath, w.path)
		return nil, "", fmt.Errorf("wal rotate reopen: %w", err)
	}
	w.f = nf

	events, readErr := readEvents(old)
	_ = old.Close()
	if readErr != nil {
		// We already rotated; report the events we got plus the snapshot path so
		// the caller can still ship+drop. A scan error mid-file keeps the prefix.
		return events, snapPath, readErr
	}
	return events, snapPath, nil
}

// dropRotated deletes a snapshot produced by rotate() after its events have been
// shipped. Safe to call without the lock — the snapshot is no longer referenced
// by the live wal. A missing file is not an error (already dropped).
func (w *wal) dropRotated(snapPath string) error {
	if err := os.Remove(snapPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("wal drop snapshot %s: %w", snapPath, err)
	}
	return nil
}

// recoverRotated returns any orphaned snapshot path left by a crash between
// rotate() and dropRotated(). The caller re-ships it on startup. Empty string if
// none. (At-least-once: an event shipped just before the crash will be re-shipped
// and deduped by the consumer on request_id.)
func (w *wal) recoverRotated() string {
	snapPath := w.path + ".draining"
	if _, err := os.Stat(snapPath); err == nil {
		return snapPath
	}
	return ""
}

// readEventsAt reads every event from a snapshot file at the given path.
func readEventsAt(path string) ([]metering.Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open snapshot %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	return readEvents(f)
}

// readEvents scans a JSONL event file from its current offset to EOF. A
// corrupted line is skipped (don't lose the rest); the prefix is preserved on a
// scan error.
func readEvents(f *os.File) ([]metering.Event, error) {
	if _, err := f.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("wal seek: %w", err)
	}
	var events []metering.Event
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var e metering.Event
		if err := json.Unmarshal(line, &e); err != nil {
			continue // corrupted line — skip, keep the rest
		}
		events = append(events, e)
	}
	return events, scanner.Err()
}

// close closes the underlying file.
func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}
