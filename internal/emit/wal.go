package emit

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	tidwall "github.com/tidwall/wal"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
)

// wal is the durability fallback when Valkey is unavailable: an fsync'd,
// sequential write-ahead log backed by github.com/tidwall/wal (small, MIT,
// well-tested, go1.13-compatible). Each entry is a JSON-encoded metering.Event.
//
// Why a library instead of the previous hand-rolled rotate-to-snapshot file:
// the rotation scheme had structural loss modes (a fixed snapshot path that a
// later rotation could clobber, whole-snapshot drops after partial reads, a
// 64KB line limit). Sequential indexed entries + truncate-after-ship make
// those impossible by construction: an entry is only ever reclaimed by
// TruncateFront after the shipper has confirmed delivery through its index,
// and concurrent appends land at higher indexes that truncation never touches.
//
// Shipping protocol: entries are written at strictly increasing indexes
// (tidwall requires Write at exactly lastIndex+1, so nextIndex is cached under
// mu). The shipper reads batches of unshipped entries via pending(), ships
// them, then calls markShipped(through) to reclaim. tidwall's
// TruncateFront(last+1) is out of range — a fully-shipped log RETAINS its last
// entry — so an in-memory shippedThrough watermark records that the retained
// tail is already delivered. On restart the watermark resets to zero and that
// single entry is re-shipped once; harmless, because delivery is
// at-least-once and the drainer dedups on request_id.
//
// All methods are safe for concurrent use.
type wal struct {
	mu  sync.Mutex
	dir string
	log *tidwall.Log

	// nextIndex is the index the next append must Write at (lastIndex+1).
	// tidwall computes nothing for us here: out-of-order writes error.
	nextIndex uint64

	// shippedThrough is the highest index confirmed shipped. In-memory only —
	// see the type comment for the restart/at-least-once contract.
	shippedThrough uint64

	logger *logging.Logger
}

// openWAL opens (or creates) the WAL directory at dir.
//
// Two recovery paths run before the normal open:
//
//   - Legacy import: earlier releases stored the WAL as a single JSONL FILE at
//     this same configured path. If dir is a regular file, its events are read
//     (tolerating a torn final line), the file is renamed aside to
//     "<dir>.imported", and the events are re-appended as entries in the new
//     log. Silently dropping them on upgrade would be silent revenue loss.
//
//   - Corrupt-log quarantine: if tidwall refuses to open the directory (e.g.
//     ErrCorrupt), the directory is moved aside to "<dir>.corrupt.<unix-ts>"
//     for forensics and a fresh log is created. Serving must not be blocked by
//     a corrupt billing buffer; the loss is bounded and loudly visible.
func openWAL(dir string, logger *logging.Logger) (*wal, error) {
	legacy, err := importLegacyFile(dir, logger)
	if err != nil {
		return nil, err
	}

	l, err := tidwall.Open(dir, nil)
	if err != nil {
		quarantine := fmt.Sprintf("%s.corrupt.%d", dir, time.Now().Unix())
		logger.Error.Printf(
			"emit: WAL OPEN FAILED at %s: %v — moving aside to %s and starting fresh; "+
				"any unshipped events in the quarantined dir are NOT billed until manually recovered",
			dir, err, quarantine)
		if rerr := os.Rename(dir, quarantine); rerr != nil {
			return nil, fmt.Errorf("open wal %s: %w (and quarantine rename failed: %v)", dir, err, rerr)
		}
		l, err = tidwall.Open(dir, nil)
		if err != nil {
			return nil, fmt.Errorf("open wal %s after quarantine: %w", dir, err)
		}
	}

	last, err := l.LastIndex()
	if err != nil {
		_ = l.Close()
		return nil, fmt.Errorf("wal last index: %w", err)
	}

	w := &wal{dir: dir, log: l, nextIndex: last + 1, logger: logger}

	for _, ev := range legacy {
		if err := w.append(ev); err != nil {
			_ = l.Close()
			return nil, fmt.Errorf("wal import legacy event request_id=%s: %w", ev.RequestID, err)
		}
	}
	if len(legacy) > 0 {
		logger.Info.Printf("emit: imported %d events from legacy wal file (renamed to %s.imported)", len(legacy), dir)
	}

	return w, nil
}

// importLegacyFile handles the file→directory upgrade described on openWAL.
// Returns the events read from the legacy file (nil if dir is not a regular
// file). The legacy file is renamed aside, never deleted — if anything in the
// import goes wrong the original bytes still exist on disk.
func importLegacyFile(dir string, logger *logging.Logger) ([]metering.Event, error) {
	fi, err := os.Stat(dir)
	if err != nil || !fi.Mode().IsRegular() {
		return nil, nil // absent, or already a directory — nothing to import
	}

	events, readErr := readLegacyJSONL(dir)
	if readErr != nil {
		// Keep the prefix we did read; the renamed-aside file retains the rest.
		logger.Error.Printf("emit: legacy wal import read error at %s: %v — importing the %d events read; original preserved at %s.imported", dir, readErr, len(events), dir)
	}

	if err := os.Rename(dir, dir+".imported"); err != nil {
		return nil, fmt.Errorf("rename legacy wal file aside %s: %w", dir, err)
	}
	return events, nil
}

// readLegacyJSONL reads every decodable event from an old-style JSONL WAL
// file. It uses an unbounded line reader (NOT bufio.Scanner, whose default
// 64KB token limit truncated large events in the old design) and tolerates a
// torn final line — a crash mid-append leaves a partial line, which must not
// abort the import of everything before it.
func readLegacyJSONL(path string) ([]metering.Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open legacy wal %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	r := bufio.NewReaderSize(f, 64*1024)
	var events []metering.Event
	for {
		line, err := r.ReadBytes('\n')
		// ReadBytes returns whatever it read even on error — a torn final line
		// (no trailing newline) arrives here alongside io.EOF.
		if trimmed := bytes.TrimSpace(line); len(trimmed) > 0 {
			var ev metering.Event
			if jerr := json.Unmarshal(trimmed, &ev); jerr == nil {
				events = append(events, ev)
			}
			// Undecodable line (torn or corrupt): skip; don't lose the rest.
		}
		if err == io.EOF {
			return events, nil
		}
		if err != nil {
			return events, fmt.Errorf("read legacy wal %s: %w", path, err)
		}
	}
}

// append writes one event as the next entry and fsyncs (tidwall default
// options sync every write — same durability as the old per-append fsync).
func (w *wal) append(e metering.Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.log.Write(w.nextIndex, data); err != nil {
		return fmt.Errorf("wal write index %d: %w", w.nextIndex, err)
	}
	w.nextIndex++
	return nil
}

// pending returns the next batch of unshipped events in index order, at most
// limit (limit <= 0 means unbounded). through is the index of the last entry
// examined — the caller passes it to markShipped after delivering the batch.
// through == 0 means there is nothing unshipped.
//
// A corrupt entry (undecodable JSON) is SKIPPED and counted in skipped, never
// aborts the drain, and is still covered by through so markShipped truncates
// past it — one bad entry must not wedge the billing buffer forever.
func (w *wal) pending(limit int) (events []metering.Event, through uint64, skipped int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	first, err := w.log.FirstIndex()
	if err != nil {
		return nil, 0, 0, fmt.Errorf("wal first index: %w", err)
	}
	last, err := w.log.LastIndex()
	if err != nil {
		return nil, 0, 0, fmt.Errorf("wal last index: %w", err)
	}
	// Empty log, or everything through the retained tail entry already shipped.
	if last == 0 || last <= w.shippedThrough {
		return nil, 0, 0, nil
	}

	start := first
	if w.shippedThrough+1 > start {
		start = w.shippedThrough + 1
	}
	end := last
	if limit > 0 && start+uint64(limit)-1 < end {
		end = start + uint64(limit) - 1
	}

	for i := start; i <= end; i++ {
		data, rerr := w.log.Read(i)
		if rerr != nil {
			// Don't report progress past an unreadable entry: returning
			// through=0 means nothing gets truncated this pass.
			return nil, 0, skipped, fmt.Errorf("wal read index %d: %w", i, rerr)
		}
		var ev metering.Event
		if jerr := json.Unmarshal(data, &ev); jerr != nil {
			skipped++
			continue
		}
		events = append(events, ev)
	}
	return events, end, skipped, nil
}

// markShipped records that every entry up to and including through has been
// delivered, and reclaims their disk space. See the type comment for why the
// last entry is retained on disk and tracked by the in-memory watermark.
func (w *wal) markShipped(through uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	last, err := w.log.LastIndex()
	if err != nil {
		return fmt.Errorf("wal last index: %w", err)
	}
	if last == 0 {
		return nil
	}
	// TruncateFront(i) makes i the new first entry, so this reclaims
	// everything BEFORE min(through, last) and retains that entry itself.
	// TruncateFront(last+1) would error "out of range" — tidwall never lets
	// the log go fully empty once written.
	tf := through
	if tf > last {
		tf = last
	}
	if err := w.log.TruncateFront(tf); err != nil {
		return fmt.Errorf("wal truncate front %d: %w", tf, err)
	}
	if through > w.shippedThrough {
		w.shippedThrough = through
	}
	return nil
}

// close closes the underlying log. Appends after close fail with
// tidwall's ErrClosed, which the emitter turns into log-floor output.
func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.log.Close()
}
