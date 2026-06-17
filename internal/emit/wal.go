package emit

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
// Failure recovery: a failed library Write can leave the library and this
// wrapper divergent — see append for the three modes. The library's own
// prescription (documented on TruncateFront's error path) is to recover by
// Close() followed by Open(); recoverLocked implements exactly that and is
// invoked on any Write failure and on ErrCorrupt from reads/truncates. If the
// reopen itself fails the wal is "poisoned": append errors immediately (the
// emitter floors the event) and retries the reopen on every call — it is
// already the failure path, so the extra open attempt is cheap relative to
// the loss it can stop — while pending/markShipped no-op cleanly.
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

	// closed marks a deliberate close(): appends fail (→ log floor) and no
	// recovery reopen is attempted — reopening after shutdown would leak a
	// live log and divert post-Close events away from the floor.
	closed bool

	// poisoned marks a failed recovery reopen: the underlying log is nil and
	// unusable. append retries the reopen; pending/markShipped no-op. See the
	// type comment.
	poisoned bool

	// failNextWrite, when non-nil, intercepts the next library Write call and
	// is then cleared — a test-only fault-injection seam. The hook receives
	// the live log so tests can reproduce the library's divergent-state
	// failure modes (e.g. perform the real write, then report the fsync as
	// failed; or tear bytes onto the tail segment, then report ENOSPC).
	failNextWrite func(log *tidwall.Log, index uint64, data []byte) error

	logger *logging.Logger
}

// openWAL opens (or creates) the WAL directory at dir.
//
// Two recovery paths run around the normal open:
//
//   - Legacy import: earlier releases stored the WAL as a single JSONL FILE at
//     this same configured path. If dir is a regular file, it is staged aside
//     to "<dir>.importing", its events are appended as entries in the new log,
//     and only THEN is the staging file parked as "<dir>.imported". A crash
//     anywhere before that final rename leaves "<dir>.importing" in place,
//     which the next startup re-imports — duplicates, which the drainer
//     dedups on request_id. Duplicate-on-crash is the correct side of the
//     trade: the old rename-first order parked events invisibly (silent
//     revenue loss) if the process died before the appends.
//
//   - Corrupt-log quarantine: if tidwall refuses to open the directory (e.g.
//     ErrCorrupt), the directory is moved aside to "<dir>.corrupt.<unix-ns>"
//     for forensics and a fresh log is created. Serving must not be blocked by
//     a corrupt billing buffer; the loss is bounded and loudly visible.
func openWAL(dir string, logger *logging.Logger) (*wal, error) {
	// A parked .imported file is forensic data from a previous upgrade that
	// nobody has recovered or removed. Warn on every startup so it cannot be
	// quietly forgotten.
	if fi, err := os.Stat(dir + ".imported"); err == nil && fi.Mode().IsRegular() {
		logger.Warn.Printf(
			"emit: leftover legacy wal import file at %s.imported — parked forensic data from a previous upgrade; verify its events were billed, then remove it",
			dir)
	}

	legacy, staged, err := stageLegacyImport(dir, logger)
	if err != nil {
		return nil, err
	}

	l, last, err := openLog(dir, logger)
	if err != nil {
		return nil, err
	}

	w := &wal{dir: dir, log: l, nextIndex: last + 1, logger: logger}

	for _, ev := range legacy {
		if err := w.append(ev); err != nil {
			_ = w.close()
			return nil, fmt.Errorf("wal import legacy event request_id=%s: %w", ev.RequestID, err)
		}
	}
	if staged {
		// Every staged event is now durably in the new log (appends fsync);
		// only now is the staging file parked. If THIS rename fails the
		// events are still safe — the next startup re-imports and dedups.
		if err := os.Rename(dir+".importing", dir+".imported"); err != nil {
			logger.Error.Printf("emit: legacy wal import: events are in the new log but parking the staging file failed: %v — next startup will re-import (duplicates, deduped downstream)", err)
		}
		logger.Info.Printf("emit: imported %d events from legacy wal file (parked at %s.imported)", len(legacy), dir)
	}

	return w, nil
}

// openLog opens (or creates) the tidwall log at dir, quarantining a corrupt
// directory aside and starting fresh — see openWAL. Returns the log and its
// last index (0 if empty).
func openLog(dir string, logger *logging.Logger) (*tidwall.Log, uint64, error) {
	l, err := tidwall.Open(dir, nil)
	if err != nil {
		// UnixNano, not Unix: in-process write-failure recovery can quarantine
		// more than once per second, and rename onto an existing dir fails.
		quarantine := fmt.Sprintf("%s.corrupt.%d", dir, time.Now().UnixNano())
		// Count prior quarantines so a flapping pod's disk growth is visible
		// in the logs. Never auto-deleted: these are unbilled operator data.
		siblings, _ := filepath.Glob(dir + ".corrupt.*")
		logger.Error.Printf(
			"emit: WAL OPEN FAILED at %s: %v — moving aside to %s and starting fresh "+
				"(%d prior .corrupt dir(s) already present); any unshipped events in the "+
				"quarantined dir are NOT billed until manually recovered",
			dir, err, quarantine, len(siblings))
		if rerr := os.Rename(dir, quarantine); rerr != nil {
			return nil, 0, fmt.Errorf("open wal %s: %w (and quarantine rename failed: %v)", dir, err, rerr)
		}
		l, err = tidwall.Open(dir, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("open wal %s after quarantine: %w", dir, err)
		}
	}

	last, err := l.LastIndex()
	if err != nil {
		_ = l.Close()
		return nil, 0, fmt.Errorf("wal last index: %w", err)
	}
	return l, last, nil
}

// stageLegacyImport handles the file→directory upgrade described on openWAL.
// If dir is a regular file (or a previous import was interrupted, leaving
// "<dir>.importing"), it returns the staged events and staged=true; the
// caller appends them to the new log and only then parks the staging file.
// The legacy bytes are never deleted — at every step the original data exists
// at dir, dir+".importing", or dir+".imported".
func stageLegacyImport(dir string, logger *logging.Logger) (events []metering.Event, staged bool, err error) {
	staging := dir + ".importing"

	// Interrupted import: a previous startup staged the legacy file and
	// crashed before parking it. Some or all of its events may already be in
	// the new log — re-import them all; duplicates are deduped downstream.
	if fi, serr := os.Stat(staging); serr == nil && fi.Mode().IsRegular() {
		events, readErr := readLegacyJSONL(staging)
		if readErr != nil {
			logger.Error.Printf("emit: interrupted legacy wal import: read error at %s: %v — importing the %d events read; original bytes preserved", staging, readErr, len(events))
		}
		logger.Warn.Printf("emit: resuming interrupted legacy wal import from %s (%d events; duplicates possible, deduped downstream)", staging, len(events))
		return events, true, nil
	}

	fi, serr := os.Stat(dir)
	if serr != nil || !fi.Mode().IsRegular() {
		return nil, false, nil // absent, or already a directory — nothing to import
	}

	events, readErr := readLegacyJSONL(dir)
	if readErr != nil {
		if len(events) == 0 {
			// Wholly unreadable (e.g. open failed): leave the file exactly
			// where it is as evidence and refuse to start — renaming it
			// aside would hide the problem behind a fresh empty log.
			logger.Error.Printf("emit: legacy wal file at %s is unreadable: %v — leaving it in place; fix or move it manually", dir, readErr)
			return nil, false, fmt.Errorf("legacy wal file %s unreadable: %w", dir, readErr)
		}
		// Partial read: import the prefix; the staging→.imported file
		// retains every original byte for manual recovery of the rest.
		logger.Error.Printf("emit: legacy wal import read error at %s: %v — importing the %d events read; original preserved at %s.imported", dir, readErr, len(events), dir)
	}

	if err := os.Rename(dir, staging); err != nil {
		return nil, false, fmt.Errorf("stage legacy wal file aside %s: %w", dir, err)
	}
	return events, true, nil
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
//
// On ANY Write error the wal recovers by close-and-reopen (recoverLocked),
// because a failed library Write leaves divergent state in three ways:
//
//	(a) fsync failed after the data hit the file: the library's lastIndex HAS
//	    advanced but nextIndex has not — without recovery every later
//	    Write(nextIndex) returns ErrOutOfOrder forever and the WAL is wedged;
//	(b) partial file write (ENOSPC): torn bytes are on disk but lastIndex did
//	    NOT advance — without recovery the next append lands AFTER the torn
//	    bytes mid-segment, a time bomb that corrupts the whole buffer at the
//	    NEXT restart; reopening immediately surfaces the corruption NOW,
//	    quarantining at most the current buffer, loudly (see openLog);
//	(c) a clean write failure leaves a ghost entry in the library's in-memory
//	    segment buffer — reads are index-shifted until the state is rebuilt.
//
// The failed event itself is returned to the caller, which floors it.
func (w *wal) append(e metering.Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("wal append: %w", tidwall.ErrClosed)
	}
	if w.poisoned {
		if !w.recoverLocked() {
			return fmt.Errorf("wal append: wal poisoned (reopen failing — see prior logs)")
		}
	}

	index := w.nextIndex
	var werr error
	if hook := w.failNextWrite; hook != nil {
		w.failNextWrite = nil
		werr = hook(w.log, index, data)
	} else {
		werr = w.log.Write(index, data)
	}
	if werr != nil {
		w.logger.Error.Printf("emit: wal write index %d failed: %v — recovering by close-and-reopen", index, werr)
		w.recoverLocked()
		return fmt.Errorf("wal write index %d: %w", index, werr)
	}
	w.nextIndex++
	return nil
}

// recoverLocked is the library's prescribed recovery: close the log and
// reopen it (via openLog, which quarantines if the on-disk state is corrupt),
// rebuilding in-memory state from disk and resyncing nextIndex. Reports
// whether the wal is usable afterwards; on failure the wal is poisoned and
// the next append retries. Caller must hold w.mu — no append or drain may
// observe the log mid-swap.
func (w *wal) recoverLocked() bool {
	if w.log != nil {
		// Best-effort: Close may itself report the corruption being recovered.
		_ = w.log.Close()
		w.log = nil
	}

	l, last, err := openLog(w.dir, w.logger)
	if err != nil {
		if !w.poisoned {
			w.logger.Error.Printf("emit: WAL POISONED at %s: recovery reopen failed: %v — events fall to the log floor; the reopen is retried on every append", w.dir, err)
		}
		w.poisoned = true
		return false
	}

	w.log = l
	w.nextIndex = last + 1
	// Conservative reset: the watermark is in-memory only and the reopened
	// log may retain entries that were already shipped. Re-shipping them is
	// safe — delivery is at-least-once and the drainer dedups on request_id;
	// the alternative (trusting a stale watermark against a rebuilt log)
	// risks truncating entries that never shipped.
	w.shippedThrough = 0
	w.poisoned = false
	w.logger.Warn.Printf("emit: wal recovered by close-and-reopen at %s (next index %d; retained entries will be re-shipped)", w.dir, w.nextIndex)
	return true
}

// recoverIfCorruptLocked applies the close-and-reopen recovery when err is
// the library's ErrCorrupt — its documented prescription for that error.
// Other errors (e.g. ErrOutOfRange, ErrClosed) are left to the caller.
// Caller must hold w.mu.
func (w *wal) recoverIfCorruptLocked(err error) {
	if errors.Is(err, tidwall.ErrCorrupt) {
		w.logger.Error.Printf("emit: wal corrupt (%v) — recovering by close-and-reopen", err)
		w.recoverLocked()
	}
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

	// Closed: shutting down. Poisoned: nothing to drain until an append's
	// reopen succeeds. Either way a clean no-op, never a nil-log panic.
	if w.closed || w.poisoned {
		return nil, 0, 0, nil
	}

	first, err := w.log.FirstIndex()
	if err != nil {
		w.recoverIfCorruptLocked(err)
		return nil, 0, 0, fmt.Errorf("wal first index: %w", err)
	}
	last, err := w.log.LastIndex()
	if err != nil {
		w.recoverIfCorruptLocked(err)
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
			// through=0 means nothing gets truncated this pass. On ErrCorrupt
			// the reopen rebuilds state; the next tick resumes from disk.
			w.recoverIfCorruptLocked(rerr)
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

	if w.closed || w.poisoned {
		return nil // no-op; see pending
	}

	last, err := w.log.LastIndex()
	if err != nil {
		w.recoverIfCorruptLocked(err)
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
		// A failed truncate can leave the library corrupt-flagged; recover
		// now if it says so (a flag set without ErrCorrupt surfaces as
		// ErrCorrupt on the next pending call, which also recovers).
		w.recoverIfCorruptLocked(err)
		return fmt.Errorf("wal truncate front %d: %w", tf, err)
	}
	if through > w.shippedThrough {
		w.shippedThrough = through
	}
	return nil
}

// close closes the underlying log. Appends after close fail (→ log floor)
// and never trigger a recovery reopen.
func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.log == nil {
		return nil // poisoned: nothing open
	}
	return w.log.Close()
}
