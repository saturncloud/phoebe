package emit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	tidwall "github.com/tidwall/wal"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
)

// ---- helpers ----------------------------------------------------------------

// tearTailSegment appends garbage bytes to the END of the log's tail segment
// file, simulating a partial (torn) write — the on-disk effect of ENOSPC
// mid-append. 0xFF bytes are an unterminated uvarint, which tidwall's entry
// loader reports as ErrCorrupt (verified empirically against v1.2.1: torn
// bytes at the very tail fail Open with ErrCorrupt, same as mid-file).
func tearTailSegment(t *testing.T, dir string) {
	t.Helper()
	segs, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil || len(segs) == 0 {
		t.Fatalf("no segment files in %s (err=%v)", dir, err)
	}
	sort.Strings(segs)
	f, err := os.OpenFile(segs[len(segs)-1], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open tail segment: %v", err)
	}
	defer f.Close() //nolint:errcheck
	if _, err := f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatalf("tear tail segment: %v", err)
	}
}

func corruptDirCount(t *testing.T, path string) int {
	t.Helper()
	matches, err := filepath.Glob(path + ".corrupt.*")
	if err != nil {
		t.Fatal(err)
	}
	return len(matches)
}

// ---- MUST FIX (a): failed Write must not wedge the WAL forever ---------------

// TestWAL_WriteErrorRecovers_NoPermanentWedge is the regression for failure
// mode (a): fsync fails AFTER the data hit the file, so the library's
// lastIndex advanced but the wrapper's nextIndex did not. Without the
// close-and-reopen recovery, every subsequent Write(nextIndex) returns
// ErrOutOfOrder FOREVER and every event for the rest of the process floors.
func TestWAL_WriteErrorRecovers_NoPermanentWedge(t *testing.T) {
	w := openTestWAL(t, tmpWALPath(t))
	defer w.close() //nolint:errcheck

	if err := w.append(makeEvent("before")); err != nil {
		t.Fatal(err)
	}

	// Simulate (a): the write lands (library lastIndex advances) but the
	// fsync is reported failed — the wrapper must treat the write as failed.
	w.failNextWrite = func(log *tidwall.Log, index uint64, data []byte) error {
		if err := log.Write(index, data); err != nil {
			t.Fatalf("injected real write: %v", err)
		}
		return errors.New("injected fsync failure")
	}
	if err := w.append(makeEvent("fsync-fail")); err == nil {
		t.Fatal("append with failing fsync should return an error (event must floor)")
	}

	// THE regression: subsequent appends must succeed, not ErrOutOfOrder-loop.
	for i := range 3 {
		if err := w.append(makeEvent(fmt.Sprintf("after-%d", i))); err != nil {
			t.Fatalf("append after recovery: %v (permanent wedge — failure mode (a))", err)
		}
	}

	got := map[string]bool{}
	for _, ev := range unshipped(t, w) {
		got[ev.RequestID] = true
	}
	for _, want := range []string{"before", "after-0", "after-1", "after-2"} {
		if !got[want] {
			t.Errorf("event %q missing from WAL after recovery", want)
		}
	}
}

// ---- MUST FIX (b): torn write must not plant a restart time bomb -------------

// TestWAL_WriteErrorNoSilentCorruption is the regression for failure mode
// (b): a partial write (ENOSPC) leaves torn bytes mid-segment. The old code
// kept appending AFTER the torn bytes, so the NEXT restart hit ErrCorrupt and
// quarantined the whole buffer at its fullest — maximal, delayed, silent
// loss. Now the reopen-after-failed-write surfaces the corruption
// immediately: the pre-failure buffer is quarantined ONCE, loudly, at failure
// time, and everything appended afterwards survives a restart cleanly.
func TestWAL_WriteErrorNoSilentCorruption(t *testing.T) {
	path := tmpWALPath(t)
	w := openTestWAL(t, path)

	for i := range 2 {
		if err := w.append(makeEvent(fmt.Sprintf("pre-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Simulate ENOSPC: torn bytes land on the tail segment, the write fails,
	// and the library's lastIndex did NOT advance.
	w.failNextWrite = func(_ *tidwall.Log, _ uint64, _ []byte) error {
		tearTailSegment(t, path)
		return errors.New("injected ENOSPC partial write")
	}
	if err := w.append(makeEvent("torn")); err == nil {
		t.Fatal("append with torn write should return an error (event must floor)")
	}

	// Honest trade, asserted: the recovery reopen found the torn tail and
	// quarantined the pre-failure buffer — bounded, loud, recoverable.
	if n := corruptDirCount(t, path); n != 1 {
		t.Fatalf("want exactly 1 quarantined dir after torn-write recovery, got %d", n)
	}

	// Post-recovery appends land in the fresh log.
	if err := w.append(makeEvent("after-torn")); err != nil {
		t.Fatalf("append after torn-write recovery: %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}

	// Simulated restart: Open must succeed with NO new quarantine, and the
	// post-recovery entries must be readable — no time bomb left behind.
	w2 := openTestWAL(t, path)
	defer w2.close() //nolint:errcheck
	if n := corruptDirCount(t, path); n != 1 {
		t.Fatalf("restart after recovery quarantined again: %d corrupt dirs, want 1", n)
	}
	got := unshipped(t, w2)
	if len(got) != 1 || got[0].RequestID != "after-torn" {
		t.Fatalf("after restart: unshipped = %+v, want exactly [after-torn]", got)
	}
}

// ---- MUST FIX: poisoned WAL floors cleanly and recovers ----------------------

// TestWAL_PoisonedFloorsAndRecovers: when the recovery reopen ITSELF fails
// (here: the quarantine rename is blocked by an unwritable parent dir), the
// wal is poisoned — appends error immediately (the emitter floors them) and
// pending/markShipped no-op — and the reopen is retried on every append, so
// restoring the dir heals the wal without a restart.
func TestWAL_PoisonedFloorsAndRecovers(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based test is meaningless as root")
	}

	parent := t.TempDir()
	path := filepath.Join(parent, "wal.jsonl")
	w := openTestWAL(t, path)
	defer w.close() //nolint:errcheck

	if err := w.append(makeEvent("pre")); err != nil {
		t.Fatal(err)
	}
	if got, ok := w.pendingCount(); got != 1 || !ok {
		t.Fatalf("pendingCount before poisoning = %d, %v, want 1, true", got, ok)
	}

	// Make the recovery reopen fail: the torn tail forces ErrCorrupt at Open,
	// and the read-only parent blocks the quarantine rename.
	w.failNextWrite = func(_ *tidwall.Log, _ uint64, _ []byte) error {
		tearTailSegment(t, path)
		if err := os.Chmod(parent, 0o500); err != nil {
			t.Fatalf("chmod parent: %v", err)
		}
		return errors.New("injected write failure")
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o755) })

	if err := w.append(makeEvent("poisoning")); err == nil {
		t.Fatal("append triggering a failed recovery should error")
	}

	// Poisoned: appends keep erroring (→ floor), pending/markShipped no-op while
	// the reopen keeps failing.
	if err := w.append(makeEvent("while-poisoned")); err == nil {
		t.Fatal("append on poisoned wal should error (event must floor)")
	}
	if !w.isPoisoned() {
		t.Fatal("isPoisoned = false while the reopen is failing")
	}
	// A poisoned wal has no open log to count, and entries may be on disk, so it
	// reports the count as unknown rather than as an empty WAL.
	if got, ok := w.pendingCount(); got != 0 || ok {
		t.Fatalf("pendingCount while poisoned = %d, %v, want 0, false", got, ok)
	}
	events, indexes, through, skipped, err := w.pending(0)
	if err != nil || events != nil || indexes != nil || through != 0 || skipped != nil {
		t.Fatalf("pending on poisoned wal = (%v, %d, %v, %v), want clean no-op", events, through, skipped, err)
	}
	if err := w.markShipped(99); err != nil {
		t.Fatalf("markShipped on poisoned wal should no-op, got %v", err)
	}

	// Disk "recovers": the next append's reopen retry must heal the wal.
	if err := os.Chmod(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := w.append(makeEvent("healed")); err != nil {
		t.Fatalf("append after restoring permissions: %v (poisoned wal never recovered)", err)
	}
	got := unshipped(t, w)
	if len(got) != 1 || got[0].RequestID != "healed" {
		t.Fatalf("after heal: unshipped = %+v, want exactly [healed]", got)
	}
	if got, ok := w.pendingCount(); got != 1 || !ok {
		t.Fatalf("pendingCount after heal = %d, %v, want 1, true", got, ok)
	}
	if w.isPoisoned() {
		t.Fatal("isPoisoned = true after heal")
	}
}

// poisonWAL makes the recovery reopen fail: the torn tail forces ErrCorrupt at
// Open, and the read-only parent blocks the quarantine rename. The caller restores
// the parent's permissions to heal.
func poisonWAL(t *testing.T, w *wal, parent, path string) {
	t.Helper()
	w.failNextWrite = func(_ *tidwall.Log, _ uint64, _ []byte) error {
		tearTailSegment(t, path)
		if err := os.Chmod(parent, 0o500); err != nil {
			t.Fatalf("chmod parent: %v", err)
		}
		return errors.New("injected write failure")
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o755) })
	if err := w.append(makeEvent("poisoning")); err == nil {
		t.Fatal("append triggering a failed recovery should error")
	}
}

// TestWAL_PoisonedRecoversFromShipperWithoutAppend: a replica with no new traffic
// must still heal, so the shipper's pending call retries the reopen too.
func TestWAL_PoisonedRecoversFromShipperWithoutAppend(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based test is meaningless as root")
	}
	parent := t.TempDir()
	path := filepath.Join(parent, "wal.jsonl")
	w := openTestWAL(t, path)
	defer w.close() //nolint:errcheck
	if err := w.append(makeEvent("pre")); err != nil {
		t.Fatal(err)
	}
	poisonWAL(t, w, parent, path)

	if events, _, through, _, err := w.pending(0); err != nil || events != nil || through != 0 {
		t.Fatalf("pending with the reopen still failing = (%v, %d, %v), want a clean no-op", events, through, err)
	}
	if !w.isPoisoned() {
		t.Fatal("pending healed a wal whose reopen still fails")
	}

	if err := os.Chmod(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := w.pending(0); err != nil {
		t.Fatalf("pending after restoring permissions: %v", err)
	}
	if w.isPoisoned() {
		t.Fatal("pending did not heal the wal once the reopen could succeed")
	}
	if _, ok := w.pendingCount(); !ok {
		t.Fatal("pendingCount still unknown after pending healed the wal")
	}
}

// TestWALPoisoned_ReportedByEmitter: the emitter exports the poisoned state and an
// unknown count, which is what the metrics read.
func TestWALPoisoned_ReportedByEmitter(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based test is meaningless as root")
	}
	parent := t.TempDir()
	cfg := testConfig(t, nil)
	cfg.WALPath = filepath.Join(parent, "wal.jsonl")
	cfg.ShipInterval = time.Hour
	em, err := New(cfg, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = em.Close(context.Background()) })
	if em.WALPoisoned() {
		t.Fatal("WALPoisoned = true on a healthy WAL")
	}
	if err := em.wal.append(makeEvent("pre")); err != nil {
		t.Fatal(err)
	}
	poisonWAL(t, em.wal, parent, cfg.WALPath)
	if !em.WALPoisoned() {
		t.Fatal("WALPoisoned = false on a poisoned WAL")
	}
	if got, ok := em.WALPending(); got != 0 || ok {
		t.Fatalf("WALPending while poisoned = %d, %v, want 0, false", got, ok)
	}
}

// TestWALPending_ReadErrorIsUnknown: an unreadable log reports an unknown count,
// not an empty WAL.
func TestWALPending_ReadErrorIsUnknown(t *testing.T) {
	w := openTestWAL(t, tmpWALPath(t))
	defer w.close() //nolint:errcheck
	if err := w.append(makeEvent("pre")); err != nil {
		t.Fatal(err)
	}
	// Closing the library log underneath the wrapper makes FirstIndex fail.
	if err := w.log.Close(); err != nil {
		t.Fatal(err)
	}
	if got, ok := w.pendingCount(); got != 0 || ok {
		t.Fatalf("pendingCount on a read error = %d, %v, want 0, false", got, ok)
	}
}

// TestWALQuarantinedDirs_CountsAfterTornWrite: a torn write moves the buffer aside
// with its unbilled events, and the count must show that.
func TestWALQuarantinedDirs_CountsAfterTornWrite(t *testing.T) {
	path := tmpWALPath(t)
	w := openTestWAL(t, path)
	defer w.close() //nolint:errcheck
	if got := w.quarantinedDirs(); got != 0 {
		t.Fatalf("quarantinedDirs on a fresh wal = %d", got)
	}
	if err := w.append(makeEvent("pre")); err != nil {
		t.Fatal(err)
	}
	w.failNextWrite = func(_ *tidwall.Log, _ uint64, _ []byte) error {
		tearTailSegment(t, path)
		return errors.New("injected ENOSPC partial write")
	}
	if err := w.append(makeEvent("torn")); err == nil {
		t.Fatal("torn write should error")
	}
	if got := w.quarantinedDirs(); got != 1 {
		t.Fatalf("quarantinedDirs after a torn write = %d, want 1", got)
	}
}

// TestWALQuarantinedDirs_CountsAfterRestartOnTornTail: the count reads the disk, so
// a quarantine that happened at startup, before any scrape, is still visible.
func TestWALQuarantinedDirs_CountsAfterRestartOnTornTail(t *testing.T) {
	path := tmpWALPath(t)
	w := openTestWAL(t, path)
	if err := w.append(makeEvent("pre")); err != nil {
		t.Fatal(err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	tearTailSegment(t, path)

	cfg := testConfig(t, nil)
	cfg.WALPath = path
	cfg.ShipInterval = time.Hour
	em, err := New(cfg, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = em.Close(context.Background()) })
	if got := em.WALQuarantinedDirs(); got != 1 {
		t.Fatalf("WALQuarantinedDirs after a restart on a torn tail = %d, want 1", got)
	}
}

// ---- SHOULD FIX: legacy import crash window ----------------------------------

// TestWAL_LegacyImportCrashWindow: the import appends to the new log FIRST
// and parks the staging file LAST, so a crash anywhere in the window leaves
// "<path>.importing" behind and the next startup re-imports it. Duplicates
// are the deliberate side of the trade (the drainer dedups on request_id);
// the old rename-first order parked events invisibly instead.
func TestWAL_LegacyImportCrashWindow(t *testing.T) {
	legacyLines := func(ids ...string) []byte {
		var buf []byte
		for _, id := range ids {
			line, err := json.Marshal(makeEvent(id))
			if err != nil {
				t.Fatal(err)
			}
			buf = append(buf, line...)
			buf = append(buf, '\n')
		}
		return buf
	}

	t.Run("crash before any append", func(t *testing.T) {
		path := tmpWALPath(t)
		// Crash right after staging: .importing exists, no new log yet.
		if err := os.WriteFile(path+".importing", legacyLines("cw-1", "cw-2"), 0o600); err != nil {
			t.Fatal(err)
		}

		w := openTestWAL(t, path)
		defer w.close() //nolint:errcheck

		got := unshipped(t, w)
		if len(got) != 2 || got[0].RequestID != "cw-1" || got[1].RequestID != "cw-2" {
			t.Fatalf("re-import after crash: unshipped = %+v, want [cw-1 cw-2]", got)
		}
		if _, err := os.Stat(path + ".importing"); !os.IsNotExist(err) {
			t.Errorf("staging file not parked after successful import (err=%v)", err)
		}
		if _, err := os.Stat(path + ".imported"); err != nil {
			t.Errorf("imported file missing: %v", err)
		}
	})

	t.Run("crash after appends before park", func(t *testing.T) {
		path := tmpWALPath(t)
		// The events already made it into the new log...
		w := openTestWAL(t, path)
		for _, id := range []string{"cw-1", "cw-2"} {
			if err := w.append(makeEvent(id)); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.close(); err != nil {
			t.Fatal(err)
		}
		// ...but the crash hit before the .importing → .imported rename.
		if err := os.WriteFile(path+".importing", legacyLines("cw-1", "cw-2"), 0o600); err != nil {
			t.Fatal(err)
		}

		w2 := openTestWAL(t, path)
		defer w2.close() //nolint:errcheck

		// Re-imported: duplicates, never invisible parking.
		ids := map[string]int{}
		for _, ev := range unshipped(t, w2) {
			ids[ev.RequestID]++
		}
		if ids["cw-1"] != 2 || ids["cw-2"] != 2 {
			t.Fatalf("want each event twice (original + re-import), got %v", ids)
		}
		if _, err := os.Stat(path + ".importing"); !os.IsNotExist(err) {
			t.Errorf("staging file not parked after re-import (err=%v)", err)
		}
	})
}

// TestWAL_ImportedLeftoverWarns: a parked .imported file is forensic data an
// operator must eventually deal with; every startup that sees one must warn.
func TestWAL_ImportedLeftoverWarns(t *testing.T) {
	path := tmpWALPath(t)
	if err := os.WriteFile(path+".imported", []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var warnBuf bytes.Buffer
	logger := logging.New(logging.DEBUG)
	logger.Warn.SetOutput(&warnBuf)

	w, err := openWAL(path, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer w.close() //nolint:errcheck

	if !strings.Contains(warnBuf.String(), ".imported") {
		t.Fatalf("startup with leftover .imported file did not warn; warn log: %q", warnBuf.String())
	}
}

// TestWAL_LegacyUnreadable: a legacy file that cannot even be opened is left
// exactly where it is (evidence) and openWAL fails loudly — renaming it aside
// would hide unbilled events behind a fresh empty log.
func TestWAL_LegacyUnreadable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based test is meaningless as root")
	}

	path := tmpWALPath(t)
	if err := os.WriteFile(path, []byte("{}\n"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o600) })

	if _, err := openWAL(path, testLogger()); err == nil {
		t.Fatal("openWAL with unreadable legacy file should error")
	}
	// Evidence left in place: no rename aside.
	if fi, err := os.Stat(path); err != nil || !fi.Mode().IsRegular() {
		t.Errorf("legacy file moved or deleted (err=%v)", err)
	}
	for _, suffix := range []string{".importing", ".imported"} {
		if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
			t.Errorf("unreadable legacy file was renamed to %s (err=%v)", suffix, err)
		}
	}
}

// TestWAL_PreCutoverSpoolReplaysAsDedicated pins that a spool written by a
// pre-cutover pod (no serving_mode key) keeps the dedicated default when a
// post-cutover pod replays it. The replay re-marshals the event, and
// ServingMode is no longer omitempty, so decoding without the absent-key rule
// would ship an explicit "" that the drain stores as the empty string and the
// rater withholds. An explicit "" in the spool stays "" so it is still
// withheld.
func TestWAL_PreCutoverSpoolReplaysAsDedicated(t *testing.T) {
	path := tmpWALPath(t)

	// Legacy JSONL file (the file-to-directory import path).
	legacy := `{"request_id":"pre-cutover"}` + "\n" + `{"request_id":"explicit-empty","serving_mode":""}` + "\n"
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	w := openTestWAL(t, path) // imports the legacy file into the tidwall log
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	// Tidwall log entry written raw by a pre-cutover pod.
	l, err := tidwall.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	last, err := l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Write(last+1, []byte(`{"request_id":"pre-cutover-log"}`)); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	w = openTestWAL(t, path)
	defer w.close() //nolint:errcheck
	got := unshipped(t, w)

	want := map[string]string{"pre-cutover": "dedicated", "explicit-empty": "", "pre-cutover-log": "dedicated"}
	if len(got) != len(want) {
		t.Fatalf("replayed %d events, want %d", len(got), len(want))
	}
	for _, ev := range got {
		if ev.ServingMode != want[ev.RequestID] {
			t.Errorf("request %s serving_mode = %q, want %q", ev.RequestID, ev.ServingMode, want[ev.RequestID])
		}
		raw, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		shipped, err := metering.UnmarshalEvent(raw)
		if err != nil {
			t.Fatal(err)
		}
		if shipped.ServingMode != want[ev.RequestID] {
			t.Errorf("request %s serving_mode after re-marshal = %q, want %q", ev.RequestID, shipped.ServingMode, want[ev.RequestID])
		}
	}
}

// ---- undecodable entries: logged raw, counted once ----------------------------

// walWithCapturedErrors opens a WAL whose ERROR log is returned for assertions.
func walWithCapturedErrors(t *testing.T) (*wal, *bytes.Buffer) {
	t.Helper()
	var logged bytes.Buffer
	logger := logging.New(logging.ERROR)
	logger.Error.SetOutput(&logged)
	w, err := openWAL(tmpWALPath(t), logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.close() })
	return w, &logged
}

// TestWAL_UndecodableEntryIsLoggedWithRawBytes: the shipper truncates past an entry
// it cannot decode, so the log line must hold the raw bytes before the truncate. The
// wrongly typed field is the case a rolling upgrade produces: valid JSON that still
// fails to decode.
func TestWAL_UndecodableEntryIsLoggedWithRawBytes(t *testing.T) {
	for _, tc := range []struct {
		name, raw, want string
	}{
		{"garbage bytes", "\x00 not json", `\x00 not json`},
		{"wrongly typed field", `{"request_id":"r-lost","prompt_tokens":"eleven"}`, `r-lost`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, logged := walWithCapturedErrors(t)
			if err := w.append(makeEvent("ok")); err != nil {
				t.Fatal(err)
			}
			appendRaw(t, w, []byte(tc.raw))

			events, _, through, skipped, err := w.pending(0)
			if err != nil {
				t.Fatal(err)
			}
			if len(events) != 1 || len(skipped) != 1 || skipped[0] != 2 || through != 2 {
				t.Fatalf("pending = %d events, skipped %v, through %d", len(events), skipped, through)
			}
			// Asserted before markShipped: the copy must exist before the truncate.
			for _, want := range []string{"index=2", tc.want} {
				if !strings.Contains(logged.String(), want) {
					t.Fatalf("log %q does not contain %q", logged.String(), want)
				}
			}
		})
	}
}

// TestWAL_UndecodableEntryLogLineIsBounded: one oversized entry must not produce an
// unbounded log line, and the line records the full length so a cut copy is obvious.
func TestWAL_UndecodableEntryLogLineIsBounded(t *testing.T) {
	w, logged := walWithCapturedErrors(t)
	appendRaw(t, w, []byte(strings.Repeat("x", 3*maxLoggedRawEntry)))
	if _, _, _, _, err := w.pending(0); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logged.String(), fmt.Sprintf("(%d bytes)", 3*maxLoggedRawEntry)) {
		t.Fatalf("log does not record the full length: %.200q", logged.String())
	}
	if got := strings.Count(logged.String(), "x"); got > maxLoggedRawEntry+10 {
		t.Fatalf("log holds %d bytes of the entry, limit %d", got, maxLoggedRawEntry)
	}
}

// TestWAL_UndecodableEntryIsLoggedOncePerProcess: while Valkey refuses writes the
// shipper re-reads the same entry on every tick, and a reopen re-reads it again.
// Neither may repeat the raw line.
func TestWAL_UndecodableEntryIsLoggedOncePerProcess(t *testing.T) {
	w, logged := walWithCapturedErrors(t)
	appendRaw(t, w, []byte("\x00 not json"))
	for range 3 {
		if _, _, _, _, err := w.pending(0); err != nil {
			t.Fatal(err)
		}
	}
	w.mu.Lock()
	w.recoverLocked()
	w.mu.Unlock()
	if _, _, _, _, err := w.pending(0); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(logged.String(), "undecodable wal entry"); got != 1 {
		t.Fatalf("raw line logged %d times, want 1: %s", got, logged.String())
	}
}

// TestWAL_ReopenDoesNotRecountSkippedEntries: the log retains its tail entry and a
// reopen forgets the shipped watermark, so a corrupt tail entry is read again. The
// counter says how many records were lost, and this is still one.
func TestWAL_ReopenDoesNotRecountSkippedEntries(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig(t, mr)
	cfg.ShipInterval = time.Hour
	rdb := redisClient(mr.Addr())
	t.Cleanup(func() { _ = rdb.Close() })
	em, err := New(cfg, testLogger(), rdb)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = em.Close(context.Background()) })

	appendRaw(t, em.wal, []byte("\x00 not json"))
	em.drainWAL()
	if got := em.WALSkippedEntries(); got != 1 {
		t.Fatalf("WALSkippedEntries after the first drain = %d, want 1", got)
	}
	em.wal.mu.Lock()
	em.wal.recoverLocked()
	em.wal.mu.Unlock()
	em.drainWAL()
	if got := em.WALSkippedEntries(); got != 1 {
		t.Fatalf("WALSkippedEntries after a reopen = %d, want 1", got)
	}
	// A later corrupt entry is a new loss and must still count.
	appendRaw(t, em.wal, []byte("\x00 also not json"))
	em.drainWAL()
	if got := em.WALSkippedEntries(); got != 2 {
		t.Fatalf("WALSkippedEntries after a new corrupt entry = %d, want 2", got)
	}
}

// TestWAL_QuarantineClearsSkipWatermarks: a quarantine replaces the log and indexes
// restart from 1, so a watermark from the old log would hide new corrupt entries from
// both the counter and the raw log line.
func TestWAL_QuarantineClearsSkipWatermarks(t *testing.T) {
	w, logged := walWithCapturedErrors(t)
	appendRaw(t, w, []byte("\x00 old 1"))
	appendRaw(t, w, []byte("\x00 old 2"))
	_, _, through, _, err := w.pending(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.markShipped(through); err != nil {
		t.Fatal(err)
	}

	w.mu.Lock()
	_ = w.log.Close()
	w.log = nil
	tearTailSegment(t, w.dir)
	if !w.recoverLocked() {
		w.mu.Unlock()
		t.Fatal("recovery failed")
	}
	w.mu.Unlock()
	if got := w.quarantinedDirs(); got != 1 {
		t.Fatalf("quarantinedDirs = %d, want 1", got)
	}

	logged.Reset()
	appendRaw(t, w, []byte("\x00 new"))
	_, _, _, skipped, err := w.pending(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(skipped) != 1 || skipped[0] != 1 {
		t.Fatalf("skipped on the new log = %v, want [1]", skipped)
	}
	if !strings.Contains(logged.String(), "new") {
		t.Fatalf("new corrupt entry not logged: %q", logged.String())
	}
}

// TestMarkPartialBatch_TruncateFailureKeepsEntriesAndDoesNotCountSkips: when the
// truncate fails after a partial ship, the watermark must not move, so nothing is
// lost, and the corrupt entry the prefix covered is counted once, on the retry.
func TestMarkPartialBatch_TruncateFailureKeepsEntriesAndDoesNotCountSkips(t *testing.T) {
	cfg := testConfig(t, nil)
	cfg.ShipInterval = time.Hour
	var logged bytes.Buffer
	logger := logging.New(logging.ERROR)
	logger.Error.SetOutput(&logged)
	rdb := &oomAfterXAdd{allowed: 1 << 30}
	em, err := New(cfg, logger, rdb)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = em.Close(context.Background()) })

	if err := em.wal.append(makeEvent("r-0")); err != nil {
		t.Fatal(err)
	}
	appendRaw(t, em.wal, []byte("\x00 not json"))
	for _, id := range []string{"r-1", "r-2"} {
		if err := em.wal.append(makeEvent(id)); err != nil {
			t.Fatal(err)
		}
	}

	// Ship two events, passing the corrupt entry, and close the log underneath so
	// that the truncate after them fails.
	em.shipFn = func([]metering.Event) int {
		em.wal.mu.Lock()
		defer em.wal.mu.Unlock()
		if err := em.wal.log.Close(); err != nil {
			t.Error(err)
		}
		return 2
	}
	em.drainWAL()
	if !strings.Contains(logged.String(), "wal truncate after partial ship") {
		t.Fatalf("truncate failure not logged: %q", logged.String())
	}
	if got := em.WALSkippedEntries(); got != 0 {
		t.Fatalf("WALSkippedEntries after a failed truncate = %d, want 0", got)
	}

	// Clear the failure. Every event must still be there to read.
	em.wal.mu.Lock()
	em.wal.recoverLocked()
	em.wal.mu.Unlock()
	events, _, _, _, err := em.wal.pending(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("pending after a failed truncate = %d events, want 3", len(events))
	}

	em.shipFn = em.shipBatch
	em.drainWAL()
	if got := strings.Join(rdb.ids, ","); got != "r-0,r-1,r-2" {
		t.Fatalf("shipped %s, want every event", got)
	}
	if got := walPendingCount(t, em); got != 0 {
		t.Fatalf("WALPending after the retry = %d, want 0", got)
	}
	if got := em.WALSkippedEntries(); got != 1 {
		t.Fatalf("WALSkippedEntries after the retry = %d, want 1", got)
	}
}

// ---- quarantine only on corruption, and an honest count of it -----------------

// TestOpenLog_TransientErrorDoesNotQuarantine: an open error that says nothing about
// the data must leave the directory in place, because it holds valid unshipped
// events. The wal stays poisoned and heals when the error clears.
func TestOpenLog_TransientErrorDoesNotQuarantine(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	path := tmpWALPath(t)
	w := openTestWAL(t, path)
	defer w.close() //nolint:errcheck
	if err := w.append(makeEvent("unshipped")); err != nil {
		t.Fatal(err)
	}

	w.mu.Lock()
	_ = w.log.Close()
	w.log = nil
	w.mu.Unlock()
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o755) })

	if _, _, err := openLog(path, testLogger()); err == nil {
		t.Fatal("openLog on an unreadable directory should error")
	}
	w.mu.Lock()
	recovered := w.recoverLocked()
	w.mu.Unlock()
	if recovered || !w.isPoisoned() {
		t.Fatalf("recovered = %v, poisoned = %v, want a poisoned wal", recovered, w.isPoisoned())
	}
	if got := corruptDirCount(t, path); got != 0 {
		t.Fatalf(".corrupt dirs after a transient error = %d, want 0", got)
	}

	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatal(err)
	}
	events, _, _, _, err := w.pending(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].RequestID != "unshipped" {
		t.Fatalf("events after the error cleared = %+v, want the unshipped one", events)
	}
	if w.isPoisoned() {
		t.Fatal("wal still poisoned after the error cleared")
	}
}

// TestOpenLog_DamagedSegmentIsQuarantined: real damage must still move aside, or a
// corrupt buffer would block the WAL forever.
func TestOpenLog_DamagedSegmentIsQuarantined(t *testing.T) {
	for _, tc := range []struct {
		name    string
		segment string
		content []byte
	}{
		{"garbage segment (ErrCorrupt)", "00000000000000000001", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
		{"empty tail segment past index 1 (ErrEmptyLog)", "00000000000000000005", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := tmpWALPath(t)
			if err := os.MkdirAll(path, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(path, tc.segment), tc.content, 0o644); err != nil {
				t.Fatal(err)
			}
			l, _, err := openLog(path, testLogger())
			if err != nil {
				t.Fatalf("openLog: %v", err)
			}
			defer l.Close() //nolint:errcheck
			if got := corruptDirCount(t, path); got != 1 {
				t.Fatalf(".corrupt dirs = %d, want 1", got)
			}
		})
	}
}

// TestQuarantinedDirs_PathWithGlobMetacharacters: the WAL path is configuration, so a
// '[' in it must not make the count fail or read 0.
func TestQuarantinedDirs_PathWithGlobMetacharacters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal[x")
	w := openTestWAL(t, path)
	defer w.close() //nolint:errcheck
	if err := os.Mkdir(path+".corrupt.1", 0o755); err != nil {
		t.Fatal(err)
	}
	if got := w.quarantinedDirs(); got != 1 {
		t.Fatalf("quarantinedDirs = %d, want 1", got)
	}
}

// TestQuarantinedDirs_UnreadableIsUnknownNotZero: zero tells an operator that no
// unbilled data is set aside, so a count that could not be taken must not read 0.
func TestQuarantinedDirs_UnreadableIsUnknownNotZero(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "gone")
	if err := os.Mkdir(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	logger := logging.New(logging.ERROR)
	logger.Error.SetOutput(&logged)
	w, err := openWAL(filepath.Join(parent, "wal"), logger)
	if err != nil {
		t.Fatal(err)
	}
	defer w.close() //nolint:errcheck
	if err := os.RemoveAll(parent); err != nil {
		t.Fatal(err)
	}
	if got := w.quarantinedDirs(); got != quarantinedDirsUnknown {
		t.Fatalf("quarantinedDirs = %d, want the unknown value", got)
	}
	if !strings.Contains(logged.String(), "cannot count quarantined") {
		t.Fatalf("failed count not logged: %q", logged.String())
	}
}

// TestFloorEmitter_ReportsQuarantinedDirsOnDisk: a failed start can leave corrupt
// directories holding unbilled events. The replica running the floor emitter is the
// one an operator most needs this count from.
func TestFloorEmitter_ReportsQuarantinedDirsOnDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	for _, name := range []string{".corrupt.1", ".corrupt.2", "-other"} {
		if err := os.Mkdir(path+name, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if got := NewFloorEmitter(testLogger(), path).WALQuarantinedDirs(); got != 2 {
		t.Fatalf("WALQuarantinedDirs = %d, want 2", got)
	}
	if got := NewFloorEmitter(testLogger(), "").WALQuarantinedDirs(); got != 0 {
		t.Fatalf("WALQuarantinedDirs with no WAL path = %d, want 0", got)
	}
	missing := filepath.Join(path+"-absent", "wal")
	if got := NewFloorEmitter(testLogger(), missing).WALQuarantinedDirs(); got != quarantinedDirsUnknown {
		t.Fatalf("WALQuarantinedDirs on an unreadable parent = %d, want the unknown value", got)
	}
}
