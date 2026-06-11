package emit

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	tidwall "github.com/tidwall/wal"

	"github.com/saturncloud/phoebe/internal/logging"
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

	// Poisoned: appends keep erroring (→ floor), pending/markShipped no-op.
	if err := w.append(makeEvent("while-poisoned")); err == nil {
		t.Fatal("append on poisoned wal should error (event must floor)")
	}
	events, through, skipped, err := w.pending(0)
	if err != nil || events != nil || through != 0 || skipped != 0 {
		t.Fatalf("pending on poisoned wal = (%v, %d, %d, %v), want clean no-op", events, through, skipped, err)
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
