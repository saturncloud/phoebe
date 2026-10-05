package prune

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestConfigWithDefaults(t *testing.T) {
	c := Config{}.WithDefaults()
	if c.BillingEventRetentionDays != DefaultBillingEventRetentionDays {
		t.Fatalf("billing_event default = %d, want %d", c.BillingEventRetentionDays, DefaultBillingEventRetentionDays)
	}
	if c.IoLogRetentionDays != DefaultIoLogRetentionDays {
		t.Fatalf("io_log default = %d, want %d", c.IoLogRetentionDays, DefaultIoLogRetentionDays)
	}
	if c.BatchSize != DefaultBatchSize {
		t.Fatalf("batchSize default = %d, want %d", c.BatchSize, DefaultBatchSize)
	}
	// Explicit values survive.
	c = Config{BillingEventRetentionDays: 45, IoLogRetentionDays: 3, BatchSize: 100}.WithDefaults()
	if c.BillingEventRetentionDays != 45 || c.IoLogRetentionDays != 3 || c.BatchSize != 100 {
		t.Fatalf("explicit values clobbered: %+v", c)
	}
}

func TestValidateFloors(t *testing.T) {
	// Below the billing_event hard floor: the money-protecting guard.
	bad := Config{BillingEventRetentionDays: MinBillingEventRetentionDays - 1, IoLogRetentionDays: 7, BatchSize: 100}
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "hard floor") {
		t.Fatalf("expected hard-floor error, got %v", err)
	}
	// Below the io_log floor.
	bad = Config{BillingEventRetentionDays: 30, IoLogRetentionDays: 0, BatchSize: 100}
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "ioLog") {
		t.Fatalf("expected io_log floor error, got %v", err)
	}
	// Negative horizons: explicit bad values, refused — never defaulted or
	// clamped (retentionDays is explicit at the store seam).
	bad = Config{BillingEventRetentionDays: -1, IoLogRetentionDays: 7, BatchSize: 100}
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "hard floor") {
		t.Fatalf("expected hard-floor error for -1 billing_event horizon, got %v", err)
	}
	bad = Config{BillingEventRetentionDays: 30, IoLogRetentionDays: -1, BatchSize: 100}
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "ioLog") {
		t.Fatalf("expected ioLog floor error for -1 horizon, got %v", err)
	}
	// WithDefaults maps only UNSET (zero) to the ruled default: an explicit
	// negative survives defaulting, and Validate then refuses it.
	neg := Config{BillingEventRetentionDays: -5}.WithDefaults()
	if neg.BillingEventRetentionDays != -5 {
		t.Fatalf("WithDefaults must preserve an explicit -5, got %d", neg.BillingEventRetentionDays)
	}
	if err := neg.Validate(); err == nil || !strings.Contains(err.Error(), "hard floor") {
		t.Fatalf("expected hard-floor error after defaulting -5, got %v", err)
	}
	// Bad batch size.
	bad = Config{BillingEventRetentionDays: 30, IoLogRetentionDays: 7, BatchSize: 0}
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "batchSize") {
		t.Fatalf("expected batchSize error, got %v", err)
	}
	// At the floors: valid.
	ok := Config{BillingEventRetentionDays: MinBillingEventRetentionDays, IoLogRetentionDays: MinIoLogRetentionDays, BatchSize: 1}
	if err := ok.Validate(); err != nil {
		t.Fatalf("floor values must be valid: %v", err)
	}
}

func TestTableNameIsClosed(t *testing.T) {
	if BillingEvent.Name() != "billing_event" || IoLog.Name() != "io_log" {
		t.Fatalf("table names drifted: %q %q", BillingEvent.Name(), IoLog.Name())
	}
	if Table(99).minRetentionDays() != 1 {
		t.Fatalf("unknown table must fall back to the conservative floor")
	}
}

// cutoffArg matches the Exec's $1 cutoff: the store-derived now-retentionDays
// truncated to the second, within one second of the test's own computation —
// proving the store, not the caller, derives the cutoff.
type cutoffArg struct{ want time.Time }

func (a cutoffArg) Match(v driver.Value) bool {
	t, ok := v.(time.Time)
	if !ok {
		return false
	}
	d := t.Sub(a.want)
	if d < 0 {
		d = -d
	}
	return d <= time.Second
}

// TestStore_PruneLoopsUntilEmptyBatch pins the loop: a full batch followed by
// an empty batch exits with the summed RowsDeleted, and each Exec binds the
// store-derived cutoff and the caller's batchSize.
func TestStore_PruneLoopsUntilEmptyBatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewStore(db)

	wantCutoff := time.Now().Add(-time.Duration(DefaultBillingEventRetentionDays) * 24 * time.Hour).Truncate(time.Second)
	mock.ExpectExec(regexp.QuoteMeta(pruneQuery(BillingEvent))).
		WithArgs(cutoffArg{want: wantCutoff}, 5000).
		WillReturnResult(sqlmock.NewResult(0, 5000))
	mock.ExpectExec(regexp.QuoteMeta(pruneQuery(BillingEvent))).
		WithArgs(cutoffArg{want: wantCutoff}, 5000).
		WillReturnResult(sqlmock.NewResult(0, 0))

	res, err := store.Prune(context.Background(), BillingEvent, DefaultBillingEventRetentionDays, 5000)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if res.RowsDeleted != 5000 {
		t.Fatalf("RowsDeleted = %d, want 5000", res.RowsDeleted)
	}
	if res.Table != BillingEvent {
		t.Fatalf("Result.Table = %v, want BillingEvent", res.Table)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestStore_PruneRefusesBelowFloorNoQuery proves the floor check precedes any
// SQL: ExpectationsWereMet passes only because zero Execs were issued.
func TestStore_PruneRefusesBelowFloorNoQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewStore(db)

	_, err = store.Prune(context.Background(), BillingEvent, MinBillingEventRetentionDays-1, 5000)
	if err == nil || !strings.Contains(err.Error(), "floor") {
		t.Fatalf("expected floor refusal, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("store touched the DB on a refused prune: %v", err)
	}
}

// TestStore_PruneBatchErrorCarriesPartialResult proves a mid-loop batch
// failure returns the wrapped cause with the batches already deleted still on
// the Result (the caller logs/exit-codes on the error, not the partial count).
func TestStore_PruneBatchErrorCarriesPartialResult(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewStore(db)

	mock.ExpectExec(regexp.QuoteMeta(pruneQuery(BillingEvent))).
		WithArgs(sqlmock.AnyArg(), 5000).
		WillReturnResult(sqlmock.NewResult(0, 5000))
	mock.ExpectExec(regexp.QuoteMeta(pruneQuery(BillingEvent))).
		WithArgs(sqlmock.AnyArg(), 5000).
		WillReturnError(errors.New("connection reset"))

	res, err := store.Prune(context.Background(), BillingEvent, DefaultBillingEventRetentionDays, 5000)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "prune billing_event batch") || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("expected wrapped batch error, got %v", err)
	}
	if res.RowsDeleted != 5000 {
		t.Fatalf("partial RowsDeleted = %d, want 5000 carried on the error Result", res.RowsDeleted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestStore_PruneRejectsBadBatchSizeBeforeTouchingDB pins the guard's
// position: it is the FIRST check, before the floor and before any query —
// NewStore(nil) would panic on any DB access, and 0/-1 must not get there.
// (batchSize=0 makes LIMIT 0 delete nothing and report success; negative makes
// LIMIT -1 unlimited, defeating batching.)
func TestStore_PruneRejectsBadBatchSizeBeforeTouchingDB(t *testing.T) {
	s := NewStore(nil)
	for _, bs := range []int{0, -1} {
		_, err := s.Prune(context.Background(), BillingEvent, DefaultBillingEventRetentionDays, bs)
		want := fmt.Sprintf("batchSize must be >= 1, got %d", bs)
		if err == nil || err.Error() != want {
			t.Fatalf("Prune(batchSize=%d) error = %v, want %q", bs, err, want)
		}
	}
}

// TestStore_PruneBatchSizeGuardPrecedesFloor pins the guard ORDER: a call bad
// on both axes reports the batchSize error, not the floor refusal.
func TestStore_PruneBatchSizeGuardPrecedesFloor(t *testing.T) {
	s := NewStore(nil)
	_, err := s.Prune(context.Background(), BillingEvent, MinBillingEventRetentionDays-1, 0)
	if err == nil || !strings.Contains(err.Error(), "batchSize") {
		t.Fatalf("expected the batchSize guard first, got %v", err)
	}
}
