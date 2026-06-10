package drain

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/saturncloud/phoebe/internal/metering"
)

// TestPostgresStore_UpsertSQL verifies the upsert emits a single parameterised
// multi-row INSERT ... ON CONFLICT (request_id) DO NOTHING inside a
// transaction, and that NULL/typed values are bound correctly.
func TestPostgresStore_UpsertSQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	ts := int64(1_700_000_000_000)
	events := []metering.Event{
		{
			RequestID:        "req-1",
			AuthID:           "auth-1",
			Model:            "m1",
			PromptTokens:     5,
			CompletionTokens: 7,
			TimestampUnixMs:  ts,
		},
		{
			RequestID: "req-2",
			// No identity fields set → must bind NULL, not "".
			Model: "m2",
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		"INSERT INTO billing_event (request_id, auth_id, user_id, group_id, resource_id, resource_type, model, adapter, prompt_tokens, cached_tokens, completion_tokens, finish_reason, gpu_type, aborted, event_ts) VALUES",
	)).
		WithArgs(
			// row 1
			"req-1", "auth-1", nil, nil, nil, nil, "m1", nil, 5, 0, 7, nil, nil, false, time.UnixMilli(ts).UTC(),
			// row 2 (no identity, no timestamp → event_ts NULL)
			"req-2", nil, nil, nil, nil, nil, "m2", nil, 0, 0, 0, nil, nil, false, nil,
		).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	if err := store.Upsert(context.Background(), events); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestPostgresStore_UpsertEmptyNoop proves an empty batch issues no SQL.
func TestPostgresStore_UpsertEmptyNoop(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	if err := store.Upsert(context.Background(), nil); err != nil {
		t.Fatalf("Upsert(nil): %v", err)
	}
	// No Begin/Exec/Commit expected.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected SQL issued: %v", err)
	}
}

// TestPostgresStore_UpsertRollsBackOnExecError proves a failed Exec rolls back
// (no partial commit) and returns a semantic error — the caller then withholds
// the XACK and the batch redelivers.
func TestPostgresStore_UpsertRollsBackOnExecError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO billing_event").
		WillReturnError(errors.New("relation \"billing_event\" does not exist"))
	mock.ExpectRollback()

	err = store.Upsert(context.Background(), []metering.Event{{RequestID: "r", Model: "m"}})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestPostgresStore_EmptyModelStoredAsNull locks the anomaly-bucket contract:
// an event whose upstream never reported a model (capture.Result.Model == "")
// must bind model as NULL, not '' — the rater's unattributable predicate is
// `model_id IS NULL`, and a stored '' would dodge it and be misreported as
// UNPRICED (wrong runbook: "backfill prices" instead of "capture gap").
func TestPostgresStore_EmptyModelStoredAsNull(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO billing_event").
		WithArgs(
			"req-no-model", "auth-1", nil, nil, nil, nil,
			nil, // model: "" must bind NULL
			nil, 1, 0, 2, nil, nil, false, nil,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = store.Upsert(context.Background(), []metering.Event{{
		RequestID:        "req-no-model",
		AuthID:           "auth-1",
		Model:            "", // upstream never reported a model
		PromptTokens:     1,
		CompletionTokens: 2,
	}})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestEventArgs_NullsEmptyIdentities locks the encoding contract: empty identity
// strings bind as NULL (so billing GROUP BY auth_id never gets a spurious ""
// bucket), token counts always bind (never nil), and event_ts is NULL when the
// timestamp is unset.
func TestEventArgs_NullsEmptyIdentities(t *testing.T) {
	args := eventArgs(metering.Event{RequestID: "r", Model: "m", PromptTokens: 3})
	if len(args) != colsPerRow {
		t.Fatalf("len(args) = %d, want %d", len(args), colsPerRow)
	}
	// auth_id is index 1 — must be nil for empty.
	if args[1] != nil {
		t.Fatalf("auth_id arg = %v, want nil for empty AuthID", args[1])
	}
	// prompt_tokens is index 8 — must be the int, not nil.
	if args[8] != 3 {
		t.Fatalf("prompt_tokens arg = %v, want 3", args[8])
	}
	// event_ts is the last index — nil when TimestampUnixMs==0.
	if args[colsPerRow-1] != nil {
		t.Fatalf("event_ts arg = %v, want nil for zero timestamp", args[colsPerRow-1])
	}
}
