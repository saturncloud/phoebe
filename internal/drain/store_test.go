package drain

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"

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
			ClientRequestID:  "logical-1",
			AuthID:           "auth-1",
			Model:            "m1",
			PromptTokens:     5,
			CompletionTokens: 7,
			ServingMode:      "dedicated",
			TimestampUnixMs:  ts,
			// Platform-set serving fields. A zero duration with a known start
			// is a measured value and must be stored as 0, not NULL.
			TopologyRevision:     3,
			Region:               "eu-north1",
			RequestStartedUnixMs: ts - 1500,
			ServingDurationMs:    0,
		},
		{
			RequestID: "req-2",
			// No identity fields set → must bind NULL, not "".
			Model:       "m2",
			ServingMode: "shared",
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		"INSERT INTO billing_event (request_id, client_request_id, auth_id, user_id, group_id, resource_id, resource_type, org_id, model, base_model, adapter, serving_mode, prompt_tokens, cached_tokens, completion_tokens, finish_reason, aborted, usage_found, status_code, streamed, graph_k8s_name, topology_revision, region, request_started_at, serving_duration_ms, event_ts) VALUES",
	)).
		WithArgs(
			// row 1 (org_id + base_model NULL: a pre-cutover-shaped event with no org
			// header and no derived_from)
			"req-1", "logical-1", "auth-1", nil, nil, nil, nil, nil, "m1", nil, nil, "dedicated", 5, 0, 7, nil, false, false, nil, false, nil,
			3, "eu-north1", time.UnixMilli(ts-1500).UTC(), int64(0), time.UnixMilli(ts).UTC(),
			// row 2 (no identity, no timestamp → event_ts NULL; no topology, no
			// start → all four serving fields NULL)
			"req-2", nil, nil, nil, nil, nil, nil, nil, "m2", nil, nil, "shared", 0, 0, 0, nil, false, false, nil, false, nil,
			nil, nil, nil, nil, nil,
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
// must bind model as NULL, not ” — the rater's unattributable predicate is
// `model_id IS NULL`, and a stored ” would dodge it and be misreported as
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
			"req-no-model", nil, "auth-1", nil, nil, nil, nil,
			nil,         // org_id: "" must bind NULL
			nil,         // model: "" must bind NULL
			nil,         // base_model: "" must bind NULL
			nil,         // adapter: "" must bind NULL
			"dedicated", // serving_mode: bound as decoded
			1, 0, 2, nil, false, false, nil, false,
			nil,                // graph_k8s_name: "" must bind NULL
			nil, nil, nil, nil, // topology_revision, region, request_started_at, serving_duration_ms
			nil,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = store.Upsert(context.Background(), []metering.Event{{
		RequestID:        "req-no-model",
		AuthID:           "auth-1",
		Model:            "", // upstream never reported a model
		ServingMode:      "dedicated",
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

// TestEventArgs_ZeroStatusCodeBindsNullNoResponse pins the status_code invariant
// by name: StatusCode 0 means no HTTP status was ever produced for the attempt,
// and billing_event_status_code_ck admits NULL or 100..599 — so 0 must bind NULL
// (a never-answered attempt), while a real status binds through verbatim.
func TestEventArgs_ZeroStatusCodeBindsNullNoResponse(t *testing.T) {
	const statusCodeIdx = 18 // request_id..usage_found is 18 columns; status_code is next.

	noResponse := eventArgs(metering.Event{RequestID: "r", Model: "m"})
	if noResponse[statusCodeIdx] != nil {
		t.Fatalf("status_code arg = %v, want nil for StatusCode 0 (no HTTP response produced)", noResponse[statusCodeIdx])
	}

	answered := eventArgs(metering.Event{RequestID: "r", Model: "m", StatusCode: 500})
	if answered[statusCodeIdx] != 500 {
		t.Fatalf("status_code arg = %v, want 500 (a real status must bind through)", answered[statusCodeIdx])
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
	// auth_id is index 2 (after client_request_id) — must be nil for empty.
	if args[2] != nil {
		t.Fatalf("auth_id arg = %v, want nil for empty AuthID", args[2])
	}
	// org_id is index 6 (after resource_type) — must be nil for empty (no producer
	// header / rollout gap); a stored '' would dodge the rater/push `org_id IS NULL`
	// held-not-billed predicate.
	if args[7] != nil {
		t.Fatalf("org_id arg = %v, want nil for empty OrgID", args[7])
	}
	// base_model is index 8 (org_id pushed model/base_model down by one) — nil for empty.
	if args[9] != nil {
		t.Fatalf("base_model arg = %v, want nil for empty BaseModel", args[9])
	}
	// prompt_tokens is index 11 (…base_model=8, adapter=9, serving_mode=10) — int, not nil.
	if args[12] != 3 {
		t.Fatalf("prompt_tokens arg = %v, want 3", args[12])
	}
	// event_ts is the last index — nil when TimestampUnixMs==0.
	if args[colsPerRow-1] != nil {
		t.Fatalf("event_ts arg = %v, want nil for zero timestamp", args[colsPerRow-1])
	}

	// POSITIVE bind: a non-empty OrgID must reach the org_id column verbatim (index 6),
	// not just be NULLed when empty — the meter-time org capture is the point of the
	// change, so the happy path is pinned here (not only transitively via e2e).
	withOrg := eventArgs(metering.Event{RequestID: "r", Model: "m", OrgID: "org-xyz"})
	if withOrg[7] != "org-xyz" {
		t.Fatalf("org_id arg = %v, want \"org-xyz\" (a non-empty OrgID must bind through)", withOrg[7])
	}
}

// TestEventArgs_ServingModeStoredAsDecoded pins that the store applies no
// serving-mode default of its own: the pre-cutover default happens at decode
// time for an absent key only (metering.UnmarshalEvent). An explicit "" binds
// as ” (not NULL, not 'dedicated') so the rater withholds it as an invalid
// serving mode, and every other value binds verbatim.
func TestEventArgs_ServingModeStoredAsDecoded(t *testing.T) {
	const servingModeIdx = 11 // request_id..adapter is 11 columns; serving_mode is next.

	cases := []struct {
		in   string
		want any
	}{
		{"", ""},
		{"dedicated", "dedicated"},
		{"shared", "shared"},
		{"bogus", "bogus"},
	}
	for _, tc := range cases {
		args := eventArgs(metering.Event{RequestID: "r", Model: "m", ServingMode: tc.in})
		if args[servingModeIdx] != tc.want {
			t.Fatalf("serving_mode arg for %q = %v, want %v", tc.in, args[servingModeIdx], tc.want)
		}
	}
}

// TestEventArgs_OutOfRangeTopologyRevisionBindsNullNeverPoisons:
// billing_event.topology_revision is INTEGER, so a revision above MaxInt32
// would fail the insert and the row-at-a-time fallback would drop the
// billable event as poison. It binds NULL instead; an in-range value binds.
func TestEventArgs_OutOfRangeTopologyRevisionBindsNullNeverPoisons(t *testing.T) {
	const topologyRevisionIdx = 21 // status_code 18, streamed 19, graph_k8s_name 20.

	inRange := eventArgs(metering.Event{RequestID: "r", Model: "m", TopologyRevision: 2147483647})
	if inRange[topologyRevisionIdx] != 2147483647 {
		t.Fatalf("topology_revision arg = %v, want 2147483647", inRange[topologyRevisionIdx])
	}
	for _, rev := range []int{2147483648, -1, 0} {
		args := eventArgs(metering.Event{RequestID: "r", Model: "m", TopologyRevision: rev})
		if args[topologyRevisionIdx] != nil {
			t.Fatalf("topology_revision arg for %d = %v, want nil", rev, args[topologyRevisionIdx])
		}
	}
}

// TestEventArgs_NegativeServingDurationStoresNull: a negative serving time is
// impossible evidence, so it is stored as NULL ("not captured") rather than
// as a misleading value; zero (a sub-millisecond request) is kept.
func TestEventArgs_NegativeServingDurationStoresNull(t *testing.T) {
	const servingDurationIdx = 24 // topology_revision 21, region 22, request_started_at 23.

	for _, tc := range []struct {
		ms   int64
		want any
	}{{0, int64(0)}, {42, int64(42)}, {-1, nil}} {
		args := eventArgs(metering.Event{RequestID: "r", Model: "m", RequestStartedUnixMs: 1, ServingDurationMs: tc.ms})
		if args[servingDurationIdx] != tc.want {
			t.Fatalf("serving_duration_ms arg for %d = %v, want %v", tc.ms, args[servingDurationIdx], tc.want)
		}
	}
}

// TestEventArgs_UnstorableRegionBindsNullNeverPoisons: billing_event.region
// is VARCHAR(64) text, so a region with a NUL byte, invalid UTF-8, or more
// than 64 characters would fail the multi-row insert and drop the whole
// batch. It binds NULL instead; a normal region binds as the string.
func TestEventArgs_UnstorableRegionBindsNullNeverPoisons(t *testing.T) {
	const regionIdx = 22 // topology_revision 21, request_started_at 23.

	cases := []struct {
		name   string
		region string
		want   any
	}{
		{"empty is not captured", "", nil},
		{"short region kept", "eu-north1", "eu-north1"},
		{"exactly max ASCII characters kept", strings.Repeat("a", metering.MaxRegionChars), strings.Repeat("a", metering.MaxRegionChars)},
		{"exactly max multibyte characters kept", strings.Repeat("é", metering.MaxRegionChars), strings.Repeat("é", metering.MaxRegionChars)},
		{"one over max is NULL", strings.Repeat("a", metering.MaxRegionChars+1), nil},
		{"invalid UTF-8 is NULL", "\xff", nil},
		{"NUL byte is NULL", "a\x00b", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := eventArgs(metering.Event{RequestID: "r", Model: "m", Region: tc.region})
			if args[regionIdx] != tc.want {
				t.Fatalf("region arg for %q = %v, want %v", tc.region, args[regionIdx], tc.want)
			}
		})
	}
}

// schemaVersionStore returns a PostgresStore over sqlmock expecting the
// schema_migrations probe once.
func schemaVersionStore(t *testing.T) (*PostgresStore, sqlmock.Sqlmock, *sqlmock.ExpectedQuery) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	q := mock.ExpectQuery(regexp.QuoteMeta("SELECT version, dirty FROM schema_migrations LIMIT 1"))
	return NewPostgresStore(db), mock, q
}

func TestPostgresStore_SchemaVersionReadsRow(t *testing.T) {
	for _, c := range []struct {
		version int64
		dirty   bool
	}{{9, false}, {12, true}} {
		store, mock, q := schemaVersionStore(t)
		q.WillReturnRows(sqlmock.NewRows([]string{"version", "dirty"}).AddRow(c.version, c.dirty))
		v, dirty, err := store.SchemaVersion(context.Background())
		if err != nil || v != uint(c.version) || dirty != c.dirty {
			t.Fatalf("SchemaVersion = (%d, %v, %v), want (%d, %v, nil)", v, dirty, err, c.version, c.dirty)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}

// No migration ever applied (no row, or no table) reads as version 0, clean:
// the gate stays closed for any RequiredSchemaVersion > 0.
func TestPostgresStore_SchemaVersionNeverMigratedIsZero(t *testing.T) {
	for name, set := range map[string]func(*sqlmock.ExpectedQuery){
		"no rows":         func(q *sqlmock.ExpectedQuery) { q.WillReturnRows(sqlmock.NewRows([]string{"version", "dirty"})) },
		"undefined table": func(q *sqlmock.ExpectedQuery) { q.WillReturnError(&pgconn.PgError{Code: "42P01"}) },
	} {
		store, mock, q := schemaVersionStore(t)
		set(q)
		v, dirty, err := store.SchemaVersion(context.Background())
		if err != nil || v != 0 || dirty {
			t.Fatalf("%s: SchemaVersion = (%d, %v, %v), want (0, false, nil)", name, v, dirty, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}

// Any other failure, including a schema_migrations table of an unexpected
// shape (undefined_column) or an invalid reference, is an error the wait log
// shows, never a silent "version 0".
func TestPostgresStore_SchemaVersionErrorIsReported(t *testing.T) {
	boom := errors.New("boom")
	for name, qerr := range map[string]error{
		"generic":          boom,
		"undefined column": &pgconn.PgError{Code: "42703"},
		"invalid column":   &pgconn.PgError{Code: "42P10"},
	} {
		store, mock, q := schemaVersionStore(t)
		q.WillReturnError(qerr)
		v, _, err := store.SchemaVersion(context.Background())
		if err == nil || v != 0 {
			t.Fatalf("%s: SchemaVersion = (%d, %v), want version 0 and an error", name, v, err)
		}
		if !errors.Is(err, qerr) {
			t.Fatalf("%s: error %v does not wrap the query error", name, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPostgresStore_SchemaVersionClampsNegativeToZero(t *testing.T) {
	store, mock, q := schemaVersionStore(t)
	q.WillReturnRows(sqlmock.NewRows([]string{"version", "dirty"}).AddRow(int64(-1), false))
	v, dirty, err := store.SchemaVersion(context.Background())
	if err != nil || v != 0 || dirty {
		t.Fatalf("SchemaVersion = (%d, %v, %v), want (0, false, nil)", v, dirty, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
