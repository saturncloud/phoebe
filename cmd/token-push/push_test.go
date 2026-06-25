package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/saturncloud/phoebe/internal/logging"
)

func quietLog() *logging.Logger { return logging.New(logging.ERROR) }

// snapshotCols is the column set snapshotQuery returns, in order.
var snapshotCols = []string{
	"id", "resource_id", "model_id", "org_id",
	"cost", "applied_prompt_rate", "applied_cached_rate", "applied_completion_rate",
	"prompt_tokens", "cached_tokens", "completion_tokens", "billable_prompt_tokens",
}

func newMockPusher(t *testing.T) (*pusher, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &pusher{db: db, log: quietLog(), client: &http.Client{Timeout: 5 * time.Second}}, mock
}

var win = time.Date(2026, 6, 16, 14, 0, 0, 0, time.UTC)

// TestBuildSnapshot_ResolvesOrgAndShape (token-push-snapshot-shape): a resolved row
// becomes a wire rollup with org_id attached, money carried as exact-decimal strings,
// and rev=0.
func TestBuildSnapshot_ResolvesOrgAndShape(t *testing.T) {
	p, mock := newMockPusher(t)
	rows := sqlmock.NewRows(snapshotCols).AddRow(
		"rated-id-1", "res-deploy-1", "meta-llama/Llama-3.1-8B-Instruct", "org-acme",
		"0.001234500", "0.000000150", "0.000000075", "0.000000600",
		int64(8000), int64(2000), int64(500), int64(6000),
	)
	mock.ExpectQuery(`SELECT.*FROM rated_usage ru.*LEFT JOIN resource_name`).
		WithArgs(win).WillReturnRows(rows)

	snap, unattributable, err := p.buildSnapshot(context.Background(), win)
	if err != nil {
		t.Fatalf("buildSnapshot: %v", err)
	}
	if unattributable != 0 {
		t.Fatalf("unattributable = %d, want 0", unattributable)
	}
	// Explicit +00:00 offset, not "Z" — Python 3.10 fromisoformat rejects "Z".
	if snap.WindowStart != "2026-06-16T14:00:00+00:00" {
		t.Fatalf("window_start = %q", snap.WindowStart)
	}
	if len(snap.Rollups) != 1 {
		t.Fatalf("rollups = %d, want 1", len(snap.Rollups))
	}
	r := snap.Rollups[0]
	if r.OrgID != "org-acme" || r.RatedUsageID != "rated-id-1" || r.Rev != 0 {
		t.Fatalf("rollup identity wrong: %+v", r)
	}
	// Money MUST be the exact strings from the DB (C8) — never reparsed through a float.
	if r.Cost != "0.001234500" || r.AppliedPromptRate != "0.000000150" ||
		r.AppliedCachedRate != "0.000000075" || r.AppliedCompletionRate != "0.000000600" {
		t.Fatalf("money fields altered: %+v", r)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestBuildSnapshot_UnattributableOmitted (token-push-unattributable-omitted): a row
// whose resource_id resolves to NO org (NULL org_id from the LEFT JOIN) is OMITTED from
// the snapshot and counted — never pushed with a guessed/empty org (C2/C7).
func TestBuildSnapshot_UnattributableOmitted(t *testing.T) {
	p, mock := newMockPusher(t)
	rows := sqlmock.NewRows(snapshotCols).
		AddRow("rated-ok", "res-1", "m", "org-acme",
									"0.001", "0.1", "0.05", "0.6", int64(1), int64(0), int64(1), int64(1)).
		AddRow("rated-orphan", "res-vanished", "m", nil, // NULL org_id
			"0.002", "0.1", "0.05", "0.6", int64(1), int64(0), int64(1), int64(1))
	mock.ExpectQuery(`FROM rated_usage ru.*LEFT JOIN resource_name`).
		WithArgs(win).WillReturnRows(rows)

	snap, unattributable, err := p.buildSnapshot(context.Background(), win)
	if err != nil {
		t.Fatalf("buildSnapshot: %v", err)
	}
	if unattributable != 1 {
		t.Fatalf("unattributable = %d, want 1", unattributable)
	}
	if len(snap.Rollups) != 1 || snap.Rollups[0].RatedUsageID != "rated-ok" {
		t.Fatalf("orphan row was not omitted: %+v", snap.Rollups)
	}
}

// TestBuildSnapshot_EmptyWindowMarshalsToEmptyArray (token-push-empty-window-is-delete-all):
// a window with no rows produces "rollups": [] (a delete-all signal), NEVER null — the
// manager needs the empty snapshot to reconcile deletes.
func TestBuildSnapshot_EmptyWindowMarshalsToEmptyArray(t *testing.T) {
	p, mock := newMockPusher(t)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).
		WillReturnRows(sqlmock.NewRows(snapshotCols))

	snap, _, err := p.buildSnapshot(context.Background(), win)
	if err != nil {
		t.Fatalf("buildSnapshot: %v", err)
	}
	b, _ := json.Marshal(snap)
	// Must contain the empty array, not null.
	if got := string(b); !containsJSONEmptyRollups(got) {
		t.Fatalf("empty window did not marshal to an empty rollups array: %s", got)
	}
}

func containsJSONEmptyRollups(s string) bool {
	// the empty-array form, not the null form.
	return strings.Contains(s, `"rollups":[]`) && !strings.Contains(s, `"rollups":null`)
}

// expectLiveness queues the rated_usage liveness pre-check (pushWindows runs it before
// any window). hasRows=true → the table is non-empty (push proceeds).
func expectLiveness(mock sqlmock.Sqlmock, hasRows bool) {
	q := mock.ExpectQuery(`SELECT 1 FROM rated_usage LIMIT 1`)
	if hasRows {
		q.WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	} else {
		q.WillReturnError(sql.ErrNoRows)
	}
}

// TestPostSnapshot_SendsAuthAndBody (token-push-post-auth-body): the POST carries the
// auth token and the JSON snapshot to the right path.
func TestPostSnapshot_SendsAuthAndBody(t *testing.T) {
	var gotAuth, gotPath, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := &pusher{log: quietLog(), managerURL: srv.URL, token: "tok-123", client: &http.Client{Timeout: 5 * time.Second}}
	snap := snapshot{WindowStart: "2026-06-16T14:00:00Z", Rollups: []rollup{{RatedUsageID: "x", OrgID: "o", Cost: "0.5"}}}
	if err := p.postSnapshot(context.Background(), snap); err != nil {
		t.Fatalf("postSnapshot: %v", err)
	}
	if gotAuth != "token tok-123" {
		t.Fatalf("auth = %q, want 'token tok-123'", gotAuth)
	}
	if gotPath != tokenUsagePath {
		t.Fatalf("path = %q, want %q", gotPath, tokenUsagePath)
	}
	if !strings.Contains(gotBody, `"rated_usage_id":"x"`) || !strings.Contains(gotBody, `"cost":"0.5"`) {
		t.Fatalf("body missing expected fields: %s", gotBody)
	}
}

// TestPostSnapshot_Non2xxFails (token-push-non2xx-fails): a rejected push is an error,
// never treated as a successful push.
func TestPostSnapshot_Non2xxFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("malformed snapshot"))
	}))
	defer srv.Close()

	p := &pusher{log: quietLog(), managerURL: srv.URL, token: "t", client: &http.Client{Timeout: 5 * time.Second}}
	err := p.postSnapshot(context.Background(), snapshot{WindowStart: "2026-06-16T14:00:00Z", Rollups: []rollup{}})
	if err == nil {
		t.Fatalf("expected an error on a 400, got nil")
	}
}

// TestPushWindows_WithholdsWindowWithUnattributable (token-push-withhold-on-orphan):
// a window containing ANY unattributable row is NOT pushed — pushing the omitted
// snapshot would delete-by-absence a possibly-billed rollup. The manager is NEVER hit
// for that window, and the run exits 2 (held, not pushed). This is the money-loss fix:
// the OLD behavior pushed the window (hits==1), un-billing the orphan's prior charge.
func TestPushWindows_WithholdsWindowWithUnattributable(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p, mock := newMockPusher(t)
	p.managerURL = srv.URL
	p.token = "t"
	rows := sqlmock.NewRows(snapshotCols).
		AddRow("ok", "res-1", "m", "org-acme", "0.001", "0.1", "0.05", "0.6", int64(1), int64(0), int64(1), int64(1)).
		AddRow("orphan", "res-x", "m", nil, "0.002", "0.1", "0.05", "0.6", int64(1), int64(0), int64(1), int64(1))
	expectLiveness(mock, true)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(rows)

	code := p.pushWindows(context.Background(), []time.Time{win})
	if code != exitUnattrib {
		t.Fatalf("exit code = %d, want exitUnattrib(%d)", code, exitUnattrib)
	}
	if hits != 0 {
		t.Fatalf("manager was hit %d times for a window with an unattributable row; it must be WITHHELD (a push would delete-by-absence the orphan's prior charge)", hits)
	}
}

// TestPushWindows_WithheldWindowDoesNotBlockCleanWindow (token-push-windows-independent):
// a withheld (or failed) window does not abort the run — a clean window still pushes.
// This is the non-atomic-reconciliation fix: one bad hour can't block reconvergence of
// every other hour.
func TestPushWindows_WithheldWindowDoesNotBlockCleanWindow(t *testing.T) {
	pushed := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var s snapshot
		_ = json.Unmarshal(b, &s)
		pushed[s.WindowStart] = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p, mock := newMockPusher(t)
	p.managerURL = srv.URL
	p.token = "t"

	win2 := win.Add(time.Hour)
	// win: has an orphan -> withheld. win2: clean -> pushed.
	expectLiveness(mock, true)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(
		sqlmock.NewRows(snapshotCols).
			AddRow("orphan", "res-x", "m", nil, "0.002", "0.1", "0.05", "0.6", int64(1), int64(0), int64(1), int64(1)))
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win2).WillReturnRows(
		sqlmock.NewRows(snapshotCols).
			AddRow("ok", "res-1", "m", "org-acme", "0.001", "0.1", "0.05", "0.6", int64(1), int64(0), int64(1), int64(1)))

	code := p.pushWindows(context.Background(), []time.Time{win, win2})
	if code != exitUnattrib {
		t.Fatalf("exit code = %d, want exitUnattrib(%d)", code, exitUnattrib)
	}
	// Keys are the snapshot's WindowStart, formatted with the +00:00 offset (not "Z").
	const fmtOffset = "2006-01-02T15:04:05+00:00"
	if pushed[win.UTC().Format(fmtOffset)] {
		t.Fatalf("the orphan window was pushed; it must be withheld")
	}
	if !pushed[win2.UTC().Format(fmtOffset)] {
		t.Fatalf("the clean window was NOT pushed; a withheld earlier window must not block it")
	}
}

// TestPushWindows_FatalOnPostError (token-push-fatal-on-post-error): a failed push
// returns exitFatal.
func TestPushWindows_FatalOnPostError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	p, mock := newMockPusher(t)
	p.managerURL = srv.URL
	p.token = "t"
	expectLiveness(mock, true)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).
		WillReturnRows(sqlmock.NewRows(snapshotCols).
			AddRow("ok", "res-1", "m", "org-acme", "0.001", "0.1", "0.05", "0.6", int64(1), int64(0), int64(1), int64(1)))

	code := p.pushWindows(context.Background(), []time.Time{win})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want exitFatal(%d)", code, exitFatal)
	}
}

// TestPushWindows_EmptyTableRefuses (token-push-empty-table-refuses): an entirely empty
// rated_usage table (fresh install / DB wipe) makes the liveness guard refuse to push —
// otherwise every window would signal delete-all and wipe the manager's prior billing.
func TestPushWindows_EmptyTableRefuses(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p, mock := newMockPusher(t)
	p.managerURL = srv.URL
	p.token = "t"
	expectLiveness(mock, false) // empty table

	code := p.pushWindows(context.Background(), []time.Time{win, win.Add(time.Hour)})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want exitFatal(%d) on an empty table", code, exitFatal)
	}
	if hits != 0 {
		t.Fatalf("manager was hit %d times on an empty table; must refuse to push (delete-all hazard)", hits)
	}
}

// TestPushWindows_FatalDominatesWithheld (token-push-fatal-dominates): when one window
// is withheld (unattributable) AND another fails to push, the run exits FATAL (1), not 2
// — a broken job is the more urgent signal.
func TestPushWindows_FatalDominatesWithheld(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError) // every push fails
	}))
	defer srv.Close()

	p, mock := newMockPusher(t)
	p.managerURL = srv.URL
	p.token = "t"
	win2 := win.Add(time.Hour)
	expectLiveness(mock, true)
	// win: orphan -> withheld (no POST). win2: clean -> POST fails (fatal).
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(
		sqlmock.NewRows(snapshotCols).
			AddRow("orphan", "res-x", "m", nil, "0.002", "0.1", "0.05", "0.6", int64(1), int64(0), int64(1), int64(1)))
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win2).WillReturnRows(
		sqlmock.NewRows(snapshotCols).
			AddRow("ok", "res-1", "m", "org-acme", "0.001", "0.1", "0.05", "0.6", int64(1), int64(0), int64(1), int64(1)))

	code := p.pushWindows(context.Background(), []time.Time{win, win2})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want exitFatal(%d) — fatal must dominate withheld", code, exitFatal)
	}
}

// TestPostSnapshot_TrimsTrailingSlash (token-push-url-trailing-slash): a managerURL with
// a trailing slash does not produce a double-slash path that could 404/redirect.
func TestPostSnapshot_TrimsTrailingSlash(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := &pusher{log: quietLog(), managerURL: srv.URL + "/", token: "t", client: &http.Client{Timeout: 5 * time.Second}}
	if err := p.postSnapshot(context.Background(), snapshot{WindowStart: "2026-06-16T14:00:00+00:00", Rollups: []rollup{}}); err != nil {
		t.Fatalf("postSnapshot: %v", err)
	}
	if gotPath != tokenUsagePath {
		t.Fatalf("path = %q, want %q (trailing slash not trimmed → double-slash)", gotPath, tokenUsagePath)
	}
}
