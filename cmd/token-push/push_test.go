package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
	return &pusher{db: db, log: quietLog(), timeout: 5 * time.Second}, mock
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
	if snap.WindowStart != "2026-06-16T14:00:00Z" {
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
	// crude but exact: the empty-array form, not the null form.
	return contains(s, `"rollups":[]`) && !contains(s, `"rollups":null`)
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
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

	p := &pusher{log: quietLog(), managerURL: srv.URL, token: "tok-123", timeout: 5 * time.Second}
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
	if !contains(gotBody, `"rated_usage_id":"x"`) || !contains(gotBody, `"cost":"0.5"`) {
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

	p := &pusher{log: quietLog(), managerURL: srv.URL, token: "t", timeout: 5 * time.Second}
	err := p.postSnapshot(context.Background(), snapshot{WindowStart: "2026-06-16T14:00:00Z", Rollups: []rollup{}})
	if err == nil {
		t.Fatalf("expected an error on a 400, got nil")
	}
}

// TestPushWindows_ExitUnattribOnOrphan (token-push-exit-2): when a window pushes
// successfully but contained an unattributable row, the run returns exitUnattrib(2),
// not exitOK.
func TestPushWindows_ExitUnattribOnOrphan(t *testing.T) {
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
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(rows)

	code := p.pushWindows(context.Background(), []time.Time{win})
	if code != exitUnattrib {
		t.Fatalf("exit code = %d, want exitUnattrib(%d)", code, exitUnattrib)
	}
	if hits != 1 {
		t.Fatalf("manager hit %d times, want 1 (the attributable rows still push)", hits)
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
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).
		WillReturnRows(sqlmock.NewRows(snapshotCols).
			AddRow("ok", "res-1", "m", "org-acme", "0.001", "0.1", "0.05", "0.6", int64(1), int64(0), int64(1), int64(1)))

	code := p.pushWindows(context.Background(), []time.Time{win})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want exitFatal(%d)", code, exitFatal)
	}
}
