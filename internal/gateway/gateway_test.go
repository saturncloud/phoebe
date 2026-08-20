package gateway

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

var resolveCols = []string{"id", "base_model", "adapter", "serving_mode", "graph_k8s_name"}

// TestPGResolver_ResolvesBaseModelRow locks the happy path and the QUERY
// CONTRACT: the lookup is keyed on org_id ($1, the tenancy boundary) and a
// byte-exact served_model_name ($2), deterministically ordered (oldest row)
// with LIMIT 1. The scan carries the row's pricing identity verbatim.
func TestPGResolver_ResolvesBaseModelRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta(resolveQuery)).
		WithArgs("org-1", "meta-llama/Llama-3.1-8B-Instruct").
		WillReturnRows(sqlmock.NewRows(resolveCols).
			AddRow("tfm-1", "meta-llama/Llama-3.1-8B-Instruct", "", "shared", "graph-llama31"))

	r, err := NewPGResolver(db).Resolve(context.Background(), "org-1", "meta-llama/Llama-3.1-8B-Instruct")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := Resolution{
		ResourceID:   "tfm-1",
		BaseModel:    "meta-llama/Llama-3.1-8B-Instruct",
		Adapter:      "",
		ServingMode:  "shared",
		GraphK8sName: "graph-llama31",
	}
	if r != want {
		t.Fatalf("Resolution = %+v, want %+v", r, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestPGResolver_ResolvesFineTuneRow: an adapter endpoint's row carries the
// checkpoint artifact id — the fine-tune premium trigger — onto the Resolution.
func TestPGResolver_ResolvesFineTuneRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta(resolveQuery)).
		WithArgs("org-1", "support-bot").
		WillReturnRows(sqlmock.NewRows(resolveCols).
			AddRow("tfm-2", "meta-llama/Llama-3.1-8B-Instruct", "ckpt-42", "shared", "graph-llama31"))

	r, err := NewPGResolver(db).Resolve(context.Background(), "org-1", "support-bot")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if r.Adapter != "ckpt-42" || r.BaseModel != "meta-llama/Llama-3.1-8B-Instruct" {
		t.Fatalf("fine-tune row: got %+v", r)
	}
}

// TestPGResolver_NoRowIsNotFound: an empty result is ErrNotFound (the proxy's
// generic 404), never a nil-Resolution success.
func TestPGResolver_NoRowIsNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta(resolveQuery)).
		WithArgs("org-1", "no-such-model").
		WillReturnRows(sqlmock.NewRows(resolveCols))

	if _, err := NewPGResolver(db).Resolve(context.Background(), "org-1", "no-such-model"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// TestPGResolver_GraphlessRowIsNotFound: a row with no graph_k8s_name is not
// addressable; it folds into ErrNotFound so the client cannot distinguish
// "doesn't exist" from "not yet servable" (no existence oracle).
func TestPGResolver_GraphlessRowIsNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta(resolveQuery)).
		WithArgs("org-1", "provisioning-bot").
		WillReturnRows(sqlmock.NewRows(resolveCols).
			AddRow("tfm-3", "meta-llama/Llama-3.1-8B-Instruct", "", "shared", ""))

	if _, err := NewPGResolver(db).Resolve(context.Background(), "org-1", "provisioning-bot"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound for graph-less row", err)
	}
}

// TestPGResolver_DBErrorIsNotNotFound: a query failure surfaces as a plain
// error, NEVER ErrNotFound — the caller must 503 (fail closed), not 404.
func TestPGResolver_DBErrorIsNotNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta(resolveQuery)).
		WithArgs("org-1", "m").
		WillReturnError(errors.New("connection refused"))

	_, rerr := NewPGResolver(db).Resolve(context.Background(), "org-1", "m")
	if rerr == nil || errors.Is(rerr, ErrNotFound) {
		t.Fatalf("err = %v, want a non-NotFound lookup error", rerr)
	}
}

// TestResolveQuery_Shape pins the load-bearing SQL fragments: the org tenancy
// filter, the byte-exact served-name match, the deterministic oldest-row pick,
// and the single-row limit.
func TestResolveQuery_Shape(t *testing.T) {
	for _, frag := range []string{
		"FROM tf_model",
		"org_id = $1",
		"served_model_name = $2",
		"ORDER BY created_at ASC, id ASC",
		"LIMIT 1",
		`COALESCE(checkpoint_artifact_id, '')`,
		`COALESCE(graph_k8s_name, '')`,
	} {
		if !strings.Contains(resolveQuery, frag) {
			t.Fatalf("resolveQuery is missing fragment %q", frag)
		}
	}
}
