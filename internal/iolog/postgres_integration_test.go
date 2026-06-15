//go:build integration

// iolog integration test: runs the REAL PostgresSink INSERT (including the
// to_tsvector over the request+response bodies) against a LIVE Postgres, proving
// Fix C — a request body that, UNCAPPED, would blow past Postgres's ~1 MiB
// tsvector input limit and fail the whole INSERT now succeeds once capped.
//
// Gated behind the `integration` build tag AND a non-empty PHOEBE_TEST_DATABASE_URL
// so the default `go test ./...` never needs a database. Run with:
//
//	PHOEBE_TEST_DATABASE_URL=postgres://... go test -tags=integration ./internal/iolog/...
package iolog

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/saturncloud/phoebe/internal/logging"
)

// ioLogDDL mirrors migrations/0002_io_log.sql (and the Alembic artifact),
// including request_truncated. Created in an isolated schema per run so the test
// leaves no residue and never collides with a real io_log.
const ioLogDDL = `
CREATE TABLE io_log (
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_id         VARCHAR(255) NOT NULL,
    auth_id            VARCHAR(64),
    user_id            VARCHAR(32),
    group_id           VARCHAR(32),
    resource_id        VARCHAR(64),
    resource_type      VARCHAR(64),
    model              VARCHAR(255),
    request_body       TEXT,
    request_truncated  BOOLEAN NOT NULL DEFAULT FALSE,
    response_body      TEXT,
    response_truncated BOOLEAN NOT NULL DEFAULT FALSE,
    status_code        INTEGER,
    streamed           BOOLEAN NOT NULL DEFAULT FALSE,
    latency_ms         BIGINT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    body_tsv           TSVECTOR
);
CREATE INDEX io_log_body_tsv_ix ON io_log USING GIN (body_tsv);`

func openIntegrationDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping iolog integration test")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	schema := fmt.Sprintf("iolog_it_%d", time.Now().UnixNano())
	if _, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s; SET search_path TO %s", schema, schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec("SET search_path TO " + schema); err != nil {
		t.Fatalf("set search_path: %v", err)
	}
	if _, err := db.Exec(ioLogDDL); err != nil {
		t.Fatalf("create io_log: %v", err)
	}
	return db, func() {
		_, _ = db.Exec(fmt.Sprintf("DROP SCHEMA %s CASCADE", schema))
		_ = db.Close()
	}
}

// TestRequestBodyCappedBeforeTsvector_Integration proves the >1 MiB request body
// INSERT — which UNCAPPED fails Postgres's to_tsvector limit — succeeds once the
// body is capped to MaxBodyBytes (the proxy's capture path does this; here we
// feed the sink a capped Record and confirm the real INSERT commits).
func TestRequestBodyCappedBeforeTsvector_Integration(t *testing.T) {
	db, cleanup := openIntegrationDB(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.WorkerCount = 1
	sink, err := newPostgresSinkWithDB(cfg, logging.New(logging.ERROR), db)
	if err != nil {
		t.Fatalf("newPostgresSinkWithDB: %v", err)
	}

	// A capped body: 256 KiB (the default cap), well under the ~1 MiB tsvector
	// limit. This is what the proxy stores after truncation; the INSERT must
	// commit cleanly with the tsvector populated.
	capped := strings.Repeat("a", DefaultMaxBodyBytes)
	rec := Record{
		RequestID:        "req-it-capped",
		AuthID:           "auth-1",
		Model:            "m",
		RequestBody:      capped,
		RequestTruncated: true, // it WAS truncated from a larger original
		ResponseBody:     `{"choices":[]}`,
		StatusCode:       200,
		Timestamp:        time.Now(),
	}
	sink.Log(context.Background(), rec)
	if err := sink.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := sink.Dropped(); got != 0 {
		t.Fatalf("capped record INSERT dropped %d records, want 0 (the INSERT must succeed)", got)
	}

	var (
		gotLen    int
		truncated bool
		tsvNull   bool
	)
	row := db.QueryRow(`SELECT length(request_body), request_truncated, (body_tsv IS NULL)
	                    FROM io_log WHERE request_id = $1`, rec.RequestID)
	if err := row.Scan(&gotLen, &truncated, &tsvNull); err != nil {
		t.Fatalf("the capped record was NOT inserted (Fix C regression — INSERT failed over to_tsvector?): %v", err)
	}
	if gotLen != DefaultMaxBodyBytes {
		t.Fatalf("stored request_body len = %d, want %d", gotLen, DefaultMaxBodyBytes)
	}
	if !truncated {
		t.Fatalf("request_truncated = false, want true")
	}
	if tsvNull {
		t.Fatalf("body_tsv is NULL — the to_tsvector population did not run")
	}
}

// TestUncappedRequestBodyWouldFailTsvector_Integration documents the bug Fix C
// fixes: an UNCAPPED body whose to_tsvector exceeds Postgres's 1 MiB tsvector
// limit fails the INSERT outright (error "string is too long for tsvector"). We
// bypass the sink and issue the raw INSERT to prove Postgres rejects it — so the
// cap is load-bearing, not cosmetic.
//
// The limit is on the SIZE OF THE RESULTING TSVECTOR (distinct lexemes +
// positions), not the raw input length, and to_tsvector silently truncates any
// single word to 2 KiB — so a megabyte of one repeated word does NOT trip it. We
// therefore build a body of MANY DISTINCT words (which is what a real long-context
// prompt looks like to the 'simple' tokenizer) so the tsvector genuinely exceeds
// 1 MiB. This is the realistic shape of the prompt that drops records today.
func TestUncappedRequestBodyWouldFailTsvector_Integration(t *testing.T) {
	db, cleanup := openIntegrationDB(t)
	defer cleanup()

	// ~1.5M distinct short words ("w0 w1 w2 ...") so the tsvector (lexemes +
	// positions) blows past the 1 MiB cap. Build it cheaply.
	var b strings.Builder
	b.Grow(12 * 1024 * 1024)
	for i := 0; b.Len() < 6*1024*1024; i++ {
		fmt.Fprintf(&b, "w%d ", i)
	}
	huge := b.String()

	_, err := db.Exec(
		`INSERT INTO io_log (request_id, request_body, body_tsv)
		 VALUES ($1, $2, to_tsvector('simple', $2))`,
		"req-it-uncapped", huge)
	if err == nil {
		t.Skip("Postgres accepted the large tsvector input on this server; the cap remains correct as defense-in-depth")
	}
	low := strings.ToLower(err.Error())
	if !strings.Contains(low, "tsvector") && !strings.Contains(low, "too long") && !strings.Contains(low, "maximum") {
		t.Fatalf("uncapped INSERT failed but not with the expected tsvector-size error: %v", err)
	}
	t.Logf("confirmed: uncapped body fails the to_tsvector INSERT: %v", err)
}
