package pricefetch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const okBody = "version: 1\n"

func serve(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	return s
}

// TestFetch_AsOfIsSentAsRFC3339UTC pins the wire contract for historical prices:
// a non-zero asOf must reach the manager as ?at=<RFC3339 in UTC>. This is what
// lets the rater price each hour at the rates in force during it.
func TestFetch_AsOfIsSentAsRFC3339UTC(t *testing.T) {
	var gotQuery, gotPath, gotAuth string
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery, gotAuth = r.URL.Path, r.URL.Query().Get("at"), r.Header.Get("Authorization")
		w.Header().Set(VersionHeader, "v-abc")
		_, _ = w.Write([]byte(okBody))
	})

	// A non-UTC instant must be normalised to UTC on the wire, so the same moment
	// is never expressed two ways.
	zone := time.FixedZone("UTC+5", 5*60*60)
	asOf := time.Date(2026, 6, 8, 15, 0, 0, 0, zone) // == 10:00Z

	body, version, err := Client{ManagerURL: srv.URL, Token: "tok"}.Fetch(context.Background(), asOf)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if gotPath != TokenPricesPath {
		t.Fatalf("path = %q, want %q", gotPath, TokenPricesPath)
	}
	if gotQuery != "2026-06-08T10:00:00Z" {
		t.Fatalf("at = %q, want the instant in UTC (2026-06-08T10:00:00Z)", gotQuery)
	}
	if gotAuth != "token tok" {
		t.Fatalf("Authorization = %q, want the customer-token scheme", gotAuth)
	}
	if string(body) != okBody || version != "v-abc" {
		t.Fatalf("body/version = %q/%q", body, version)
	}
}

// TestFetch_ZeroAsOfSendsNoAtParameter: the periodic sync wants CURRENT prices, and
// must not pin itself to an instant.
func TestFetch_ZeroAsOfSendsNoAtParameter(t *testing.T) {
	var hadAt bool
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, hadAt = r.URL.Query()["at"]
		w.Header().Set(VersionHeader, "v1")
		_, _ = w.Write([]byte(okBody))
	})
	if _, _, err := (Client{ManagerURL: srv.URL, Token: "tok"}).Fetch(context.Background(), time.Time{}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if hadAt {
		t.Fatal("a zero asOf must send no ?at= (it means 'current prices')")
	}
}

func TestFetch_FailsClosed(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		wantErr string
	}{
		{
			name: "non-200 is refused",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(VersionHeader, "v1")
				w.WriteHeader(http.StatusServiceUnavailable)
			},
			wantErr: "status 503",
		},
		{
			// The manager always sets it; its absence means a proxy stripped it or
			// the wrong endpoint answered. An unversioned book cannot be attributed
			// in a billing reconcile.
			name: "200 without the version header is refused",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(okBody))
			},
			wantErr: "missing the " + VersionHeader,
		},
		{
			// A truncated valid-YAML prefix would be a partial price book.
			name: "over-cap body is refused, never truncated",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(VersionHeader, "v1")
				_, _ = w.Write([]byte(strings.Repeat("x", MaxBodyBytes+1)))
			},
			wantErr: "exceeds",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := serve(t, tc.handler)
			_, _, err := Client{ManagerURL: srv.URL, Token: "tok"}.Fetch(context.Background(), time.Time{})
			if err == nil {
				t.Fatal("expected a refusal")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

// TestFetch_RequiresURLAndToken: misconfiguration is an error, never a silent
// unauthenticated or empty-URL request.
func TestFetch_RequiresURLAndToken(t *testing.T) {
	if _, _, err := (Client{Token: "tok"}).Fetch(context.Background(), time.Time{}); err == nil {
		t.Fatal("an empty manager URL must be refused")
	}
	if _, _, err := (Client{ManagerURL: "http://example"}).Fetch(context.Background(), time.Time{}); err == nil {
		t.Fatal("an empty token must be refused")
	}
}
