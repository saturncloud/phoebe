package meteredusage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// managerStub answers with the given statuses in order (the last one repeats)
// and counts the calls. A 200 answers with a contract body for the request.
func managerStub(t *testing.T, statuses ...int) (*httptest.Server, *int32) {
	t.Helper()
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(atomic.AddInt32(&calls, 1))
		status := statuses[len(statuses)-1]
		if n <= len(statuses) {
			status = statuses[n-1]
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"stub"}`))
			return
		}
		var req Request
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &req)
		_ = json.NewEncoder(w).Encode(Response{Accepted: int64(len(req.Records)), WindowStart: req.WindowStart, Kinds: req.Kinds})
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func instantClient(url string) *Client {
	return &Client{
		BaseURL: url, Token: "tok", HTTP: &http.Client{Timeout: 5 * time.Second},
		Sleep: func(context.Context, time.Duration) error { return nil },
	}
}

func TestPost_RetriesOn5xxThenSucceeds(t *testing.T) {
	srv, calls := managerStub(t, 502, 503, 500, 200)
	resp, err := instantClient(srv.URL).Post(context.Background(), validRequest(validRecord(nil)))
	if err != nil {
		t.Fatal(err)
	}
	if *calls != 4 || resp.Accepted != 1 {
		t.Fatalf("calls=%d accepted=%d, want 4 calls and 1 accepted", *calls, resp.Accepted)
	}
}

func TestPost_NeverRetries400_401_409_422(t *testing.T) {
	for _, status := range []int{400, 401, 409, 422} {
		srv, calls := managerStub(t, status, 200)
		_, err := instantClient(srv.URL).Post(context.Background(), validRequest())
		var rej *RejectedError
		if !errors.As(err, &rej) || rej.Status != status {
			t.Fatalf("status %d: err = %v, want RejectedError", status, err)
		}
		if *calls != 1 {
			t.Fatalf("status %d: %d calls, want exactly 1 (never retried)", status, *calls)
		}
	}
}

func TestPost_GivesUpAfterMaxAttemptsOnPersistent5xx(t *testing.T) {
	srv, calls := managerStub(t, 503)
	c := instantClient(srv.URL)
	c.MaxAttempts = 3
	_, err := c.Post(context.Background(), validRequest())
	var te *TransientError
	if !errors.As(err, &te) || te.Attempts != 3 {
		t.Fatalf("err = %v, want TransientError after 3 attempts", err)
	}
	if *calls != 3 {
		t.Fatalf("%d calls, want 3", *calls)
	}
}

func TestPost_RetriesNetworkErrors(t *testing.T) {
	srv, _ := managerStub(t, 200)
	url := srv.URL
	srv.Close() // nothing listens: every attempt is a network error
	c := instantClient(url)
	c.MaxAttempts = 2
	_, err := c.Post(context.Background(), validRequest())
	var te *TransientError
	if !errors.As(err, &te) || te.Attempts != 2 {
		t.Fatalf("err = %v, want TransientError after 2 attempts", err)
	}
}

func TestPost_SendsTheInstallCustomerTokenAndJSON(t *testing.T) {
	var gotAuth, gotType, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotType, gotPath = r.Header.Get("Authorization"), r.Header.Get("Content-Type"), r.URL.Path
		_ = json.NewEncoder(w).Encode(Response{Accepted: 0, WindowStart: refWindow})
	}))
	defer srv.Close()
	c := instantClient(srv.URL + "/")
	if _, err := c.Post(context.Background(), validRequest()); err != nil {
		t.Fatal(err)
	}
	if gotAuth != "token tok" || gotType != "application/json" || gotPath != Path {
		t.Fatalf("auth=%q type=%q path=%q", gotAuth, gotType, gotPath)
	}
}

// The manager answering with a different record count means the two sides
// disagree about the wire. That is an error, and not a retryable one.
func TestPost_AcceptedCountMismatchIsAnErrorNotARetry(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		_ = json.NewEncoder(w).Encode(Response{Accepted: 7, WindowStart: refWindow})
	}))
	defer srv.Close()
	_, err := instantClient(srv.URL).Post(context.Background(), validRequest(validRecord(nil)))
	if err == nil {
		t.Fatal("want an error for accepted 7 of 1")
	}
	var te *TransientError
	if errors.As(err, &te) || calls != 1 {
		t.Fatalf("err=%v calls=%d, want a single non-transient failure", err, calls)
	}
}

func TestPost_StopsRetryingWhenTheContextEnds(t *testing.T) {
	srv, calls := managerStub(t, 503)
	c := instantClient(srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	c.Sleep = func(ctx context.Context, _ time.Duration) error { cancel(); return ctx.Err() }
	_, err := c.Post(ctx, validRequest())
	var te *TransientError
	if !errors.As(err, &te) || *calls != 1 {
		t.Fatalf("err=%v calls=%d, want one attempt then a TransientError", err, *calls)
	}
}

func TestBackoff_DoublesFromTheBaseAndIsCapped(t *testing.T) {
	c := &Client{BaseBackoff: time.Second, MaxBackoff: 5 * time.Second}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}
	for i, w := range want {
		if got := c.backoff(i + 1); got != w {
			t.Fatalf("retry %d: %v, want %v", i+1, got, w)
		}
	}
}

// 429 means "slow down", not "no": it is retried with backoff.
func TestPost_Retries429ThenSucceeds(t *testing.T) {
	srv, calls := managerStub(t, 429, 429, 200)
	if _, err := instantClient(srv.URL).Post(context.Background(), validRequest()); err != nil {
		t.Fatal(err)
	}
	if *calls != 3 {
		t.Fatalf("%d calls, want 3", *calls)
	}
}

// retryAfterStub answers 429 with the given Retry-After once, then 200.
func retryAfterStub(t *testing.T, retryAfter string) string {
	t.Helper()
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.Header().Set("Retry-After", retryAfter)
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(Response{Accepted: 0, WindowStart: refWindow})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestPost_HonoursRetryAfterWithinTheBackoffCap(t *testing.T) {
	cases := map[string]struct {
		header string
		want   time.Duration
	}{
		"seconds longer than backoff":  {"7", 7 * time.Second},
		"seconds shorter than backoff": {"0", time.Second}, // backoff wins
		"seconds beyond the cap":       {"3600", 30 * time.Second},
		"http date beyond the cap":     {time.Now().Add(2 * time.Hour).UTC().Format(http.TimeFormat), 30 * time.Second},
		"unparseable":                  {"soon", time.Second},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var slept []time.Duration
			c := instantClient(retryAfterStub(t, tc.header))
			c.BaseBackoff, c.MaxBackoff = time.Second, 30*time.Second
			c.Sleep = func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil }
			if _, err := c.Post(context.Background(), validRequest()); err != nil {
				t.Fatal(err)
			}
			if len(slept) != 1 || slept[0] != tc.want {
				t.Fatalf("waited %v, want [%v]", slept, tc.want)
			}
		})
	}
}

func TestParseRetryAfter_HTTPDate(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	if got := parseRetryAfter(now.Add(90*time.Second).Format(http.TimeFormat), now); got != 90*time.Second {
		t.Fatalf("got %v, want 90s", got)
	}
	if got := parseRetryAfter(now.Add(-time.Minute).Format(http.TimeFormat), now); got != 0 {
		t.Fatalf("date in the past: got %v, want 0", got)
	}
}
