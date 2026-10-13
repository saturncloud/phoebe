package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/logging"
)

// oslRecordingBackend serves every request and records the Dynamo output
// hint (nvext.agent_hints.osl) phoebe forwarded, which equals the output
// reservation admission charged.
type oslRecordingBackend struct {
	mu   sync.Mutex
	osl  []int64
	hits int
}

func (b *oslRecordingBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var body struct {
		Nvext struct {
			AgentHints struct {
				Osl int64 `json:"osl"`
			} `json:"agent_hints"`
		} `json:"nvext"`
	}
	_ = json.Unmarshal(raw, &body)
	b.mu.Lock()
	b.hits++
	b.osl = append(b.osl, body.Nvext.AgentHints.Osl)
	b.mu.Unlock()
	_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
}

func (b *oslRecordingBackend) snapshot() (int, []int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.hits, append([]int64(nil), b.osl...)
}

// chatBody is a chat body whose prompt estimate is controlled by contentChars;
// maxTokens <= 0 leaves max_tokens out.
func chatBody(contentChars int, maxTokens int64) []byte {
	mt := ""
	if maxTokens > 0 {
		mt = `"max_tokens":` + strconv.FormatInt(maxTokens, 10) + `,`
	}
	return []byte(`{"model":"model-a",` + mt + `"messages":[{"role":"user","content":"` +
		strings.Repeat("x", contentChars) + `"}]}`)
}

func kvTestServer(t *testing.T, cfg config.AdmissionSettings) *Server {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithAdmitter(admission.New(client, cfg))
}

func kvTestConfig() config.AdmissionSettings {
	return config.AdmissionSettings{Enabled: true, KeyPrefix: "kv-undeclared", LeaseTTL: time.Minute,
		DefaultMaxOutputTokens: 4096}
}

func serveChat(s *Server, up *url.URL, body []byte) *httptest.ResponseRecorder {
	req := sharedRequest(up)
	replaceRequestBody(req, body)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	return rr
}

// A request that declares no max_tokens reserves the default output only as
// far as the tightest reserved-KV cap allows: with a lane share of 3000 (1500
// times weight 2) and a 200-token prompt it is admitted with a reservation of
// 2800, and the Dynamo hint equals that reservation. Declaring
// max_tokens=4096 explicitly still gets the non-retryable kv_exceeds_limit 413.
func TestUndeclaredOutputReservationFitsLaneKVShare(t *testing.T) {
	be := &oslRecordingBackend{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)

	body := chatBody(4*200, 0)
	if est := estimatePromptTokens(body); est != 200 {
		t.Fatalf("precondition: estimate=%d, want 200", est)
	}
	cfg := kvTestConfig()
	cfg.Lanes = map[string]config.AdmissionLane{"default": {Weight: 2, Limits: config.AdmissionLimits{MaxReservedKVTokens: ptr64(1500)}}}
	s := kvTestServer(t, cfg)

	if rr := serveChat(s, up, body); rr.Code != http.StatusOK {
		t.Fatalf("undeclared request status=%d body=%q, want 200", rr.Code, rr.Body.String())
	}
	if hits, osl := be.snapshot(); hits != 1 || len(osl) != 1 || osl[0] != 2800 {
		t.Fatalf("hits=%d osl=%v, want one request reserving 2800 output tokens", hits, osl)
	}

	rr := serveChat(s, up, chatBody(4*200, 4096))
	if rr.Code != http.StatusRequestEntityTooLarge || rr.Header().Get("Retry-After") != "" {
		t.Fatalf("declared max_tokens=4096 status=%d retry-after=%q, want 413 without Retry-After", rr.Code, rr.Header().Get("Retry-After"))
	}
	if !strings.Contains(rr.Body.String(), "KV-cache") {
		t.Fatalf("declared over-KV body=%q, want the KV-cache message", rr.Body.String())
	}
	if hits, _ := be.snapshot(); hits != 1 {
		t.Fatalf("upstream hits=%d, the declared over-KV request must not reach the engine", hits)
	}
}

// The same clamp applies to the KV capacity Atlas publishes for the graph:
// with admission_max_reserved_kv_tokens = prompt + 3500 an undeclared request
// reserves 3500 (and hints 3500); the same body with max_tokens=4096 is
// refused 413 kv_exceeds_limit; an embeddings request keeps its one-token
// reservation and is unaffected.
func TestUndeclaredOutputReservationFitsPublishedGraphKV(t *testing.T) {
	be := &oslRecordingBackend{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)

	body := chatBody(4*2500, 0)
	est := estimatePromptTokens(body)
	cfg := kvTestConfig()
	cfg.Enabled = false
	s := kvTestServer(t, cfg)
	s.graphCapacity = staticGraphCapacity{graphFromUpstreamHost(up.Host): {MaxReservedKVTokens: ptr64(est + 3500)}}

	if rr := serveChat(s, up, body); rr.Code != http.StatusOK {
		t.Fatalf("undeclared request status=%d body=%q, want 200", rr.Code, rr.Body.String())
	}
	if _, osl := be.snapshot(); len(osl) != 1 || osl[0] != 3500 {
		t.Fatalf("osl=%v, want one request reserving 3500 output tokens", osl)
	}

	rr := serveChat(s, up, chatBody(4*2500, 4096))
	if rr.Code != http.StatusRequestEntityTooLarge || rr.Header().Get("Retry-After") != "" {
		t.Fatalf("declared max_tokens=4096 status=%d, want 413 without Retry-After", rr.Code)
	}
	if hits, _ := be.snapshot(); hits != 1 {
		t.Fatalf("upstream hits=%d, the declared over-KV request must not reach the engine", hits)
	}

	embed := sharedRequest(up)
	embed.URL.Path = "/v1/embeddings"
	embed.RequestURI = "/v1/embeddings"
	replaceRequestBody(embed, []byte(`{"model":"model-a","input":"`+strings.Repeat("y", 400)+`"}`))
	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, embed)
	if rr.Code != http.StatusOK {
		t.Fatalf("embeddings status=%d body=%q, want 200", rr.Code, rr.Body.String())
	}
	if _, osl := be.snapshot(); len(osl) != 2 || osl[1] != 1 {
		t.Fatalf("osl=%v, want the embeddings request to reserve exactly 1 output token", osl)
	}
}

func TestUndeclaredOutputWithinKV(t *testing.T) {
	for _, c := range []struct{ output, input, sequences, cap, want int64 }{
		{4096, 200, 1, 0, 4096},    // no cap
		{4096, 200, 1, 3000, 2800}, // clamped to the room left
		{100, 200, 1, 3000, 100},   // already fits
		{4096, 3000, 1, 3000, 1},   // prompt fills the cap: floor 1, still 413
		{4096, 5000, 1, 3000, 1},   // prompt alone over the cap
		{4096, 200, 4, 3000, 700},  // n=4: each sequence gets a quarter of the room
		{4096, 200, 0, 3000, 2800}, // 0 sequences reads as 1
	} {
		if got := undeclaredOutputWithinKV(c.output, c.input, c.sequences, c.cap); got != c.want {
			t.Errorf("undeclaredOutputWithinKV(%d, %d, %d, %d) = %d, want %d", c.output, c.input, c.sequences, c.cap, got, c.want)
		}
	}
}

func TestAdmissionWorkCountsOutputSequences(t *testing.T) {
	for _, c := range []struct {
		body string
		want int64
		ok   bool
	}{
		{`{"model":"m"}`, 1, true},
		{`{"model":"m","n":4}`, 4, true},
		{`{"model":"m","n":2,"best_of":5}`, 5, true},
		{`{"model":"m","n":0}`, 1, true},
		{`{"model":"m","n":70000}`, 0, false},
		{`{"model":"m","n":2,"N":9}`, 0, false},
		// A batched completions prompt is one generation per prompt.
		{`{"model":"m","prompt":["a","b","c"],"n":2}`, 6, true},
		{`{"model":"m","prompt":[[1,2],[3]]}`, 2, true},
		{`{"model":"m","prompt":[1,2,3]}`, 1, true},
		{`{"model":"m","prompt":"abc","n":3}`, 3, true},
		{`{"model":"m","prompt":["a","b"],"best_of":40000}`, 0, false}, // 80000 sequences
	} {
		got, ok := admissionWork([]byte(c.body), 10)
		if ok != c.ok || (ok && got.OutputSequences != c.want) {
			t.Errorf("admissionWork(%s) = (%d, %v), want (%d, %v)", c.body, got.OutputSequences, ok, c.want, c.ok)
		}
	}
}

// TestAdmissionWorkRefusesReservedOutputBeyondExactLeasePrecision: output x
// sequences above maxReservedOutput (2^46) is refused like a broken body, so no
// KV charge reaches the 14 significant digits Redis's cjson keeps for a
// number; a huge max_tokens with one sequence is still accepted.
func TestAdmissionWorkRefusesReservedOutputBeyondExactLeasePrecision(t *testing.T) {
	for _, c := range []struct {
		body string
		ok   bool
	}{
		{`{"model":"m","max_tokens":4000000000,"n":40000}`, false},       // 1.6e14
		{`{"model":"m","max_tokens":4000000000,"best_of":40000}`, false}, // same via best_of
		{`{"model":"m","max_tokens":4000000000,"n":1}`, true},
		{`{"model":"m","max_tokens":4000000000}`, true},
		{`{"model":"m","max_tokens":1073741824,"n":65536}`, true},  // exactly 2^46
		{`{"model":"m","max_tokens":1073741825,"n":65536}`, false}, // one sequence-width past
		// The prompt count multiplies too: 2^30 x 2 x 32768 = 2^46 fits, one
		// more token per sequence does not.
		{`{"model":"m","max_tokens":1073741824,"n":2,"prompt":[` + strings.TrimSuffix(strings.Repeat(`"a",`, 32768), ",") + `]}`, true},
		{`{"model":"m","max_tokens":1073741825,"n":2,"prompt":[` + strings.TrimSuffix(strings.Repeat(`"a",`, 32768), ",") + `]}`, false},
	} {
		got, ok := admissionWork([]byte(c.body), 10)
		if ok != c.ok {
			t.Errorf("admissionWork(%s) ok=%v, want %v", c.body, ok, c.ok)
		}
		if ok && got.OutputTokens*got.OutputSequences > maxReservedOutput {
			t.Errorf("admissionWork(%s) accepted %d x %d past the bound", c.body, got.OutputTokens, got.OutputSequences)
		}
	}
}
