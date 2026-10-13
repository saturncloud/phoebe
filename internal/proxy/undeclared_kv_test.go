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
	"github.com/saturncloud/phoebe/internal/identity"
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

func TestUndeclaredOutputWithinCaps(t *testing.T) {
	for _, c := range []struct{ output, input, sequences, kvCap, outCap, want int64 }{
		{4096, 200, 1, 0, 0, 4096},        // no cap
		{4096, 200, 1, 3000, 0, 2800},     // clamped to the KV room left
		{100, 200, 1, 3000, 0, 100},       // already fits
		{4096, 3000, 1, 3000, 0, 1},       // prompt fills the cap: floor 1, still 413
		{4096, 5000, 1, 3000, 0, 1},       // prompt alone over the cap
		{4096, 200, 4, 3000, 0, 700},      // n=4: each sequence gets a quarter of the room
		{4096, 200, 0, 3000, 0, 2800},     // 0 sequences reads as 1
		{4096, 200, 1, 0, 2048, 2048},     // output cap alone
		{4096, 200, 4, 0, 2048, 512},      // n=4: the output counter is charged output x sequences
		{4096, 200, 1, 3000, 2048, 2048},  // output cap tighter than the KV room
		{4096, 2000, 1, 3000, 2048, 1000}, // KV room tighter than the output cap
		{100, 200, 1, 3000, 2048, 100},    // fits both
	} {
		if got := undeclaredOutputWithinCaps(c.output, c.input, c.sequences, c.kvCap, c.outCap); got != c.want {
			t.Errorf("undeclaredOutputWithinCaps(%d, %d, %d, %d, %d) = %d, want %d", c.output, c.input, c.sequences, c.kvCap, c.outCap, got, c.want)
		}
	}
}

func TestAdmissionWorkCountsOutputSequences(t *testing.T) {
	const chat, completions = "/v1/chat/completions", "/v1/completions"
	for _, c := range []struct {
		route string
		body  string
		want  int64
		ok    bool
	}{
		{chat, `{"model":"m"}`, 1, true},
		{chat, `{"model":"m","n":4}`, 4, true},
		{chat, `{"model":"m","n":2,"best_of":5}`, 5, true},
		{chat, `{"model":"m","n":0}`, 1, true},
		{chat, `{"model":"m","n":70000}`, 0, false},
		{chat, `{"model":"m","n":2,"N":9}`, 0, false},
		// A batched completions prompt is one generation per prompt.
		{completions, `{"model":"m","prompt":["a","b","c"],"n":2}`, 6, true},
		{completions, `{"model":"m","prompt":[[1,2],[3]]}`, 2, true},
		{completions, `{"model":"m","prompt":[1,2,3]}`, 1, true},
		{completions, `{"model":"m","prompt":"abc","n":3}`, 3, true},
		{completions, `{"model":"m","prompt":["a","b"],"best_of":40000}`, 0, false}, // 80000 sequences
	} {
		got, ok := admissionWork([]byte(c.body), 10, c.route)
		if ok != c.ok || (ok && got.OutputSequences != c.want) {
			t.Errorf("admissionWork(%s, %s) = (%d, %v), want (%d, %v)", c.route, c.body, got.OutputSequences, ok, c.want, c.ok)
		}
	}
}

// The batched-prompt multiplier applies only on /v1/completions (D27). Chat
// completions and embeddings ignore a top-level "prompt", so an extra
// "prompt" array there must not multiply the output sequences admission
// charges, nor push a request past the sequence bound.
func TestAdmissionWorkBatchMultiplierIsCompletionsOnly(t *testing.T) {
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"prompt":["a","b"]}`)
	for _, c := range []struct {
		route string
		want  int64
	}{
		{"/v1/chat/completions", 1},
		{"/v1/embeddings", 1},
		{"/v1/completions", 2},
	} {
		got, ok := admissionWork(body, 10, c.route)
		if !ok || got.OutputSequences != c.want {
			t.Errorf("admissionWork(%s) = (%d, %v), want (%d, true)", c.route, got.OutputSequences, ok, c.want)
		}
	}
	// 70000 prompts would be refused on completions; chat ignores them.
	huge := []byte(`{"model":"m","prompt":[` + strings.TrimSuffix(strings.Repeat(`"a",`, 70000), ",") + `]}`)
	if got, ok := admissionWork(huge, 10, "/v1/chat/completions"); !ok || got.OutputSequences != 1 {
		t.Errorf("chat with a 70000-prompt array = (%d, %v), want (1, true)", got.OutputSequences, ok)
	}
	if _, ok := admissionWork(huge, 10, "/v1/completions"); ok {
		t.Error("completions with a 70000-prompt array accepted past the sequence bound")
	}
}

// D28 (revised, D32): n, best_of, max_tokens and max_completion_tokens are
// read the way the engine's validation reads them (pydantic lax integers): a
// JSON integer, an integral float (2.0, 1e0) or an integer string ("4") counts
// at its value, so admission charges what the engine runs and no request the
// engine accepts gets a new phoebe 400. A fraction or a non-numeric value,
// which the engine would refuse too, is refused.
func TestAdmissionWorkReadsCountsLikeTheEngine(t *testing.T) {
	for _, c := range []struct {
		body      string
		sequences int64
		output    int64
		ok        bool
	}{
		{`{"model":"m","n":4}`, 4, 10, true},
		{`{"model":"m","n":4.0}`, 4, 10, true},
		{`{"model":"m","n":"4"}`, 4, 10, true},
		{`{"model":"m","n":1e0}`, 1, 10, true},
		{`{"model":"m","best_of":"2"}`, 2, 10, true},
		{`{"model":"m","max_tokens":16.0}`, 1, 16, true},
		{`{"model":"m","max_tokens":"16"}`, 1, 16, true},
		{`{"model":"m","max_completion_tokens":"16"}`, 1, 16, true},
		{`{"model":"m","n":null}`, 1, 10, true},
		{`{"model":"m","n":1.5}`, 0, 0, false},
		{`{"model":"m","n":"four"}`, 0, 0, false},
		{`{"model":"m","n":true}`, 0, 0, false},
		{`{"model":"m","max_tokens":16.5}`, 0, 0, false},
		{`{"model":"m","max_tokens":"16","max_completion_tokens":16}`, 1, 16, true},
		{`{"model":"m","max_tokens":"16","max_completion_tokens":17}`, 0, 0, false},
	} {
		got, ok := admissionWork([]byte(c.body), 10, "/v1/chat/completions")
		if ok != c.ok || (ok && (got.OutputSequences != c.sequences || got.OutputTokens != c.output)) {
			t.Errorf("admissionWork(%s) = (seq %d, out %d, %v), want (seq %d, out %d, %v)",
				c.body, got.OutputSequences, got.OutputTokens, ok, c.sequences, c.output, c.ok)
		}
	}
}

// End to end: a count the engine would accept is served and charged at its
// value; a fractional one gets 400, creates no admission state and never
// reaches the engine.
func TestSharedRequestReadsCountsLikeTheEngine(t *testing.T) {
	for _, field := range []string{"n", "best_of", "max_tokens", "max_completion_tokens"} {
		for _, c := range []struct {
			value string
			ok    bool
		}{{`2.0`, true}, {`"2"`, true}, {`1.5`, false}} {
			t.Run(field+"="+c.value, func(t *testing.T) {
				be := &oslRecordingBackend{}
				srv := httptest.NewServer(be)
				defer srv.Close()
				up, _ := url.Parse(srv.URL)
				s, mr := kvTestServerWithStore(t, kvTestConfig())
				body := []byte(`{"model":"model-a","` + field + `":` + c.value + `,"messages":[{"role":"user","content":"hi"}]}`)
				rr := serveChat(s, up, body)
				hits, _ := be.snapshot()
				if c.ok {
					if rr.Code != http.StatusOK || hits != 1 {
						t.Fatalf("status=%d hits=%d, want 200 served", rr.Code, hits)
					}
					return
				}
				if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "invalid shared inference request") {
					t.Fatalf("status=%d body=%q, want the 400 invalid shared inference request", rr.Code, rr.Body.String())
				}
				if hits != 0 {
					t.Fatalf("upstream hits=%d, a refused request must not reach the engine", hits)
				}
				if keys := mr.Keys(); len(keys) != 0 {
					t.Fatalf("admission store holds %v after a 400, want no admission record", keys)
				}
			})
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
		got, ok := admissionWork([]byte(c.body), 10, "/v1/completions")
		if ok != c.ok {
			t.Errorf("admissionWork(%s) ok=%v, want %v", c.body, ok, c.ok)
		}
		if ok && got.OutputTokens*got.OutputSequences > maxReservedOutput {
			t.Errorf("admissionWork(%s) accepted %d x %d past the bound", c.body, got.OutputTokens, got.OutputSequences)
		}
	}
}

// assertNoAdmissionCountersHeld fails when any admission counter in mr is
// non-zero: a refused request must leave nothing charged.
func assertNoAdmissionCountersHeld(t *testing.T, mr *miniredis.Miniredis, prefix string) {
	t.Helper()
	key := "{" + prefix + "}:counters"
	if !mr.Exists(key) {
		return
	}
	fields, err := mr.HKeys(key)
	if err != nil {
		t.Fatalf("read admission counters: %v", err)
	}
	for _, field := range fields {
		if v := mr.HGet(key, field); v != "0" && v != "" {
			t.Fatalf("admission counter %s=%s after a refused request, want 0", field, v)
		}
	}
}

func kvTestServerWithStore(t *testing.T, cfg config.AdmissionSettings) (*Server, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithAdmitter(admission.New(client, cfg)), mr
}

// A request that sent no max_tokens and whose prompt alone fills (or passes)
// a positive KV cap cannot be fitted by clamping its default output: it is
// refused 413 without Retry-After, the message blames the prompt alone (there
// is no max_tokens to lower), nothing reaches the engine and nothing stays
// charged.
func TestUndeclaredPromptFillingKVCapIs413(t *testing.T) {
	body := chatBody(4*2500, 0)
	est := estimatePromptTokens(body)
	assert413 := func(t *testing.T, s *Server, mr *miniredis.Miniredis, be *oslRecordingBackend, up *url.URL) {
		t.Helper()
		rr := serveChat(s, up, body)
		if rr.Code != http.StatusRequestEntityTooLarge || rr.Header().Get("Retry-After") != "" {
			t.Fatalf("status=%d retry-after=%q body=%q, want 413 without Retry-After", rr.Code, rr.Header().Get("Retry-After"), rr.Body.String())
		}
		if b := rr.Body.String(); !strings.Contains(b, "KV-cache") || strings.Contains(b, "max_tokens") {
			t.Fatalf("body=%q, want the KV-cache message without max_tokens", b)
		}
		if hits, _ := be.snapshot(); hits != 0 {
			t.Fatalf("upstream hits=%d, a refused request must not reach the engine", hits)
		}
		assertNoAdmissionCountersHeld(t, mr, kvTestConfig().KeyPrefix)
	}
	for _, c := range []struct {
		name  string
		kvCap int64
	}{
		{"prompt fills the published cap exactly", est},
		{"prompt alone is over the published cap", est - 100},
	} {
		t.Run(c.name, func(t *testing.T) {
			be := &oslRecordingBackend{}
			srv := httptest.NewServer(be)
			defer srv.Close()
			up, _ := url.Parse(srv.URL)
			cfg := kvTestConfig()
			cfg.Enabled = false
			s, mr := kvTestServerWithStore(t, cfg)
			s.graphCapacity = staticGraphCapacity{graphFromUpstreamHost(up.Host): {MaxReservedKVTokens: ptr64(c.kvCap)}}
			assert413(t, s, mr, be, up)
		})
	}
	t.Run("prompt fills the weighted lane share", func(t *testing.T) {
		be := &oslRecordingBackend{}
		srv := httptest.NewServer(be)
		defer srv.Close()
		up, _ := url.Parse(srv.URL)
		cfg := kvTestConfig()
		cfg.Lanes = map[string]config.AdmissionLane{"default": {Weight: 2, Limits: config.AdmissionLimits{MaxReservedKVTokens: ptr64(est / 2)}}}
		s, mr := kvTestServerWithStore(t, cfg)
		assert413(t, s, mr, be, up)
	})
}

// A request that sent no max_tokens is never answered a retry-forever 503
// for a default it did not choose: under an operator graph
// maxReservedOutputTokens of 2048 with a default of 4096 it is admitted, and
// the reservation (the Dynamo output hint) is clamped to 2048. A prompt that
// fits the KV cap but whose default output does not is admitted the same way
// with its reservation clamped to the KV room.
func TestUndeclaredOutputReservationFitsOperatorOutputCap(t *testing.T) {
	be := &oslRecordingBackend{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)

	cfg := kvTestConfig()
	cfg.Graph = config.AdmissionLimits{MaxReservedOutputTokens: ptr64(2048)}
	s := kvTestServer(t, cfg)
	if rr := serveChat(s, up, chatBody(4*200, 0)); rr.Code != http.StatusOK {
		t.Fatalf("undeclared request status=%d body=%q, want 200", rr.Code, rr.Body.String())
	}
	if hits, osl := be.snapshot(); hits != 1 || len(osl) != 1 || osl[0] != 2048 {
		t.Fatalf("hits=%d osl=%v, want one request reserving 2048 output tokens", hits, osl)
	}

	cfg.Graph.MaxReservedKVTokens = ptr64(200 + 1000)
	s = kvTestServer(t, cfg)
	if rr := serveChat(s, up, chatBody(4*200, 0)); rr.Code != http.StatusOK {
		t.Fatalf("undeclared request under both caps status=%d body=%q, want 200", rr.Code, rr.Body.String())
	}
	if _, osl := be.snapshot(); len(osl) != 2 || osl[1] != 1000 {
		t.Fatalf("osl=%v, want the second request reserving the 1000-token KV room", osl)
	}
}

// An undeclared max_tokens with n=4 reserves the default only as far as
// default x 4 fits the tightest contract generated-token limit: with a
// 400-token owner window each sequence gets 100, the Dynamo hint is 100,
// and the request is admitted instead of refused for a default the client
// did not choose.
func TestUndeclaredOutputPerSequenceFitsContractGeneratedLimit(t *testing.T) {
	be := &oslRecordingBackend{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)
	s := kvTestServer(t, kvTestConfig())

	req := sharedRequest(up)
	req.Header.Set(identity.HeaderOwnerRateLimitGeneratedTokens, "400")
	replaceRequestBody(req, []byte(`{"model":"model-a","n":4,"messages":[{"role":"user","content":"hi"}]}`))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("undeclared n=4 status=%d body=%q, want 200", rr.Code, rr.Body.String())
	}
	if hits, osl := be.snapshot(); hits != 1 || len(osl) != 1 || osl[0] != 100 {
		t.Fatalf("hits=%d osl=%v, want one request reserving 400 / 4 = 100 output tokens", hits, osl)
	}
}

// Embeddings have no client-declared output (phoebe reserves one token on
// its own), so a KV 413 on /v1/embeddings blames the prompt alone and never
// tells the client to lower max_tokens.
func TestEmbeddingsKVRefusalNeverMentionsMaxTokens(t *testing.T) {
	be := &oslRecordingBackend{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)
	cfg := kvTestConfig()
	cfg.Enabled = false
	s := kvTestServer(t, cfg)
	s.graphCapacity = staticGraphCapacity{graphFromUpstreamHost(up.Host): {MaxReservedKVTokens: ptr64(10)}}

	embed := sharedRequest(up)
	embed.URL.Path = "/v1/embeddings"
	embed.RequestURI = "/v1/embeddings"
	replaceRequestBody(embed, []byte(`{"model":"model-a","input":"`+strings.Repeat("y", 400)+`"}`))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, embed)
	body := rr.Body.String()
	if rr.Code != http.StatusRequestEntityTooLarge || rr.Header().Get("Retry-After") != "" {
		t.Fatalf("status=%d retry-after=%q body=%q, want 413 without Retry-After", rr.Code, rr.Header().Get("Retry-After"), body)
	}
	if !strings.Contains(body, "estimated prompt tokens exceed") || !strings.Contains(body, "KV-cache") || strings.Contains(body, "max_tokens") {
		t.Fatalf("body=%q, want the prompt-only KV-cache wording without max_tokens", body)
	}
	if hits, _ := be.snapshot(); hits != 0 {
		t.Fatalf("upstream hits=%d, the over-KV embeddings request must not reach the engine", hits)
	}
}

// An undeclared max_tokens is clamped to one token per sequence, so when n
// pushes the reservation (prompt + n) over the KV cap while the prompt alone
// fits, the 413 names the sequence count and not the prompt alone or
// max_tokens: lowering n is what would admit it.
func TestUndeclaredKVRefusalDrivenByNNamesSequences(t *testing.T) {
	be := &oslRecordingBackend{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)
	cfg := kvTestConfig()
	cfg.Enabled = false
	s := kvTestServer(t, cfg)
	prompt := chatBody(4*2000, 0)
	est := estimatePromptTokens(prompt)
	s.graphCapacity = staticGraphCapacity{graphFromUpstreamHost(up.Host): {MaxReservedKVTokens: ptr64(est + 10)}}

	body := []byte(strings.Replace(string(prompt), `{"model":"model-a",`, `{"model":"model-a","n":64,`, 1))
	rr := serveChat(s, up, body)
	got := rr.Body.String()
	if rr.Code != http.StatusRequestEntityTooLarge || rr.Header().Get("Retry-After") != "" {
		t.Fatalf("status=%d retry-after=%q body=%q, want 413 without Retry-After", rr.Code, rr.Header().Get("Retry-After"), got)
	}
	if !strings.Contains(got, "one output token per requested sequence (n, best_of, or batched prompts)") || strings.Contains(got, "max_tokens") {
		t.Fatalf("body=%q, want the sequence-count KV wording without max_tokens", got)
	}
	if hits, _ := be.snapshot(); hits != 0 {
		t.Fatalf("upstream hits=%d, the over-KV request must not reach the engine", hits)
	}
}

// A request whose own reserved output (max_tokens x output sequences, or the
// one-token clamp x sequences when max_tokens is absent) is larger than a
// positive maxReservedOutputTokens cap can never be admitted: it is refused
// 413 without Retry-After, never a retryable 503 the client would retry
// forever, and it never reaches the engine.
func TestRequestOutputAboveReservedOutputCapIs413(t *testing.T) {
	be := &oslRecordingBackend{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)

	cfg := kvTestConfig()
	cfg.Lanes = map[string]config.AdmissionLane{"default": {Weight: 1, Limits: config.AdmissionLimits{MaxReservedOutputTokens: ptr64(64)}}}
	s := kvTestServer(t, cfg)

	for _, tc := range []struct {
		name, body, want string
	}{
		{"undeclared max_tokens, n=128", `{"model":"model-a","n":128,"messages":[{"role":"user","content":"hi"}]}`,
			"the number of output sequences " + sequenceTerms + " exceeds a configured reserved-output limit"},
		{"max_tokens=10, n=10", `{"model":"model-a","max_tokens":10,"n":10,"messages":[{"role":"user","content":"hi"}]}`,
			"max_tokens times the number of output sequences " + sequenceTerms + " exceeds a configured reserved-output limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := serveChat(s, up, []byte(tc.body))
			if rr.Code != http.StatusRequestEntityTooLarge || rr.Header().Get("Retry-After") != "" {
				t.Fatalf("status=%d retry-after=%q body=%q, want 413 without Retry-After", rr.Code, rr.Header().Get("Retry-After"), rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), tc.want) {
				t.Fatalf("body=%q, want %q", rr.Body.String(), tc.want)
			}
		})
	}
	if hits, _ := be.snapshot(); hits != 0 {
		t.Fatalf("upstream hits=%d, an over-cap output request must not reach the engine", hits)
	}
}
