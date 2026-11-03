package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/emit"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

func newMetricsTestServer(t *testing.T) *Server {
	t.Helper()
	s := &config.Settings{ListenAddr: ":0", MetricsAddr: ":0"}
	return New(s, logging.New(logging.ERROR), &recordingEmitter{})
}

// phoebeFamilies gathers the phoebe_* metric families (the Go and process
// collectors are left out).
func phoebeFamilies(t *testing.T, m *metrics) map[string]*dto.MetricFamily {
	t.Helper()
	fams, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*dto.MetricFamily{}
	for _, f := range fams {
		if strings.HasPrefix(f.GetName(), metricsNamespace+"_") {
			out[f.GetName()] = f
		}
	}
	return out
}

// setMetricsTestIdentity sets the headers that pass the billing-identity gate.
func setMetricsTestIdentity(req *http.Request) {
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "res-1")
	req.Header.Set(identity.HeaderServingMode, identity.ServingModeDedicated)
}

func freeLocalAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func labelMap(m *dto.Metric) map[string]string {
	out := map[string]string{}
	for _, l := range m.GetLabel() {
		out[l.GetName()] = l.GetValue()
	}
	return out
}

// histogramCount returns the sample count of the series with exactly these
// labels, or -1 when there is no such series.
func histogramCount(t *testing.T, m *metrics, name string, want map[string]string) int64 {
	t.Helper()
	f, ok := phoebeFamilies(t, m)[name]
	if !ok {
		return -1
	}
	for _, s := range f.GetMetric() {
		if fmt.Sprint(labelMap(s)) == fmt.Sprint(want) {
			return int64(s.GetHistogram().GetSampleCount())
		}
	}
	return -1
}

func sendIdentified(t *testing.T, srv *Server, upstream *url.URL, resourceID string) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	setUpstream(req, upstream)
	req.Header.Set(identity.HeaderAuthID, "token-secret-777")
	req.Header.Set(identity.HeaderUserID, "user-alice-555")
	req.Header.Set(identity.HeaderOrgID, "org-acme-333")
	if resourceID != "" {
		req.Header.Set(identity.HeaderResourceID, resourceID)
	}
	srv.Handler().ServeHTTP(rr, req)
	return rr
}

// TestMetricsLabelSetIsFixed pins the approved label set: every phoebe_* series
// carries only resource_id, serving_mode and outcome (in-flight: the first
// two), and no org, user or token value ever appears as a label value.
func TestMetricsLabelSetIsFixed(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	srv := newMetricsTestServer(t)
	if rr := sendIdentified(t, srv, upstream, "res-1"); rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	recordFrom(context.WithValue(context.Background(), requestRecordKey{}, &requestRecord{m: srv.metrics})).observeWake(0, wakeOutcomeOK)

	allowed := map[string]map[string]bool{
		"phoebe_request_duration_seconds": {"resource_id": true, "serving_mode": true, "outcome": true},
		"phoebe_wake_hold_seconds":        {"resource_id": true, "serving_mode": true, "outcome": true},
		"phoebe_requests_in_flight":       {"resource_id": true, "serving_mode": true},
	}
	fams := phoebeFamilies(t, srv.metrics)
	for name := range fams {
		if _, ok := allowed[name]; !ok {
			t.Errorf("unexpected metric family %s: every new family needs a review of its labels", name)
		}
	}
	for name, labels := range allowed {
		f, ok := fams[name]
		if !ok {
			t.Errorf("metric family %s missing", name)
			continue
		}
		for _, s := range f.GetMetric() {
			for k, v := range labelMap(s) {
				if !labels[k] {
					t.Errorf("%s has label %q, not in the approved set", name, k)
				}
				for _, secret := range []string{"token-secret-777", "user-alice-555", "org-acme-333"} {
					if strings.Contains(v, secret) {
						t.Errorf("%s label %s=%q carries a request identity value", name, k, v)
					}
				}
			}
		}
	}
}

// TestMetricsRecordRequestAndInFlight: a served request is counted once under
// its trusted resource id and serving mode, and is no longer in flight after.
func TestMetricsRecordRequestAndInFlight(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	srv := newMetricsTestServer(t)
	sendIdentified(t, srv, upstream, "res-1")

	want := map[string]string{"resource_id": "res-1", "serving_mode": "dedicated", "outcome": "ok"}
	if got := histogramCount(t, srv.metrics, "phoebe_request_duration_seconds", want); got != 1 {
		t.Fatalf("request count for %v = %d, want 1", want, got)
	}
	g := srv.metrics.inFlight.WithLabelValues("res-1", "dedicated")
	var out dto.Metric
	_ = g.Write(&out)
	if v := out.GetGauge().GetValue(); v != 0 {
		t.Fatalf("in flight after the request = %v, want 0", v)
	}
}

// TestMetricsRequestRefusedBeforeBillingGateIsUnresolved: a request that never
// passed the billing-identity gate has no trusted resource id, so it is counted
// as unresolved and never under the value its headers claimed.
func TestMetricsRequestRefusedBeforeBillingGateIsUnresolved(t *testing.T) {
	srv := newMetricsTestServer(t)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set(identity.HeaderResourceID, "claimed-res") // no auth id: refused at the gate
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	want := map[string]string{"resource_id": resourceUnresolved, "serving_mode": servingModeUnknown, "outcome": "client_error"}
	if got := histogramCount(t, srv.metrics, "phoebe_request_duration_seconds", want); got != 1 {
		t.Fatalf("unresolved count = %d, want 1", got)
	}
	for _, s := range phoebeFamilies(t, srv.metrics)["phoebe_request_duration_seconds"].GetMetric() {
		if labelMap(s)["resource_id"] == "claimed-res" {
			t.Fatal("an ungated request was labelled with its claimed resource id")
		}
	}
}

// TestMetricsResourceLabelFoldsToOverflow: distinct resource ids beyond
// maxMetricResources fold into one overflow value. The end-to-end series bound
// is TestMetricsSeriesStayBoundedPastResourceCap.
func TestMetricsResourceLabelFoldsToOverflow(t *testing.T) {
	m := newMetrics()
	for i := 0; i < maxMetricResources; i++ {
		if got := m.resourceLabel(fmt.Sprintf("r%d", i)); got != fmt.Sprintf("r%d", i) {
			t.Fatalf("id %d folded early: %q", i, got)
		}
	}
	if got := m.resourceLabel("one-too-many"); got != resourceOverflow {
		t.Fatalf("id past the cap = %q, want %q", got, resourceOverflow)
	}
	if got := m.resourceLabel("r0"); got != "r0" {
		t.Fatalf("an id seen before the cap = %q, want it kept", got)
	}
	if got := m.resourceLabel(""); got != resourceUnresolved {
		t.Fatalf("empty id = %q, want %q", got, resourceUnresolved)
	}
}

func TestMetricsServingModeLabelIsBounded(t *testing.T) {
	for in, want := range map[string]string{
		identity.ServingModeShared:    identity.ServingModeShared,
		identity.ServingModeDedicated: identity.ServingModeDedicated,
		"":                            servingModeUnknown,
		"anything-else":               servingModeUnknown,
	} {
		if got := servingModeLabel(in); got != want {
			t.Errorf("servingModeLabel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMetricsRequestOutcome(t *testing.T) {
	cases := []struct {
		status  int
		aborted bool
		want    string
	}{
		{200, false, outcomeOK},
		{304, false, outcomeOK},
		{404, false, outcomeClientError},
		{429, false, outcomeClientError},
		{503, false, outcomeServerError},
		{200, true, outcomeAborted},
		{503, true, outcomeAborted},
	}
	for _, c := range cases {
		if got := requestOutcome(c.status, c.aborted); got != c.want {
			t.Errorf("requestOutcome(%d, %v) = %q, want %q", c.status, c.aborted, got, c.want)
		}
	}
}

func TestWakeOutcome(t *testing.T) {
	expired, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want string
	}{
		{"woke", context.Background(), nil, wakeOutcomeOK},
		{"unavailable", context.Background(), fmt.Errorf("x: %w", ErrWakeUnavailable), wakeOutcomeUnavailable},
		{"status unknown", context.Background(), ErrWakeStatusUnknown, wakeOutcomeStatusUnknown},
		{"not ready", context.Background(), ErrWakeNotReady, wakeOutcomeNotReady},
		{"wrapped not ready", context.Background(), fmt.Errorf("x: %w", ErrWakeNotReady), wakeOutcomeNotReady},
		{"hold deadline", expired, errors.New("wait aborted"), wakeOutcomeTimeout},
		{"hold deadline with its own error", expired, fmt.Errorf("x: %w", context.DeadlineExceeded), wakeOutcomeTimeout},
		{"client cancelled the hold", cancelled, context.Canceled, wakeOutcomeCanceled},
		{"client cancelled the hold, wrapped", cancelled, fmt.Errorf("x: %w", context.Canceled), wakeOutcomeCanceled},
		{"inner deadline with the hold still live", context.Background(), fmt.Errorf("x: %w", context.DeadlineExceeded), wakeOutcomeError},
		{"inner cancel with the hold still live", context.Background(), context.Canceled, wakeOutcomeError},
		{"other", context.Background(), errors.New("boom"), wakeOutcomeError},
	}
	for _, c := range cases {
		if got := wakeOutcome(c.ctx, c.err); got != c.want {
			t.Errorf("%s: wakeOutcome = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestMetricsPathRefusedAtBillingGateOnProxyPort: an unidentified GET /metrics
// through the proxy handler stops at the billing gate. It proves nothing about
// routing behind the gate; TestMetricsPathNotExposedToIdentifiedCallerOnProxyPort
// and TestMetricsServedOnlyOnMetricsListener cover that.
func TestMetricsPathRefusedAtBillingGateOnProxyPort(t *testing.T) {
	srv := newMetricsTestServer(t)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 from the billing gate", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if strings.Contains(string(body), "phoebe_") || strings.Contains(string(body), "# HELP") {
		t.Fatalf("the proxy port served metrics: %d %q", rr.Code, body)
	}
}

// TestMetricsPathNotExposedToIdentifiedCallerOnProxyPort: a caller past the
// billing gate gets neither phoebe's exposition nor the upstream's /metrics.
// The route allow-list refuses the path whether or not the route carries a
// served-model binding.
func TestMetricsPathNotExposedToIdentifiedCallerOnProxyPort(t *testing.T) {
	var backendHits atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		backendHits.Add(1)
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)

	for name, servedModel := range map[string]string{"no allow-list": "", "bound route": "m"} {
		t.Run(name, func(t *testing.T) {
			srv := newMetricsTestServer(t)
			srv.metrics.inFlight.WithLabelValues("res-1", "shared").Set(0)
			req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			setUpstream(req, upstream)
			setMetricsTestIdentity(req)
			if servedModel != "" {
				req.Header.Set(identity.HeaderServedModel, servedModel)
			}
			rr := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want the allow-list's 404", rr.Code)
			}
			if body := rr.Body.String(); strings.Contains(body, "phoebe_") || strings.Contains(body, "# HELP") {
				t.Fatalf("the proxy port served metrics: %q", body)
			}
			if got := backendHits.Load(); got != 0 {
				t.Fatalf("upstream reached %d times for /metrics", got)
			}
		})
	}
}

// TestMetricsServedOnlyOnMetricsListener runs both real listeners: /metrics
// answers with the phoebe families on the metrics port, and the proxy port does
// not expose them even to an identified caller.
func TestMetricsServedOnlyOnMetricsListener(t *testing.T) {
	proxyAddr, metricsAddr := freeLocalAddr(t), freeLocalAddr(t)
	srv := New(&config.Settings{ListenAddr: proxyAddr, MetricsAddr: metricsAddr}, logging.New(logging.ERROR), &recordingEmitter{})
	srv.metrics.inFlight.WithLabelValues("res-1", "shared").Set(0)

	stop := make(chan os.Signal, 1)
	done := make(chan error, 1)
	go func() { done <- srv.serve(stop) }()
	defer func() {
		stop <- syscall.SIGTERM
		if err := <-done; err != nil {
			t.Errorf("serve: %v", err)
		}
	}()

	get := func(addr string, identified bool) (int, string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+addr+"/metrics", nil)
			if identified {
				setMetricsTestIdentity(req)
			}
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)
				return resp.StatusCode, string(body)
			}
			if time.Now().After(deadline) {
				t.Fatalf("GET %s: %v", addr, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	if code, body := get(metricsAddr, false); code != http.StatusOK || !strings.Contains(body, "phoebe_requests_in_flight") {
		t.Fatalf("metrics port: %d %q", code, body)
	}
	for _, identified := range []bool{false, true} {
		code, body := get(proxyAddr, identified)
		if code == http.StatusOK || strings.Contains(body, "phoebe_") || strings.Contains(body, "# HELP") {
			t.Fatalf("proxy port (identified=%v) served metrics: %d %q", identified, code, body)
		}
	}
}

// TestMetricsMuxServesOnlyMetrics: the metrics listener answers /metrics and
// returns 404 for every other path, so adding a route to it fails this test.
func TestMetricsMuxServesOnlyMetrics(t *testing.T) {
	srv := newMetricsTestServer(t)
	metricsSrv := newMetricsServer(":0", srv.metrics.Handler())

	cases := []struct {
		path string
		want int
	}{
		{"/metrics", http.StatusOK},
		{"/", http.StatusNotFound},
		{"/metrics/", http.StatusNotFound},
		{"/debug/pprof", http.StatusNotFound},
		{"/debug/pprof/", http.StatusNotFound},
		{"/healthz", http.StatusNotFound},
		{"/other", http.StatusNotFound},
	}
	for _, c := range cases {
		rr := httptest.NewRecorder()
		metricsSrv.Handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, c.path, nil))
		if rr.Code != c.want {
			t.Errorf("GET %s = %d, want %d", c.path, rr.Code, c.want)
		}
	}
}

// TestRunStopsWhenMetricsPortInUse: a failure to bind the metrics port stops
// phoebe with that error. Without this, phoebe would keep serving with no
// metrics, which looks the same as an idle install.
func TestRunStopsWhenMetricsPortInUse(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()

	srv := New(&config.Settings{ListenAddr: "127.0.0.1:0", MetricsAddr: occupied.Addr().String()}, logging.New(logging.ERROR), &recordingEmitter{})

	done := make(chan error, 1)
	go func() { done <- srv.serve(make(chan os.Signal)) }()

	select {
	case err := <-done:
		if !errors.Is(err, syscall.EADDRINUSE) {
			t.Fatalf("serve returned %v, want the metrics port's address-in-use error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serve kept running after the metrics port failed to bind")
	}
}

// TestMetricsExpositionOnMetricsHandler: the metrics listener's handler serves
// the phoebe families.
func TestMetricsExpositionOnMetricsHandler(t *testing.T) {
	srv := newMetricsTestServer(t)
	srv.metrics.inFlight.WithLabelValues("res-1", "shared").Set(0)
	rr := httptest.NewRecorder()
	srv.metrics.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), "phoebe_requests_in_flight") {
		t.Fatalf("metrics handler: %d %q", rr.Code, rr.Body.String())
	}
}

// TestMetricsOffWithoutMetricsAddr: no metrics port configured means no
// collectors and an uninstrumented proxy.
func TestMetricsOffWithoutMetricsAddr(t *testing.T) {
	srv := New(&config.Settings{ListenAddr: ":0"}, logging.New(logging.ERROR), &recordingEmitter{})
	if srv.metrics != nil {
		t.Fatal("metrics built without a metrics port")
	}
	var rec *requestRecord
	rec.bind(identity.Identity{ResourceID: "x"}) // nil-safe: no panic
	rec.observeWake(0, wakeOutcomeOK)
}

// TestMetricsWakeHoldIsRecorded: a request that finds its model cold is timed
// once on phoebe_wake_hold_seconds under its resource id, then counted as a
// served request.
func TestMetricsWakeHoldIsRecorded(t *testing.T) {
	attempts := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		if attempts == 1 {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"Model not found"}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)

	s := New(&config.Settings{MetricsAddr: ":0"}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithWaker(&fakeWaker{}, time.Second)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hello"}]}`))
	setUpstream(req, up)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r1")
	req.Header.Set(identity.HeaderServedModel, "m")
	req.Header.Set(identity.HeaderServingMode, identity.ServingModeDedicated)
	req.Header.Set(identity.HeaderServingProfile, "disaggregated")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rr.Code, rr.Body.String())
	}
	wake := map[string]string{"resource_id": "r1", "serving_mode": "dedicated", "outcome": wakeOutcomeOK}
	if got := histogramCount(t, s.metrics, "phoebe_wake_hold_seconds", wake); got != 1 {
		t.Fatalf("wake holds for %v = %d, want 1", wake, got)
	}
	served := map[string]string{"resource_id": "r1", "serving_mode": "dedicated", "outcome": outcomeOK}
	if got := histogramCount(t, s.metrics, "phoebe_request_duration_seconds", served); got != 1 {
		t.Fatalf("requests for %v = %d, want 1", served, got)
	}
}

// TestMetricsResourceLabelRejectsMalformedIds: an id that is too long or uses
// characters outside the allowed set counts as "other" and is never stored, so
// a bad edge value can neither bloat a scrape nor use up the cap.
func TestMetricsResourceLabelRejectsMalformedIds(t *testing.T) {
	m := newMetrics()
	for name, id := range map[string]string{
		"65 bytes":     strings.Repeat("a", maxResourceLabelLen+1),
		"10 KB":        strings.Repeat("a", 10*1024),
		"space":        "res 123",
		"newline":      "res\n123",
		"quote":        `res"123`,
		"slash":        "res/123",
		"invalid utf8": "res\xff",
	} {
		if got := m.resourceLabel(id); got != resourceOverflow {
			t.Errorf("%s: label = %q, want %q", name, got, resourceOverflow)
		}
		if _, stored := m.resources[id]; stored {
			t.Errorf("%s: a rejected id was stored", name)
		}
	}
	if len(m.resources) != 0 {
		t.Fatalf("rejected ids used up the cap: %d stored", len(m.resources))
	}
	for _, id := range []string{
		"res-123",
		"0123456789abcdef0123456789abcdef",
		"123e4567-e89b-12d3-a456-426614174000",
		"a_b.c:d-e",
		strings.Repeat("a", maxResourceLabelLen),
	} {
		if got := m.resourceLabel(id); got != id {
			t.Errorf("legitimate id %q became %q", id, got)
		}
	}
}

// serveInstrumented runs handler through instrument with a context the test
// controls. The handler binds res-1 the way handleProxy does.
func serveInstrumented(ctx context.Context, m *metrics, handler http.HandlerFunc) {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(ctx)
	m.instrument(func(w http.ResponseWriter, r *http.Request) {
		recordFrom(r.Context()).bind(identity.Identity{ResourceID: "res-1", ServingMode: identity.ServingModeDedicated})
		handler(w, r)
	})(httptest.NewRecorder(), req)
}

func requestsWithOutcome(t *testing.T, m *metrics, outcome string) int64 {
	t.Helper()
	return histogramCount(t, m, "phoebe_request_duration_seconds",
		map[string]string{"resource_id": "res-1", "serving_mode": identity.ServingModeDedicated, "outcome": outcome})
}

// TestStatusRecorderRecordsOnlyFinalStatus: informational codes are forwarded
// but not recorded; 101 is final; the first final status stays recorded.
func TestStatusRecorderRecordsOnlyFinalStatus(t *testing.T) {
	cases := []struct {
		name    string
		codes   []int
		status  int
		outcome string
	}{
		{"103 then 500", []int{103, 500}, 500, outcomeServerError},
		{"103 then 502", []int{103, 502}, 502, outcomeServerError},
		{"100 and 103 then 200", []int{100, 103, 200}, 200, outcomeOK},
		{"103 alone leaves status unset, which instrument maps to 200", []int{103}, 0, outcomeOK},
		{"101 is final", []int{101}, 101, outcomeOK},
		{"first final status wins", []int{200, 500}, 200, outcomeOK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sw := &statusRecorder{ResponseWriter: httptest.NewRecorder()}
			for _, code := range c.codes {
				sw.WriteHeader(code)
			}
			if sw.status != c.status {
				t.Fatalf("status = %d, want %d", sw.status, c.status)
			}
			if got := requestOutcome(sw.status, false); got != c.outcome {
				t.Fatalf("outcome = %q, want %q", got, c.outcome)
			}
		})
	}
}

// TestInstrumentRecordsFinalStatusAfter103: an upstream's 103 Early Hints
// forwarded through the real proxy path does not hide the 5xx that follows.
func TestInstrumentRecordsFinalStatusAfter103(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusEarlyHints)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	srv := newMetricsTestServer(t)
	sendIdentified(t, srv, upstream, "res-1")

	labels := func(outcome string) map[string]string {
		return map[string]string{"resource_id": "res-1", "serving_mode": identity.ServingModeDedicated, "outcome": outcome}
	}
	if got := histogramCount(t, srv.metrics, "phoebe_request_duration_seconds", labels(outcomeServerError)); got != 1 {
		t.Fatalf("server_error count = %d, want 1", got)
	}
	if got := histogramCount(t, srv.metrics, "phoebe_request_duration_seconds", labels(outcomeOK)); got != -1 {
		t.Fatalf("ok count = %d, want no such series", got)
	}
}

// TestInstrumentRecordsAbortedOnCancelledContext: a request whose context ends
// before the handler returns is aborted, even over a written 502 (abort takes
// precedence over status). An uncancelled 502 stays a server error.
func TestInstrumentRecordsAbortedOnCancelledContext(t *testing.T) {
	cases := []struct {
		name   string
		status int
		cancel bool
		want   string
		absent string
	}{
		{"cancelled 200", http.StatusOK, true, outcomeAborted, outcomeOK},
		{"cancelled 502", http.StatusBadGateway, true, outcomeAborted, outcomeServerError},
		{"uncancelled 502", http.StatusBadGateway, false, outcomeServerError, outcomeAborted},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := newMetrics()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			serveInstrumented(ctx, m, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(c.status)
				if c.cancel {
					cancel()
				}
			})
			if got := requestsWithOutcome(t, m, c.want); got != 1 {
				t.Fatalf("outcome %s count = %d, want 1", c.want, got)
			}
			if got := requestsWithOutcome(t, m, c.absent); got != -1 {
				t.Fatalf("outcome %s count = %d, want no such series", c.absent, got)
			}
		})
	}
}

// TestInstrumentRecordsPanicAbort: a handler that panics with
// http.ErrAbortHandler after sending 200 headers (an upstream failure
// mid-stream, with the client still connected) counts as aborted, is no longer
// in flight, and the panic still reaches net/http.
func TestInstrumentRecordsPanicAbort(t *testing.T) {
	m := newMetrics()
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		serveInstrumented(context.Background(), m, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			panic(http.ErrAbortHandler)
		})
	}()
	if recovered != http.ErrAbortHandler {
		t.Fatalf("recovered %v, want http.ErrAbortHandler", recovered)
	}
	if got := requestsWithOutcome(t, m, outcomeAborted); got != 1 {
		t.Fatalf("aborted count = %d, want 1", got)
	}
	if got := requestsWithOutcome(t, m, outcomeOK); got != -1 {
		t.Fatalf("ok count = %d, want no such series", got)
	}
	var out dto.Metric
	_ = m.inFlight.WithLabelValues("res-1", identity.ServingModeDedicated).Write(&out)
	if v := out.GetGauge().GetValue(); v != 0 {
		t.Fatalf("in flight after the panic = %v, want 0", v)
	}
}

// TestMetricsSeriesStayBoundedPastResourceCap: requests through instrument with
// more distinct ids than the cap leave every phoebe_* family with at most the
// cap plus "other" and "unresolved" as resource_id values, and later ids appear
// only as "other".
func TestMetricsSeriesStayBoundedPastResourceCap(t *testing.T) {
	m := newMetrics()
	for i := 0; i < maxMetricResources+50; i++ {
		id := identity.Identity{ResourceID: fmt.Sprintf("r%d", i), ServingMode: identity.ServingModeDedicated}
		m.instrument(func(_ http.ResponseWriter, r *http.Request) {
			rec := recordFrom(r.Context())
			rec.bind(id)
			rec.observeWake(time.Millisecond, wakeOutcomeOK)
		})(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	}
	// A request refused at the gate never binds.
	m.instrument(func(http.ResponseWriter, *http.Request) {})(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))

	pastCap := fmt.Sprintf("r%d", maxMetricResources+10)
	fams := phoebeFamilies(t, m)
	for _, name := range []string{"phoebe_request_duration_seconds", "phoebe_wake_hold_seconds", "phoebe_requests_in_flight"} {
		f, ok := fams[name]
		if !ok {
			t.Fatalf("metric family %s missing", name)
		}
		ids := map[string]bool{}
		for _, s := range f.GetMetric() {
			ids[labelMap(s)["resource_id"]] = true
		}
		if len(ids) > maxMetricResources+2 {
			t.Errorf("%s has %d resource ids, want at most %d", name, len(ids), maxMetricResources+2)
		}
		if !ids[resourceOverflow] {
			t.Errorf("%s has no %q series", name, resourceOverflow)
		}
		if ids[pastCap] {
			t.Errorf("%s has a series for %s, which is past the cap", name, pastCap)
		}
	}
}

// exposedSeries counts the lines a family exposes: a histogram exposes its
// finite buckets plus +Inf, _sum and _count; a gauge exposes one line.
func exposedSeries(f *dto.MetricFamily) int {
	n := 0
	for _, s := range f.GetMetric() {
		if h := s.GetHistogram(); h != nil {
			n += len(h.GetBucket()) + 3
			continue
		}
		n++
	}
	return n
}

// TestMetricsTotalSeriesStayWithinDocumentedBound drives every label combination
// past the cap and pins the worst-case arithmetic in the comment on
// maxMetricResources. Changing a bucket list, an outcome or the cap fails this
// until that comment is updated.
func TestMetricsTotalSeriesStayWithinDocumentedBound(t *testing.T) {
	const documentedWorstCase = 359934

	modes := []string{identity.ServingModeShared, identity.ServingModeDedicated, ""}
	requestOutcomes := []string{outcomeOK, outcomeClientError, outcomeServerError, outcomeAborted}
	wakeOutcomes := []string{
		wakeOutcomeOK, wakeOutcomeUnavailable, wakeOutcomeStatusUnknown, wakeOutcomeNotReady,
		wakeOutcomeTimeout, wakeOutcomeCanceled, wakeOutcomeError,
	}
	ids := []string{""} // resourceUnresolved
	for i := 0; i < maxMetricResources+10; i++ {
		ids = append(ids, fmt.Sprintf("r%d", i))
	}

	m := newMetrics()
	for _, id := range ids {
		for _, mode := range modes {
			rec := &requestRecord{m: m}
			rec.bind(identity.Identity{ResourceID: id, ServingMode: mode})
			resource, label := rec.labels()
			for _, outcome := range requestOutcomes {
				m.requestDuration.WithLabelValues(resource, label, outcome).Observe(0)
			}
			for _, outcome := range wakeOutcomes {
				rec.observeWake(0, outcome)
			}
		}
	}

	labelIDs := maxMetricResources + 2 // resourceOverflow and resourceUnresolved
	bound := labelIDs*len(modes)*len(requestOutcomes)*(len(requestDurationBuckets)+3) +
		labelIDs*len(modes)*len(wakeOutcomes)*(len(wakeHoldBuckets)+3) +
		labelIDs*len(modes)
	if bound != documentedWorstCase {
		t.Fatalf("computed bound %d differs from the %d in the comment on maxMetricResources; update the comment", bound, documentedWorstCase)
	}

	total, overflowSeen := 0, false
	for _, f := range phoebeFamilies(t, m) {
		total += exposedSeries(f)
		for _, s := range f.GetMetric() {
			overflowSeen = overflowSeen || labelMap(s)["resource_id"] == resourceOverflow
		}
	}
	if total > bound {
		t.Fatalf("%d series exposed, above the bound of %d", total, bound)
	}
	if total != bound {
		t.Fatalf("%d series exposed, want the worst case %d: the driver missed a label combination", total, bound)
	}
	if !overflowSeen {
		t.Fatalf("no %q series", resourceOverflow)
	}
}

type fakeMeteringHealth struct {
	probe      emit.MeteringProbe
	err        error
	walUnknown bool
	poisoned   bool
}

func (f fakeMeteringHealth) WALPending() (uint64, bool) {
	if f.walUnknown {
		return 0, false
	}
	return 7, true
}
func (f fakeMeteringHealth) WALPoisoned() bool       { return f.poisoned }
func (fakeMeteringHealth) WALQuarantinedDirs() int   { return 3 }
func (fakeMeteringHealth) WALSkippedEntries() uint64 { return 4 }
func (fakeMeteringHealth) FloorEvents() uint64       { return 2 }
func (f fakeMeteringHealth) Probe(context.Context) (emit.MeteringProbe, error) {
	return f.probe, f.err
}

// TestMeteringHealthMetricsHaveNoLabels: these families describe the replica and
// Valkey, so they carry no labels at all, and the request families keep theirs.
func TestMeteringHealthMetricsHaveNoLabels(t *testing.T) {
	srv := newMetricsTestServer(t).WithMeteringHealth(fakeMeteringHealth{probe: emit.MeteringProbe{
		StreamEntries: 10, Undelivered: 3, UsedMemory: 100, MaxMemory: 200,
	}})
	want := map[string]float64{
		"phoebe_metering_wal_pending_events":         7,
		"phoebe_metering_wal_healthy":                1,
		"phoebe_metering_wal_poisoned":               0,
		"phoebe_metering_wal_quarantined_dirs":       3,
		"phoebe_metering_wal_skipped_entries_total":  4,
		"phoebe_metering_floor_events_total":         2,
		"phoebe_metering_valkey_up":                  1,
		"phoebe_metering_stream_entries":             10,
		"phoebe_metering_stream_undelivered_entries": 3,
		"phoebe_valkey_used_memory_bytes":            100,
		"phoebe_valkey_maxmemory_bytes":              200,
	}
	fams := phoebeFamilies(t, srv.metrics)
	for name, value := range want {
		f, ok := fams[name]
		if !ok {
			t.Errorf("%s missing", name)
			continue
		}
		m := f.GetMetric()[0]
		if len(m.GetLabel()) != 0 {
			t.Errorf("%s has labels %v", name, m.GetLabel())
		}
		got := m.GetGauge().GetValue() + m.GetCounter().GetValue()
		if got != value {
			t.Errorf("%s = %v, want %v", name, got, value)
		}
	}
}

// TestMeteringHealthProbeFailureReportsValkeyDown: an unreachable Valkey is
// visible as up=0, the WAL backlog still reports, and the scrape still succeeds.
func TestMeteringHealthProbeFailureReportsValkeyDown(t *testing.T) {
	srv := newMetricsTestServer(t).WithMeteringHealth(fakeMeteringHealth{err: errors.New("down")})
	fams := phoebeFamilies(t, srv.metrics)
	if got := fams["phoebe_metering_valkey_up"].GetMetric()[0].GetGauge().GetValue(); got != 0 {
		t.Fatalf("valkey_up = %v, want 0", got)
	}
	if _, ok := fams["phoebe_metering_stream_entries"]; ok {
		t.Fatal("stream_entries reported from a failed probe")
	}
	if got := fams["phoebe_metering_wal_pending_events"].GetMetric()[0].GetGauge().GetValue(); got != 7 {
		t.Fatalf("wal_pending = %v, want 7", got)
	}
}

// TestMeteringHealthUnknownWALCountIsNotZero: a broken WAL may hold unshipped
// events, so it reports healthy=0 and no pending sample, never a pending of 0 that
// would look like a drained WAL and clear PhoebeMeteringOnWAL.
func TestMeteringHealthUnknownWALCountIsNotZero(t *testing.T) {
	srv := newMetricsTestServer(t).WithMeteringHealth(fakeMeteringHealth{walUnknown: true, poisoned: true})
	fams := phoebeFamilies(t, srv.metrics)
	if f, ok := fams["phoebe_metering_wal_pending_events"]; ok {
		t.Fatalf("pending reported while the count is unknown: %v", f)
	}
	if got := fams["phoebe_metering_wal_healthy"].GetMetric()[0].GetGauge().GetValue(); got != 0 {
		t.Fatalf("wal_healthy = %v, want 0", got)
	}
	if got := fams["phoebe_metering_wal_poisoned"].GetMetric()[0].GetGauge().GetValue(); got != 1 {
		t.Fatalf("wal_poisoned = %v, want 1", got)
	}
}

// TestMeteringWALFamiliesServedOnRealEmitter: the real emitter reaches the scrape,
// not only the fake.
func TestMeteringWALFamiliesServedOnRealEmitter(t *testing.T) {
	cfg := emit.DefaultConfig()
	cfg.WALPath = filepath.Join(t.TempDir(), "wal")
	cfg.ShipInterval = time.Hour
	em, err := emit.New(cfg, logging.New(logging.ERROR), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = em.Close(context.Background()) })
	fams := phoebeFamilies(t, newMetricsTestServer(t).WithMeteringHealth(em).metrics)
	for name, want := range map[string]float64{
		"phoebe_metering_wal_pending_events":        0,
		"phoebe_metering_wal_healthy":               1,
		"phoebe_metering_wal_poisoned":              0,
		"phoebe_metering_wal_quarantined_dirs":      0,
		"phoebe_metering_wal_skipped_entries_total": 0,
	} {
		f, ok := fams[name]
		if !ok {
			t.Errorf("%s missing", name)
			continue
		}
		m := f.GetMetric()[0]
		if got := m.GetGauge().GetValue() + m.GetCounter().GetValue(); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}
