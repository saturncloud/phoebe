package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/saturncloud/phoebe/internal/config"
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

// TestMetricsResourceLabelIsCapped: distinct resource ids beyond
// maxMetricResources fold into one overflow value, so series stay bounded.
func TestMetricsResourceLabelIsCapped(t *testing.T) {
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
		{"hold deadline", expired, errors.New("wait aborted"), wakeOutcomeTimeout},
		{"other", context.Background(), errors.New("boom"), wakeOutcomeError},
	}
	for _, c := range cases {
		if got := wakeOutcome(c.ctx, c.err); got != c.want {
			t.Errorf("%s: wakeOutcome = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestMetricsNotServedOnProxyPort: the proxy handler has no /metrics route; a
// GET /metrics through it is an ordinary proxied request (here refused at the
// billing gate), never the exposition.
func TestMetricsNotServedOnProxyPort(t *testing.T) {
	srv := newMetricsTestServer(t)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rr.Body)
	if strings.Contains(string(body), "phoebe_") || strings.Contains(string(body), "# HELP") {
		t.Fatalf("the proxy port served metrics: %d %q", rr.Code, body)
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
