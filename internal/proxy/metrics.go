package proxy

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/saturncloud/phoebe/internal/identity"
)

// Prometheus metrics (Hugo, 2026-10-31: "open phoebe to prom").
//
// POSTURE. /metrics is served on its own port (settings metricsPort), never on
// the proxy port, so nothing that reaches the proxy through Traefik can read
// it; the chart admits that port only from the Prometheus pods. The label set
// is fixed to resource_id, serving_mode and outcome. Org, user, token, model
// and request-body values are never labels: a scrape must not become a list of
// who is calling what. TestMetricsLabelSetIsFixed pins this.
//
// CARDINALITY. Every label value is bounded. serving_mode and outcome come
// from fixed enums. resource_id is set only after the billing-identity gate,
// so it is the trusted value Atlas stamped (or the gateway resolved), never a
// client string; a request refused before that gate is counted under
// resourceUnresolved. Distinct resource ids are capped per process at
// maxMetricResources; later ids fold into resourceOverflow, so a pod that
// outlives many endpoint creations cannot grow without bound. An id that does
// not look like a resource id (too long, or outside the allowed characters)
// also counts as resourceOverflow and does not use up the cap, so the bound
// does not depend entirely on the edge stripping the header.

const (
	metricsNamespace = "phoebe"

	resourceUnresolved = "unresolved"
	resourceOverflow   = "other"
	servingModeUnknown = "unknown"

	// maxMetricResources bounds distinct resource ids, not series. Each id
	// multiplies by the other labels and by the lines one histogram exposes.
	// With ids = maxMetricResources + 2 (resourceOverflow, resourceUnresolved),
	// the worst case per process is:
	//
	//	request_duration: ids x 3 modes x 4 outcomes x 28 (25 buckets, +Inf, _sum, _count) = 168,672
	//	wake_hold:        ids x 3 modes x 7 outcomes x 18 (15 buckets, +Inf, _sum, _count) = 189,756
	//	in_flight:        ids x 3 modes                                                    =   1,506
	//	total                                                                              = 359,934
	//
	// An id has one serving mode in practice, and series exist only for label
	// combinations a request actually produced. A typical id (ok and client_error
	// requests, wake holds that succeed) exposes 75 series, in line with the
	// roughly 110 per router accepted for Traefik in saturn-k8s#1166, so about
	// 37,500 per pod at the cap. Endpoints past the cap have no per-endpoint
	// phoebe series until the pod restarts; they still count under "other".
	// TestMetricsTotalSeriesStayWithinDocumentedBound pins this arithmetic.
	maxMetricResources = 500

	// maxResourceLabelLen matches the VARCHAR(64) resource_id column in
	// billing_event. Atlas's ids are shorter (Unicode(32)).
	maxResourceLabelLen = 64
)

// outcome label values for phoebe_request_duration_seconds.
const (
	outcomeOK          = "ok"           // 1xx-3xx
	outcomeClientError = "client_error" // 4xx
	outcomeServerError = "server_error" // 5xx
	outcomeAborted     = "aborted"      // the response did not finish: the client went away, or the handler aborted mid-stream
)

// outcome label values for phoebe_wake_hold_seconds.
const (
	wakeOutcomeOK            = "ok"
	wakeOutcomeUnavailable   = "unavailable"
	wakeOutcomeStatusUnknown = "status_unknown"
	wakeOutcomeNotReady      = "not_ready"
	wakeOutcomeTimeout       = "timeout"
	wakeOutcomeCanceled      = "canceled"
	wakeOutcomeError         = "error"
)

// requestDurationBuckets match Traefik's router buckets (saturn-k8s#1166): 25
// log-spaced buckets, 0.05-600 s, so phoebe's and the edge's latency charts
// line up bucket for bucket.
var requestDurationBuckets = []float64{
	0.05, 0.0739, 0.109, 0.162, 0.239, 0.354, 0.523, 0.774, 1.14, 1.69, 2.5, 3.7,
	5.48, 8.1, 12, 17.7, 26.2, 38.8, 57.3, 84.8, 125, 185, 274, 406, 600,
}

// wakeHoldBuckets cover a cold start, bounded by wake.timeout (default 300 s).
// Fewer buckets than requests: a hold is never sub-second in practice.
var wakeHoldBuckets = []float64{0.5, 1, 2, 4, 8, 15, 30, 60, 90, 120, 180, 240, 300, 450, 600}

var (
	requestLabels  = []string{"resource_id", "serving_mode", "outcome"}
	inFlightLabels = []string{"resource_id", "serving_mode"}
)

// metrics holds phoebe's collectors on a private registry, so only what is
// declared here (plus the standard Go and process collectors, which carry no
// tenant data) is ever exposed.
type metrics struct {
	registry        *prometheus.Registry
	requestDuration *prometheus.HistogramVec
	wakeHold        *prometheus.HistogramVec
	inFlight        *prometheus.GaugeVec

	mu        sync.Mutex
	resources map[string]struct{}
}

func newMetrics() *metrics {
	m := &metrics{
		registry: prometheus.NewRegistry(),
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "request_duration_seconds",
			Help:      "Time from phoebe receiving a proxied request to the end of its response, including any wake hold and the whole stream.",
			Buckets:   requestDurationBuckets,
		}, requestLabels),
		wakeHold: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "wake_hold_seconds",
			Help:      "Time a request waited in phoebe for a cold model to wake.",
			Buckets:   wakeHoldBuckets,
		}, requestLabels),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "requests_in_flight",
			Help:      "Proxied requests phoebe is currently serving, after the billing-identity gate.",
		}, inFlightLabels),
		resources: map[string]struct{}{},
	}
	m.registry.MustRegister(
		m.requestDuration, m.wakeHold, m.inFlight,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Handler serves the registry. It is mounted only on the metrics listener.
func (m *metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// resourceLabel returns id unchanged while fewer than maxMetricResources
// distinct ids have been seen, and resourceOverflow after that. A malformed id
// is resourceOverflow at once and is never stored, so it cannot use up the cap.
func (m *metrics) resourceLabel(id string) string {
	if id == "" {
		return resourceUnresolved
	}
	if !validResourceLabel(id) {
		return resourceOverflow
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.resources[id]; ok {
		return id
	}
	if len(m.resources) >= maxMetricResources {
		return resourceOverflow
	}
	m.resources[id] = struct{}{}
	return id
}

// validResourceLabel reports whether id fits maxResourceLabelLen and uses only
// [A-Za-z0-9._:-]. This also keeps invalid UTF-8 away from WithLabelValues,
// which panics on it.
func validResourceLabel(id string) bool {
	if len(id) > maxResourceLabelLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == ':', c == '-':
		default:
			return false
		}
	}
	return true
}

func servingModeLabel(mode string) string {
	if identity.ValidServingMode(mode) {
		return mode
	}
	return servingModeUnknown
}

// requestOutcome maps a finished request to its outcome label. An abort takes
// precedence over the status: a 5xx written to a client that has already gone
// counts as aborted, not server_error.
func requestOutcome(status int, aborted bool) string {
	switch {
	case aborted:
		return outcomeAborted
	case status >= 500:
		return outcomeServerError
	case status >= 400:
		return outcomeClientError
	default:
		return outcomeOK
	}
}

// requestRecord is one proxied request's metric state. handleProxy binds the
// labels once the billing-identity gate has passed; until then the request is
// counted as unresolved and is not in flight.
type requestRecord struct {
	m           *metrics
	start       time.Time
	resource    string
	servingMode string
	bound       bool
}

type requestRecordKey struct{}

func recordFrom(ctx context.Context) *requestRecord {
	rec, _ := ctx.Value(requestRecordKey{}).(*requestRecord)
	return rec
}

// bind sets the request's labels from its trusted identity and marks it in
// flight. Safe on a nil record (metrics disabled) and idempotent.
func (rec *requestRecord) bind(id identity.Identity) {
	if rec == nil || rec.bound {
		return
	}
	rec.resource = rec.m.resourceLabel(id.ResourceID)
	rec.servingMode = servingModeLabel(id.ServingMode)
	rec.bound = true
	rec.m.inFlight.WithLabelValues(rec.resource, rec.servingMode).Inc()
}

func (rec *requestRecord) labels() (string, string) {
	if rec.bound {
		return rec.resource, rec.servingMode
	}
	return resourceUnresolved, servingModeUnknown
}

// observeWake records one wake hold. Safe on a nil record.
func (rec *requestRecord) observeWake(d time.Duration, outcome string) {
	if rec == nil {
		return
	}
	resource, mode := rec.labels()
	rec.m.wakeHold.WithLabelValues(resource, mode, outcome).Observe(d.Seconds())
}

// statusRecorder captures the final status code a handler writes. Unwrap lets
// http.ResponseController (which ReverseProxy uses to flush a stream) reach the
// underlying writer, so wrapping never breaks streaming.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(code int) {
	// An informational response (ReverseProxy forwards 100 and 103 from the
	// upstream) precedes the final status, so it is forwarded and not recorded.
	// 101 is final: net/http sends nothing after it.
	if code >= 100 && code < 200 && code != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(code)
		return
	}
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusRecorder) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusRecorder) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		if w.status == 0 {
			w.status = http.StatusOK
		}
		f.Flush()
	}
}

func (w *statusRecorder) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// instrument wraps the proxy handler: it times the whole request, including a
// stream to its end, and records the outcome under the labels handleProxy
// bound.
func (m *metrics) instrument(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &requestRecord{m: m, start: time.Now()}
		sw := &statusRecorder{ResponseWriter: w}
		r = r.WithContext(context.WithValue(r.Context(), requestRecordKey{}, rec))
		// Cleared only if next returns. On a panic (ReverseProxy raises
		// http.ErrAbortHandler when an upstream fails mid-stream) it stays true
		// and the panic keeps propagating to net/http; nothing recovers it here.
		panicked := true
		defer func() {
			resource, mode := rec.labels()
			if rec.bound {
				m.inFlight.WithLabelValues(resource, mode).Dec()
			}
			status := sw.status
			if status == 0 {
				status = http.StatusOK
			}
			outcome := requestOutcome(status, r.Context().Err() != nil || panicked)
			m.requestDuration.WithLabelValues(resource, mode, outcome).Observe(time.Since(rec.start).Seconds())
		}()
		next(sw, r)
		panicked = false
	}
}
