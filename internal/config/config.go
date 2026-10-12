// Package config loads interceptor settings from a YAML file, applying
// defaults then parsing/validating. The load → defaults → unmarshal → parse
// flow mirrors auth-server's util.Settings.
package config

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

// Settings stores configuration for the interceptor.
type Settings struct {
	// --- YAML file settings ---

	Debug bool `yaml:"debug"`

	// Port the interceptor listens on for inbound (post-Traefik) traffic.
	ListenPort int `yaml:"listenPort"`

	// IdleTimeout bounds how long an idle keep-alive connection may stay open
	// BETWEEN requests (http.Server.IdleTimeout). It does not bound a request
	// in flight: a client that stops reading a stream is ended by
	// protection.streamStallTimeout instead.
	IdleTimeoutStr string `yaml:"idleTimeout"`

	// Protection bounds the resources any single request can hold in phoebe.
	// It applies to ALL traffic, shared and dedicated, and is set once for the
	// whole gateway (ruling 2026-10-12): it protects phoebe itself, which every
	// tenant shares, so it is not a per-model or per-tenant setting.
	Protection ProtectionSettings `yaml:"protection"`

	// Emit configures the durable metering emitter (M2). main.go translates
	// these into an emit.Config for the same reason.
	Emit EmitSettings `yaml:"emit"`

	// IOLog configures the M5 I/O-logging subsystem (opt-in body capture).
	// OFF by default — see IOLogSettings. main.go translates these into an
	// iolog.Config + iolog.StaticPolicy, keeping config free of an iolog
	// dependency (same pattern as Emit).
	IOLog IOLogSettings `yaml:"ioLog"`

	// Gateway configures the TF single-host gateway resolution path (requests
	// marked X-Saturn-Gateway resolve their body model= against Atlas's
	// tf_model). OFF by default — a phoebe without it refuses gateway-marked
	// requests fail-closed (503). main.go translates these into the
	// internal/gateway resolver wiring (same pattern as Emit/IOLog).
	Gateway GatewaySettings `yaml:"gateway"`

	// Wake configures shared Kubernetes and dedicated Atlas wake actuation.
	// OFF by default — without it a cold response passes through unchanged.
	// main.go builds each configured actuator independently.
	Wake WakeSettings `yaml:"wake"`

	// Admission configures distributed shared-lane admission. It is deliberately
	// separate from Emit even though both normally use Valkey: metering may fall
	// back to its WAL, while an unavailable admission store temporarily bypasses
	// only fairness/capacity limits for otherwise authorized, valid requests.
	Admission AdmissionSettings `yaml:"admission"`

	// --- Parsed settings (populated by parse) ---

	ListenAddr  string        `yaml:"-"`
	IdleTimeout time.Duration `yaml:"-"`

	// configDir is the directory the settings file was loaded from; relative
	// paths in the YAML are resolved against it.
	configDir string
}

// ProtectionSettings is the YAML shape for gateway-wide resource bounds. Every
// field is optional: absent (zero) selects the shipped default below, and a
// negative value is a startup error. There is deliberately no "off" value —
// each bound protects the shared process, and the defaults are generous enough
// that a well-behaved client never meets them.
type ProtectionSettings struct {
	// MaxRequestBodyBytes caps every request body phoebe reads, on every route.
	MaxRequestBodyBytes int64 `yaml:"maxRequestBodyBytes"`
	// StreamStallTimeoutStr bounds how long one write to the client may make no
	// progress. A client that stops reading mid-response fills its TCP window
	// and the write blocks; without this bound the request (and its engine
	// work, admission lease and buffers) would be held forever.
	StreamStallTimeoutStr string        `yaml:"streamStallTimeout"`
	StreamStallTimeout    time.Duration `yaml:"-"`
	// MaxStreamDurationStr bounds a whole request, from arrival to the last
	// response byte, including any wake hold.
	MaxStreamDurationStr string        `yaml:"maxStreamDuration"`
	MaxStreamDuration    time.Duration `yaml:"-"`
	// MaxResponseBufferBytesPerStream caps the response bytes phoebe holds in
	// memory for one request while it inspects them (an incomplete SSE line,
	// or a whole non-streamed JSON body). The fixed 16 MiB SSE event-line
	// limit still applies inside it. While an SSE line is being assembled it
	// is held twice, once in the capture buffer and once in the usage
	// filter, and both copies count; a value below 32 MiB therefore cuts
	// lines shorter than the 16 MiB event-line limit.
	MaxResponseBufferBytesPerStream int64 `yaml:"maxResponseBufferBytesPerStream"`
	// MaxResponseBufferBytesTotal caps those bytes summed over every request
	// in this phoebe replica.
	MaxResponseBufferBytesTotal int64 `yaml:"maxResponseBufferBytesTotal"`
	// MaxDedicatedColdHoldsPerEndpoint caps, per phoebe replica, how many
	// requests to ONE dedicated endpoint may be held while it starts from
	// zero. The shared tier bounds the same thing with admission's
	// maxColdHolds; dedicated endpoints have no admission.
	MaxDedicatedColdHoldsPerEndpoint int64 `yaml:"maxDedicatedColdHoldsPerEndpoint"`
}

// Shipped protection defaults.
const (
	DefaultMaxRequestBodyBytes              int64 = 64 << 20
	DefaultStreamStallTimeout                     = 2 * time.Minute
	DefaultMaxStreamDuration                      = 60 * time.Minute
	DefaultMaxResponseBufferBytesPerStream  int64 = 64 << 20
	DefaultMaxResponseBufferBytesTotal      int64 = 256 << 20
	DefaultMaxDedicatedColdHoldsPerEndpoint int64 = 32
)

// Effective returns the settings with every absent (zero) field replaced by
// its default. The proxy calls it rather than reading fields directly, so a
// hand-built Settings (tests, embedders) is protected exactly like a loaded
// one.
func (p ProtectionSettings) Effective() ProtectionSettings {
	if p.MaxRequestBodyBytes <= 0 {
		p.MaxRequestBodyBytes = DefaultMaxRequestBodyBytes
	}
	if p.StreamStallTimeout <= 0 {
		p.StreamStallTimeout = DefaultStreamStallTimeout
	}
	if p.MaxStreamDuration <= 0 {
		p.MaxStreamDuration = DefaultMaxStreamDuration
	}
	if p.MaxResponseBufferBytesPerStream <= 0 {
		p.MaxResponseBufferBytesPerStream = DefaultMaxResponseBufferBytesPerStream
	}
	if p.MaxResponseBufferBytesTotal <= 0 {
		p.MaxResponseBufferBytesTotal = DefaultMaxResponseBufferBytesTotal
	}
	if p.MaxDedicatedColdHoldsPerEndpoint <= 0 {
		p.MaxDedicatedColdHoldsPerEndpoint = DefaultMaxDedicatedColdHoldsPerEndpoint
	}
	return p
}

func (p *ProtectionSettings) parse() error {
	for _, item := range []struct {
		name  string
		value int64
	}{
		{"maxRequestBodyBytes", p.MaxRequestBodyBytes},
		{"maxResponseBufferBytesPerStream", p.MaxResponseBufferBytesPerStream},
		{"maxResponseBufferBytesTotal", p.MaxResponseBufferBytesTotal},
		{"maxDedicatedColdHoldsPerEndpoint", p.MaxDedicatedColdHoldsPerEndpoint},
	} {
		if item.value < 0 {
			return fmt.Errorf("protection.%s cannot be negative", item.name)
		}
	}
	for _, item := range []struct {
		name string
		raw  string
		dst  *time.Duration
	}{
		{"streamStallTimeout", p.StreamStallTimeoutStr, &p.StreamStallTimeout},
		{"maxStreamDuration", p.MaxStreamDurationStr, &p.MaxStreamDuration},
	} {
		if item.raw == "" {
			continue
		}
		d, err := time.ParseDuration(item.raw)
		if err != nil || d <= 0 {
			return fmt.Errorf("invalid protection.%s %q (a positive duration such as 2m)", item.name, item.raw)
		}
		*item.dst = d
	}
	eff := p.Effective()
	if eff.MaxResponseBufferBytesPerStream > eff.MaxResponseBufferBytesTotal {
		return fmt.Errorf("protection.maxResponseBufferBytesPerStream (%d) cannot exceed maxResponseBufferBytesTotal (%d)",
			eff.MaxResponseBufferBytesPerStream, eff.MaxResponseBufferBytesTotal)
	}
	return nil
}

// AdmissionLimits is one independently-enforced scope budget. R4 sentinel
// semantics (the Saturn UsageLimit pattern): nil — a YAML null or an absent
// key — means that dimension is unlimited; an explicit 0 is a zero cap that
// blocks every request at that dimension. There is no 0-sentinel for
// unlimited. Window applies to RequestsPerWindow, the three token-throughput
// windows, and WakesPerWindow. Total prompt includes cached and uncached
// prompt tokens; uncached prompt is the subset that required prefill compute.
type AdmissionLimits struct {
	MaxActiveRequests      *int64 `yaml:"maxActiveRequests"`
	MaxConcurrentPrefills  *int64 `yaml:"maxConcurrentPrefills"`
	MaxReservedDecodeSlots *int64 `yaml:"maxReservedDecodeSlots"`
	// MaxPrefillTokens bounds the estimated prompt tokens of requests still in
	// prefill (admitted, no response byte yet). The estimate is the request's
	// message-text characters divided by 4 plus a fixed cost per image part
	// (ruling 2026-10-12); it replaced the old byte-based maxPromptBytes.
	MaxPrefillTokens        *int64 `yaml:"maxPrefillTokens"`
	MaxReservedOutputTokens *int64 `yaml:"maxReservedOutputTokens"`
	// MaxReservedKVTokens bounds the KV-cache tokens admitted requests may
	// need: estimated input plus reserved output tokens, held until the
	// request finishes.
	MaxReservedKVTokens           *int64        `yaml:"maxReservedKvTokens"`
	MaxActiveAdapters             *int64        `yaml:"maxActiveAdapters"`
	RequestsPerWindow             *int64        `yaml:"requestsPerWindow"`
	TotalPromptTokensPerWindow    *int64        `yaml:"totalPromptTokensPerWindow"`
	UncachedPromptTokensPerWindow *int64        `yaml:"uncachedPromptTokensPerWindow"`
	GeneratedTokensPerWindow      *int64        `yaml:"generatedTokensPerWindow"`
	MaxColdHolds                  *int64        `yaml:"maxColdHolds"`
	WakesPerWindow                *int64        `yaml:"wakesPerWindow"`
	WindowStr                     string        `yaml:"window"`
	Window                        time.Duration `yaml:"-"`

	// RemovedMaxPromptBytes catches the renamed maxPromptBytes key. phoebe
	// reads YAML non-strictly, so a leftover value would otherwise be dropped
	// in silence and the operator would believe a limit is in force.
	RemovedMaxPromptBytes *int64 `yaml:"maxPromptBytes"`
}

// AdmissionLane gives an operator-defined scheduling class an isolated protected
// lane. The lane is intentionally a hard partition: unused capacity is not
// borrowed, so another lane can never consume a protected share. Weight is an
// explicit capacity multiplier for the lane, not a claim of request-ordering
// fairness inside Dynamo.
type AdmissionLane struct {
	Weight int64 `yaml:"weight"`
	// DynamoPriority is the trusted soft priority propagated through Dynamo to
	// engines that support per-request scheduling. Higher values are more
	// important. DynamoStrictPriority is the unsigned strict-priority queue band.
	DynamoPriority       int64           `yaml:"dynamoPriority"`
	DynamoStrictPriority int64           `yaml:"dynamoStrictPriority"`
	Limits               AdmissionLimits `yaml:"limits"`
}

// DefaultMaxOutputTokens is the output-token reservation for a shared request
// that declares neither max_tokens nor max_completion_tokens. R10: the
// ratified default (Baseten-aligned).
const DefaultMaxOutputTokens int64 = 4096

// AdmissionSettings is the YAML shape for Saturn-owned HTTP admission.
type AdmissionSettings struct {
	// Enabled turns on the OPERATOR side of admission: the capacity tiers
	// below and the requirement that every shared request carry the trusted
	// envelope (a request without it fails closed with 503). It is not a gate
	// on contract limits: those are enforced whenever an admission store is
	// configured (see Settings.EffectiveAdmission).
	Enabled bool `yaml:"enabled"`
	// ValkeyAddr is the admission store. Empty falls back to emit.valkeyAddr
	// (see Settings.AdmissionStoreAddr); Enabled requires it set explicitly.
	ValkeyAddr             string                   `yaml:"valkeyAddr"`
	KeyPrefix              string                   `yaml:"keyPrefix"`
	LeaseTTLStr            string                   `yaml:"leaseTtl"`
	LeaseTTL               time.Duration            `yaml:"-"`
	DefaultMaxOutputTokens int64                    `yaml:"defaultMaxOutputTokens"`
	Platform               AdmissionLimits          `yaml:"platform"`
	Graph                  AdmissionLimits          `yaml:"graph"`
	Organization           AdmissionLimits          `yaml:"organization"`
	OrganizationModel      AdmissionLimits          `yaml:"organizationModel"`
	Lanes                  map[string]AdmissionLane `yaml:"lanes"`
	OrganizationLanes      map[string]string        `yaml:"organizationLanes"`
}

const minimumAdmissionLeaseTTL = time.Second

// defaultAdmissionLeaseTTL is the lease TTL used when admission.leaseTtl is
// unset, or when no admission store is configured (the value is then unused).
const defaultAdmissionLeaseTTL = 15 * time.Minute

// EmitSettings is the YAML shape for the durable emitter. Mirrors emit.Config
// without importing it.
type EmitSettings struct {
	// ValkeyAddr is the Valkey/Redis address. Empty disables Valkey (WAL-only).
	ValkeyAddr string `yaml:"valkeyAddr"`
	StreamName string `yaml:"streamName"`
	// WALPath is the WAL DIRECTORY (the WAL is a tidwall/wal segment log). A
	// legacy single-file JSONL WAL from a pre-upgrade release found at this
	// exact path is imported on startup and renamed aside to
	// "<walPath>.imported" — do not change the configured path across the
	// upgrade, or the old file's events won't be found.
	WALPath string `yaml:"walPath"`
}

// IOLogSettings is the YAML shape for the M5 I/O-logging subsystem. Mirrors
// iolog.Config + iolog.StaticPolicy without importing them (avoids a
// config→iolog dependency).
//
// FAIL CLOSED: Enabled defaults to false and SampleRate to 0.0, so I/O logging
// captures nothing unless an operator explicitly turns it on. Bodies are
// sensitive; capturing them is always a deliberate opt-in.
type IOLogSettings struct {
	// Enabled is the global kill switch for body capture. Default false.
	Enabled bool `yaml:"enabled"`

	// SampleRate is the fraction of opted-in requests to capture, [0,1].
	// Default 0.0 (capture nothing even when Enabled).
	SampleRate float64 `yaml:"sampleRate"`

	// AllowAuthIDs / AllowGroupIDs are the per-tenant opt-in allowlists. Empty
	// opts in NO ONE (fail-closed) — forgetting the allowlist must not capture
	// every tenant's bodies. For deliberate fleet-wide debug capture, set
	// allowAllTenants: true explicitly.
	AllowAuthIDs  []string `yaml:"allowAuthIds"`
	AllowGroupIDs []string `yaml:"allowGroupIds"`

	// AllowAllTenants is the EXPLICIT fleet-wide opt-in (debug only). An empty
	// allowlist never means "everyone"; this flag must be set to capture across
	// all tenants (still subject to Enabled + SampleRate).
	AllowAllTenants bool `yaml:"allowAllTenants"`

	// DatabaseURL is the Postgres DSN for the io_log store. Required when
	// Enabled is true; ignored otherwise.
	DatabaseURL string `yaml:"databaseUrl"`

	// MaxBodyBytes caps the buffered response-body copy (default 256 KiB if 0).
	MaxBodyBytes int `yaml:"maxBodyBytes"`
}

// GatewaySettings is the YAML shape for the TF gateway resolution path.
//
// FAIL CLOSED: Enabled defaults to false, and enabling without a Namespace is
// a startup error — the namespace is half of every composed upstream
// (<graph>-frontend.<namespace>.svc.cluster.local:<port>), and phoebe never
// guesses a forward target. OPERATING ASSUMPTION (documented, load-bearing):
// every serving graph reachable through this phoebe's gateway host lives in
// this ONE namespace (the platform shared-graph namespace); a tf_model row
// whose graph lives elsewhere fails at dial, never misroutes.
type GatewaySettings struct {
	// Enabled turns gateway resolution on. Default false: gateway-marked
	// requests are refused (503) rather than resolved.
	Enabled bool `yaml:"enabled"`

	// Namespace is the k8s namespace the shared serving graphs' frontend
	// Services live in. REQUIRED when Enabled (fail closed, see above).
	Namespace string `yaml:"namespace"`

	// Port is the graphs' OpenAI-compatible serve port. Default 8000 (vLLM's
	// serve port, the same value Atlas's header-injected upstreams carry).
	Port int `yaml:"port"`

	// LegacyPostgresResolver selects the DEPRECATED direct Atlas-DB (tf_model)
	// resolution path instead of the default k8s-watch registry resolver.
	// Kept for ONE release so installs can roll forward/back across the
	// registry transition; requires databaseUrl (or the DATABASE_URL env) and
	// Atlas-DB reachability from the data plane. Remove after the transition
	// release together with gateway.PGResolver.
	LegacyPostgresResolver bool `yaml:"legacyPostgresResolver"`

	// Kubeconfig is a kubeconfig file path for dev/tests of the registry
	// resolver's watch. Empty (production) uses in-cluster config.
	Kubeconfig string `yaml:"kubeconfig"`

	// DatabaseURL is the Atlas Postgres DSN for tf_model lookups — used ONLY
	// by the deprecated legacyPostgresResolver path (the default registry
	// resolver watches ConfigMaps and needs no database). In legacy mode,
	// empty falls back to the DATABASE_URL env var (Atlas convention,
	// resolved in main); legacy-enabled with NEITHER set is a startup error.
	// Ignored in registry mode.
	DatabaseURL string `yaml:"databaseUrl"`
}

// WakeSettings is the YAML shape for wake-from-zero actuators. Shared graphs
// use the Kubernetes actuator and require gateway.namespace. Dedicated
// disaggregated graphs use Atlas and require atlasURL instead.
type WakeSettings struct {
	// Enabled turns the proactive DGDSA waker on. Default false: requests
	// forward directly without a readiness wait.
	Enabled bool `yaml:"enabled"`

	// Kubeconfig is a kubeconfig file path for dev/tests. Empty (production)
	// uses in-cluster config.
	Kubeconfig string `yaml:"kubeconfig"`

	// AtlasURL is the in-cluster Atlas base URL. Dedicated disaggregated
	// endpoints wake through Atlas so it can validate the resource and restore
	// the persisted prefill and decode counts without broader Kubernetes RBAC.
	AtlasURL string `yaml:"atlasURL"`

	// TimeoutStr bounds how long a single wake may hold the woken request
	// (empty = the proxy default, 300s). THE TRADEOFF: this must exceed the
	// serving stack's real cold start (vLLM's cold reload measured ~2.5min on
	// staging — a budget below it usually makes the eventual inference forward
	// see a cold response), but every second is also how long a doomed wake
	// keeps a client waiting before that one honest forward. Size it to the
	// measured cold start plus headroom, not to impatience.
	TimeoutStr string `yaml:"timeout"`

	// Timeout is the parsed TimeoutStr (0 = proxy default).
	Timeout time.Duration `yaml:"-"`
}

// Load reads, defaults, and parses a settings YAML file.
func Load(settingsFile string) (*Settings, error) {
	s := &Settings{
		Debug:          false,
		ListenPort:     8080,
		IdleTimeoutStr: "10m",
		Emit: EmitSettings{
			StreamName: "phoebe:metering",
			WALPath:    "/var/lib/phoebe/metering-wal.jsonl",
		},
	}

	absFilePath, err := filepath.Abs(settingsFile)
	if err != nil {
		return nil, err
	}
	s.configDir = path.Dir(absFilePath)

	settingsYAML, err := os.ReadFile(absFilePath)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(settingsYAML, s); err != nil {
		return nil, err
	}
	if err := s.parse(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Settings) parse() error {
	var err error

	if s.IdleTimeout, err = time.ParseDuration(s.IdleTimeoutStr); err != nil {
		return fmt.Errorf("invalid idleTimeout: %w", err)
	}

	s.ListenAddr = fmt.Sprintf(":%d", s.ListenPort)

	if err := s.Protection.parse(); err != nil {
		return err
	}

	if err := s.IOLog.parse(); err != nil {
		return err
	}
	if err := s.Gateway.parse(); err != nil {
		return err
	}
	s.Wake.AtlasURL = strings.TrimSpace(s.Wake.AtlasURL)
	// Shared wake needs the graph namespace. Dedicated wake uses Atlas and does
	// not need Kubernetes access, so its configured Atlas URL is independently
	// sufficient to enable the wake dispatcher.
	if s.Wake.Enabled && s.Gateway.Namespace == "" && s.Wake.AtlasURL == "" {
		return fmt.Errorf("wake.enabled=true requires gateway.namespace or wake.atlasURL")
	}
	if s.Wake.TimeoutStr != "" {
		if s.Wake.Timeout, err = time.ParseDuration(s.Wake.TimeoutStr); err != nil {
			return fmt.Errorf("invalid wake.timeout: %w", err)
		}
		if s.Wake.Timeout <= 0 {
			return fmt.Errorf("wake.timeout %q must be positive", s.Wake.TimeoutStr)
		}
	}
	// Emit is already decoded, so AdmissionStoreAddr sees the metering
	// Valkey fallback here.
	if err := s.Admission.parse(s.AdmissionStoreAddr() != ""); err != nil {
		return err
	}
	return nil
}

// parse validates the admission settings. storeConfigured reports whether an
// admission store resolves (admission.valkeyAddr or emit.valkeyAddr): the
// store settings are only in use when it does, so their validation is fatal
// only then.
func (a *AdmissionSettings) parse(storeConfigured bool) error {
	removed := map[string]AdmissionLimits{"platform": a.Platform, "graph": a.Graph,
		"organization": a.Organization, "organizationModel": a.OrganizationModel}
	for name, lane := range a.Lanes {
		removed["lanes."+name+".limits"] = lane.Limits
	}
	for name, l := range removed {
		if l.RemovedMaxPromptBytes != nil {
			return fmt.Errorf("admission.%s.maxPromptBytes was renamed to maxPrefillTokens and is now counted in estimated tokens, not bytes; convert and rename it", name)
		}
	}
	// Trusted lane hints are stamped onto every shared request even while
	// admission is disabled (the proxy overwrites client-supplied Dynamo
	// priority headers from the lane), so hint ranges and the lane map's
	// referential integrity are validated regardless of the rollout switch.
	for name, lane := range a.Lanes {
		if lane.DynamoPriority < -(1<<31) || lane.DynamoPriority > 1<<31-1 {
			return fmt.Errorf("admission.lanes.%s.dynamoPriority must fit in a signed 32-bit integer", name)
		}
		if lane.DynamoStrictPriority < 0 || lane.DynamoStrictPriority > 1<<32-1 {
			return fmt.Errorf("admission.lanes.%s.dynamoStrictPriority must fit in an unsigned 32-bit integer", name)
		}
	}
	if len(a.Lanes) > 0 {
		if _, ok := a.Lanes["default"]; !ok {
			return fmt.Errorf("admission.lanes requires a default lane for unmapped organizations")
		}
	}
	for org, lane := range a.OrganizationLanes {
		if _, ok := a.Lanes[lane]; !ok {
			return fmt.Errorf("admission.organizationLanes.%s names unknown lane %q", org, lane)
		}
	}
	// The store settings are defaulted whether or not admission.enabled is
	// set: contract scopes (the Atlas UsageLimits carried on the trusted
	// envelope) are enforced whenever an admission store is configured, so
	// the key prefix, lease TTL, and output reservation are in use then too
	// (ruling R12 clarification, 2026-10-06: there is no enablement gate for
	// contract limits).
	if a.KeyPrefix == "" {
		a.KeyPrefix = "phoebe:admission"
	}
	if a.LeaseTTLStr == "" {
		a.LeaseTTLStr = "15m"
	}
	if storeConfigured || a.Enabled {
		var err error
		if a.LeaseTTL, err = time.ParseDuration(a.LeaseTTLStr); err != nil || a.LeaseTTL < minimumAdmissionLeaseTTL {
			return fmt.Errorf("invalid admission.leaseTtl %q", a.LeaseTTLStr)
		}
	} else {
		// No store resolves and admission is disabled, so the lease TTL is
		// never used. A leftover value that was ignored before contract
		// limits became store-gated must not stop startup; keep the field
		// non-zero with the default.
		a.LeaseTTL = defaultAdmissionLeaseTTL
	}
	if a.DefaultMaxOutputTokens <= 0 {
		a.DefaultMaxOutputTokens = DefaultMaxOutputTokens
	}
	if !a.Enabled {
		return nil
	}
	if a.ValkeyAddr == "" {
		return fmt.Errorf("admission.enabled=true requires admission.valkeyAddr")
	}
	limits := []struct {
		name  string
		value *AdmissionLimits
	}{
		{"platform", &a.Platform}, {"graph", &a.Graph},
		{"organization", &a.Organization}, {"organizationModel", &a.OrganizationModel},
	}
	for name, lane := range a.Lanes {
		if lane.Weight <= 0 {
			return fmt.Errorf("admission.lanes.%s.weight must be positive", name)
		}
		if err := lane.Limits.parse("admission.lanes." + name); err != nil {
			return err
		}
		a.Lanes[name] = lane
	}
	for _, item := range limits {
		if err := item.value.parse("admission." + item.name); err != nil {
			return err
		}
	}
	if len(a.Lanes) > 0 {
		if err := a.validateLaneShares(); err != nil {
			return err
		}
	}
	return nil
}

// AdmissionStoreAddr is the Valkey address of the distributed admission
// store: admission.valkeyAddr when set, otherwise the install Valkey that
// metering already uses (emit.valkeyAddr). Admission keys live under their own
// prefix, so sharing the metering Valkey is the documented deployment. Empty
// means no store is configured and no limit — contract or operator — can be
// enforced.
func (s *Settings) AdmissionStoreAddr() string {
	if s.Admission.ValkeyAddr != "" {
		return s.Admission.ValkeyAddr
	}
	return s.Emit.ValkeyAddr
}

// EffectiveAdmission returns the settings the distributed admitter runs with,
// and false when no admission store is configured.
//
// Contract scopes — the organization and owner rate limits Atlas stamps on
// the trusted envelope — are enforced whenever a store exists, independent of
// admission.enabled. admission.enabled governs only two operator-side things:
// the operator capacity tiers (platform, graph, organization,
// organizationModel, lanes) and the requirement that EVERY shared request
// carry the trusted envelope. With it false the operator tiers are cleared
// here, so the admitter enforces the contract scopes alone; the lane
// configuration in s.Admission still supplies trusted Dynamo hints.
func (s *Settings) EffectiveAdmission() (AdmissionSettings, bool) {
	cfg := s.Admission
	cfg.ValkeyAddr = s.AdmissionStoreAddr()
	if cfg.ValkeyAddr == "" {
		return cfg, false
	}
	if !cfg.Enabled {
		cfg.Platform = AdmissionLimits{}
		cfg.Graph = AdmissionLimits{}
		cfg.Organization = AdmissionLimits{}
		cfg.OrganizationModel = AdmissionLimits{}
		cfg.Lanes = nil
		cfg.OrganizationLanes = nil
	}
	return cfg, true
}

// LimitValues is the one canonical list of AdmissionLimits limit fields, in a
// fixed order. Every caller that walks the limit fields (lane-share
// validation, Any) goes through it, so a new limit field is added here once.
func LimitValues(l AdmissionLimits) []*int64 {
	return []*int64{l.MaxActiveRequests, l.MaxConcurrentPrefills, l.MaxReservedDecodeSlots, l.MaxPrefillTokens,
		l.MaxReservedOutputTokens, l.MaxReservedKVTokens, l.MaxActiveAdapters, l.RequestsPerWindow,
		l.TotalPromptTokensPerWindow, l.UncachedPromptTokensPerWindow,
		l.GeneratedTokensPerWindow, l.MaxColdHolds, l.WakesPerWindow}
}

// Any reports whether at least one limit field is set (nil means unlimited;
// an explicit zero cap counts as set).
func (l AdmissionLimits) Any() bool {
	for _, v := range LimitValues(l) {
		if v != nil {
			return true
		}
	}
	return false
}

func (a *AdmissionSettings) validateLaneShares() error {
	names := []string{"maxActiveRequests", "maxConcurrentPrefills", "maxReservedDecodeSlots", "maxPrefillTokens",
		"maxReservedOutputTokens", "maxReservedKvTokens", "maxActiveAdapters", "requestsPerWindow",
		"totalPromptTokensPerWindow", "uncachedPromptTokensPerWindow",
		"generatedTokensPerWindow", "maxColdHolds", "wakesPerWindow"}
	platform := LimitValues(a.Platform)
	sums := make([]int64, len(platform))
	for laneName, lane := range a.Lanes {
		for i, v := range LimitValues(lane.Limits) {
			if platform[i] != nil && *platform[i] > 0 && v == nil {
				return fmt.Errorf("admission.lanes.%s.%s must be set when the platform limit is set", laneName, names[i])
			}
			if v == nil {
				continue
			}
			value := *v
			if value > 0 && lane.Weight > (1<<63-1)/value {
				return fmt.Errorf("admission.lanes.%s.%s overflows after weight", laneName, names[i])
			}
			weighted := value * lane.Weight
			if weighted > 0 && sums[i] > (1<<63-1)-weighted {
				return fmt.Errorf("weighted admission lane shares for %s overflow", names[i])
			}
			sums[i] += weighted
		}
	}
	for i, max := range platform {
		if max != nil && *max > 0 && sums[i] > *max {
			return fmt.Errorf("weighted admission lane shares for %s total %d above platform limit %d", names[i], sums[i], *max)
		}
	}
	return nil
}

func (l *AdmissionLimits) parse(name string) error {
	values := []*int64{l.MaxActiveRequests, l.MaxConcurrentPrefills, l.MaxReservedDecodeSlots, l.MaxPrefillTokens, l.MaxReservedOutputTokens,
		l.MaxReservedKVTokens, l.MaxActiveAdapters, l.RequestsPerWindow, l.TotalPromptTokensPerWindow, l.UncachedPromptTokensPerWindow,
		l.GeneratedTokensPerWindow, l.MaxColdHolds, l.WakesPerWindow}
	for _, value := range values {
		if value != nil && *value < 0 {
			return fmt.Errorf("%s limits cannot be negative", name)
		}
	}
	if l.TotalPromptTokensPerWindow != nil && l.UncachedPromptTokensPerWindow != nil &&
		*l.UncachedPromptTokensPerWindow > *l.TotalPromptTokensPerWindow {
		return fmt.Errorf("%s.uncachedPromptTokensPerWindow cannot exceed totalPromptTokensPerWindow", name)
	}
	if l.MaxActiveRequests != nil && l.MaxConcurrentPrefills != nil && *l.MaxConcurrentPrefills > *l.MaxActiveRequests {
		return fmt.Errorf("%s.maxConcurrentPrefills cannot exceed maxActiveRequests", name)
	}
	if l.MaxActiveRequests != nil && l.MaxReservedDecodeSlots != nil && *l.MaxReservedDecodeSlots > *l.MaxActiveRequests {
		return fmt.Errorf("%s.maxReservedDecodeSlots cannot exceed maxActiveRequests", name)
	}
	if l.WindowStr == "" {
		l.WindowStr = "1m"
	}
	var err error
	if l.Window, err = time.ParseDuration(l.WindowStr); err != nil || l.Window <= 0 {
		return fmt.Errorf("invalid %s.window %q", name, l.WindowStr)
	}
	return nil
}

// parse validates the gateway settings and applies the port default. It fails
// closed: an enabled gateway with no namespace is a misconfiguration rejected
// at startup — serving gateway requests would require guessing where the
// graphs live, which phoebe never does. DatabaseURL is deliberately NOT
// required here: the default registry resolver needs no database; only the
// deprecated legacyPostgresResolver path uses it, and that requirement is
// enforced in main after the DATABASE_URL env fallback.
func (g *GatewaySettings) parse() error {
	if !g.Enabled {
		return nil // off: nothing to validate
	}
	if g.Namespace == "" {
		return fmt.Errorf("gateway.enabled=true requires gateway.namespace (the shared-graph namespace; phoebe never guesses a forward target)")
	}
	if g.Port == 0 {
		g.Port = 8000
	}
	if g.Port < 1 || g.Port > 65535 {
		return fmt.Errorf("gateway.port %d out of range [1,65535]", g.Port)
	}
	return nil
}

// parse validates the I/O-logging settings. It fails closed: an enabled logger
// with no DatabaseURL or an out-of-range sample rate is a misconfiguration we
// reject at startup rather than silently capturing nothing (or everything).
func (i *IOLogSettings) parse() error {
	if !i.Enabled {
		return nil // off: nothing to validate
	}
	if i.DatabaseURL == "" {
		return fmt.Errorf("ioLog.enabled=true requires ioLog.databaseUrl")
	}
	if i.SampleRate < 0 || i.SampleRate > 1 {
		return fmt.Errorf("ioLog.sampleRate %.3f out of range [0,1]", i.SampleRate)
	}
	return nil
}
