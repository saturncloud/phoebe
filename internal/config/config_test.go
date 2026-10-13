package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func writeTemp(t *testing.T, yaml string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "settings.yaml")
	if err := os.WriteFile(p, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadDefaults(t *testing.T) {
	s, err := Load(writeTemp(t, "debug: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !s.Debug {
		t.Fatal("debug not loaded")
	}
	if s.ListenAddr != ":8080" {
		t.Fatalf("ListenAddr = %q, want :8080", s.ListenAddr)
	}
}

// TestLoadIOLogOffByDefault verifies M5 I/O logging is OFF unless configured:
// no ioLog block means Enabled stays false and parse imposes no requirements.
func TestLoadIOLogOffByDefault(t *testing.T) {
	s, err := Load(writeTemp(t, "debug: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.IOLog.Enabled {
		t.Fatal("ioLog must be disabled by default")
	}
	if s.IOLog.SampleRate != 0 {
		t.Fatalf("default sampleRate = %v, want 0", s.IOLog.SampleRate)
	}
}

// TestLoadIOLogEnabledRequiresDatabaseURL verifies the fail-closed validation:
// enabling logging without a DatabaseURL is a startup error, not silent.
func TestLoadIOLogEnabledRequiresDatabaseURL(t *testing.T) {
	_, err := Load(writeTemp(t, "ioLog:\n  enabled: true\n  sampleRate: 0.1\n"))
	if err == nil {
		t.Fatal("expected error: ioLog.enabled without databaseUrl")
	}
}

// TestLoadIOLogRejectsOutOfRangeRate verifies sampleRate is bounded to [0,1].
func TestLoadIOLogRejectsOutOfRangeRate(t *testing.T) {
	_, err := Load(writeTemp(t, `
ioLog:
  enabled: true
  databaseUrl: "postgres://x/y"
  sampleRate: 1.5
`))
	if err == nil {
		t.Fatal("expected error: sampleRate out of [0,1]")
	}
}

// TestLoadIOLogParsesEnabled verifies a full enabled config round-trips.
func TestLoadIOLogParsesEnabled(t *testing.T) {
	s, err := Load(writeTemp(t, `
ioLog:
  enabled: true
  sampleRate: 0.25
  databaseUrl: "postgres://u:p@h:5432/db"
  allowAuthIds: ["auth-a", "auth-b"]
  allowGroupIds: ["grp-1"]
  maxBodyBytes: 4096
`))
	if err != nil {
		t.Fatal(err)
	}
	if !s.IOLog.Enabled || s.IOLog.SampleRate != 0.25 {
		t.Fatalf("ioLog settings wrong: %+v", s.IOLog)
	}
	if s.IOLog.DatabaseURL != "postgres://u:p@h:5432/db" {
		t.Fatalf("databaseUrl = %q", s.IOLog.DatabaseURL)
	}
	if len(s.IOLog.AllowAuthIDs) != 2 || len(s.IOLog.AllowGroupIDs) != 1 {
		t.Fatalf("allowlists wrong: %+v", s.IOLog)
	}
	if s.IOLog.MaxBodyBytes != 4096 {
		t.Fatalf("maxBodyBytes = %d, want 4096", s.IOLog.MaxBodyBytes)
	}
}

// TestLoadGatewayOffByDefault: no gateway block means disabled — and parse
// imposes no requirements (a legacy config file keeps loading unchanged).
func TestLoadGatewayOffByDefault(t *testing.T) {
	s, err := Load(writeTemp(t, "debug: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Gateway.Enabled {
		t.Fatal("gateway must be disabled by default")
	}
}

// TestLoadGatewayEnabledRequiresNamespace: the fail-closed validation —
// enabling the gateway without the shared-graph namespace is a startup error
// (phoebe never guesses a forward target), not a silent misroute.
func TestLoadGatewayEnabledRequiresNamespace(t *testing.T) {
	_, err := Load(writeTemp(t, "gateway:\n  enabled: true\n"))
	if err == nil {
		t.Fatal("expected error: gateway.enabled without namespace")
	}
}

// TestLoadGatewayPortDefaults8000: an enabled gateway without a port takes
// 8000 (vLLM's serve port); an explicit port is honored; out-of-range is
// rejected.
func TestLoadGatewayPortDefaults8000(t *testing.T) {
	s, err := Load(writeTemp(t, "gateway:\n  enabled: true\n  namespace: \"tf-shared\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Gateway.Port != 8000 {
		t.Fatalf("port = %d, want default 8000", s.Gateway.Port)
	}

	s, err = Load(writeTemp(t, "gateway:\n  enabled: true\n  namespace: \"tf-shared\"\n  port: 9000\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Gateway.Port != 9000 {
		t.Fatalf("port = %d, want 9000", s.Gateway.Port)
	}

	if _, err := Load(writeTemp(t, "gateway:\n  enabled: true\n  namespace: \"tf-shared\"\n  port: 70000\n")); err == nil {
		t.Fatal("expected error: gateway.port out of range")
	}
}

// TestLoadGatewayParsesEnabled: a full gateway block round-trips.
func TestLoadGatewayParsesEnabled(t *testing.T) {
	s, err := Load(writeTemp(t, `
gateway:
  enabled: true
  namespace: "tf-shared"
  port: 8000
  databaseUrl: "postgres://u:p@h:5432/atlas"
`))
	if err != nil {
		t.Fatal(err)
	}
	if !s.Gateway.Enabled || s.Gateway.Namespace != "tf-shared" || s.Gateway.DatabaseURL != "postgres://u:p@h:5432/atlas" {
		t.Fatalf("gateway settings wrong: %+v", s.Gateway)
	}
}

// TestLoadGatewayRegistryIsDefault: an enabled gateway with no resolver
// selection uses the registry resolver (legacy flag false) and needs NO
// databaseUrl — the registry watches ConfigMaps, not Postgres.
func TestLoadGatewayRegistryIsDefault(t *testing.T) {
	s, err := Load(writeTemp(t, "gateway:\n  enabled: true\n  namespace: \"tf-shared\"\n"))
	if err != nil {
		t.Fatalf("registry-mode gateway without databaseUrl must load: %v", err)
	}
	if s.Gateway.LegacyPostgresResolver {
		t.Fatal("legacyPostgresResolver must default false (registry is the default)")
	}
	if s.Gateway.DatabaseURL != "" {
		t.Fatalf("databaseUrl = %q, want empty (not required in registry mode)", s.Gateway.DatabaseURL)
	}
}

// TestLoadGatewayLegacyResolverFlag: the deprecated PG path stays selectable
// for one release behind the explicit flag (rollback seam); databaseUrl is
// still not a PARSE requirement (main enforces it after the DATABASE_URL env
// fallback, as before).
func TestLoadGatewayLegacyResolverFlag(t *testing.T) {
	s, err := Load(writeTemp(t, `
gateway:
  enabled: true
  namespace: "tf-shared"
  legacyPostgresResolver: true
  databaseUrl: "postgres://u:p@h:5432/atlas"
`))
	if err != nil {
		t.Fatal(err)
	}
	if !s.Gateway.LegacyPostgresResolver || s.Gateway.DatabaseURL == "" {
		t.Fatalf("legacy settings wrong: %+v", s.Gateway)
	}
}

// TestLoadWakeOffByDefault: no wake block means disabled, no requirements.
func TestLoadWakeOffByDefault(t *testing.T) {
	s, err := Load(writeTemp(t, "debug: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Wake.Enabled {
		t.Fatal("wake must be disabled by default")
	}
}

// TestLoadWakeEnabledRequiresGatewayNamespace: shared wake patches DGDSAs in
// the gateway namespace. With no Atlas URL selecting dedicated-only wake,
// enabling wake without a namespace is a startup error.
func TestLoadWakeEnabledRequiresGatewayNamespace(t *testing.T) {
	if _, err := Load(writeTemp(t, "wake:\n  enabled: true\n")); err == nil {
		t.Fatal("expected error: wake.enabled without gateway.namespace")
	}
	s, err := Load(writeTemp(t, "wake:\n  enabled: true\ngateway:\n  namespace: \"tf-shared\"\n"))
	if err != nil {
		t.Fatalf("wake with gateway.namespace (gateway disabled) must load: %v", err)
	}
	if !s.Wake.Enabled || s.Gateway.Enabled {
		t.Fatalf("settings wrong: wake=%+v gateway=%+v", s.Wake, s.Gateway)
	}
}

func TestLoadDedicatedWakeRequiresOnlyAtlasURL(t *testing.T) {
	s, err := Load(writeTemp(t, "wake:\n  enabled: true\n  atlasURL: http://atlas.saturn:8888\n"))
	if err != nil {
		t.Fatalf("dedicated Atlas wake must not require shared gateway configuration: %v", err)
	}
	if !s.Wake.Enabled || s.Wake.AtlasURL != "http://atlas.saturn:8888" {
		t.Fatalf("settings wrong: wake=%+v", s.Wake)
	}
}

func TestLoadDedicatedWakeRejectsWhitespaceOnlyAtlasURL(t *testing.T) {
	if _, err := Load(writeTemp(t, "wake:\n  enabled: true\n  atlasURL: \"   \"\n")); err == nil {
		t.Fatal("expected whitespace-only wake.atlasURL to fail without gateway.namespace")
	}
}

// TestLoadWakeTimeout: wake.timeout parses to a duration; empty means "proxy
// default" (0); garbage and non-positive values are startup errors.
func TestLoadWakeTimeout(t *testing.T) {
	base := "gateway:\n  namespace: \"tf-shared\"\nwake:\n  enabled: true\n"

	s, err := Load(writeTemp(t, base))
	if err != nil {
		t.Fatal(err)
	}
	if s.Wake.Timeout != 0 {
		t.Fatalf("unset wake.timeout = %v, want 0 (proxy default)", s.Wake.Timeout)
	}

	s, err = Load(writeTemp(t, base+"  timeout: \"7m\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Wake.Timeout != 7*time.Minute {
		t.Fatalf("wake.timeout = %v, want 7m", s.Wake.Timeout)
	}

	if _, err := Load(writeTemp(t, base+"  timeout: \"soon\"\n")); err == nil {
		t.Fatal("expected error: unparseable wake.timeout")
	}
	if _, err := Load(writeTemp(t, base+"  timeout: \"-30s\"\n")); err == nil {
		t.Fatal("expected error: non-positive wake.timeout")
	}
}

func TestLoadEmitSettings(t *testing.T) {
	s, err := Load(writeTemp(t, `
emit:
  valkeyAddr: "valkey:6379"
  streamName: "custom:stream"
`))
	if err != nil {
		t.Fatal(err)
	}
	if s.Emit.ValkeyAddr != "valkey:6379" || s.Emit.StreamName != "custom:stream" {
		t.Fatalf("emit settings wrong: %+v", s.Emit)
	}
}

func TestLoadAdmissionFailClosedAndDefaults(t *testing.T) {
	if _, err := Load(writeTemp(t, "admission:\n  enabled: true\n")); err == nil {
		t.Fatal("enabled admission without Valkey must fail startup")
	}
	s, err := Load(writeTemp(t, `
admission:
  enabled: true
  valkeyAddr: "valkey:6379"
  platform:
    maxActiveRequests: 10
  lanes:
    default:
      weight: 1
      limits:
        maxActiveRequests: 4
    protected:
      weight: 2
      dynamoPriority: 17
      dynamoStrictPriority: 3
      limits:
        maxActiveRequests: 2
        requestsPerWindow: 5
        window: "30s"
  organizationLanes:
    org-a: protected
`))
	if err != nil {
		t.Fatal(err)
	}
	if s.Admission.LeaseTTL != 15*time.Minute || s.Admission.DefaultMaxOutputTokens != DefaultMaxOutputTokens {
		t.Fatalf("admission defaults wrong: %+v", s.Admission)
	}
	if got := s.Admission.Lanes["protected"].Limits.Window; got != 30*time.Second {
		t.Fatalf("lane window=%s, want 30s", got)
	}
	if got := s.Admission.Lanes["protected"]; got.DynamoPriority != 17 || got.DynamoStrictPriority != 3 {
		t.Fatalf("protected Dynamo hints wrong: %+v", got)
	}
}

// R4 sentinel semantics: YAML null or an absent key decodes to nil = unlimited;
// an explicit 0 decodes to a zero cap that blocks every request. Without the
// pointer conversion a chart-side null would decode to 0 = zero cap and block
// everything. Table-driven over each shape.
func TestLoadAdmissionLimitsNullableSentinel(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want map[string]*int64
	}{
		{
			name: "absent keys are nil",
			yaml: "admission:\n  enabled: true\n  valkeyAddr: v\n  platform:\n    maxPrefillTokens: 1024\n",
			want: map[string]*int64{"MaxActiveRequests": nil, "MaxPrefillTokens": int64ptr(1024)},
		},
		{
			name: "explicit null is nil",
			yaml: "admission:\n  enabled: true\n  valkeyAddr: v\n  platform:\n    maxActiveRequests: null\n    maxPrefillTokens: 1024\n",
			want: map[string]*int64{"MaxActiveRequests": nil, "MaxPrefillTokens": int64ptr(1024)},
		},
		{
			name: "explicit zero is a zero cap",
			yaml: "admission:\n  enabled: true\n  valkeyAddr: v\n  platform:\n    maxActiveRequests: 0\n    maxPrefillTokens: 0\n",
			want: map[string]*int64{"MaxActiveRequests": int64ptr(0), "MaxPrefillTokens": int64ptr(0)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Load(writeTemp(t, tc.yaml))
			if err != nil {
				t.Fatal(err)
			}
			for field, want := range tc.want {
				got := fieldValue(s.Admission.Platform, field)
				if (got == nil) != (want == nil) || (got != nil && *got != *want) {
					t.Fatalf("platform.%s = %v, want %v", field, got, want)
				}
			}
		})
	}

	// A zero cap on one scope must not constrain the others, and a zero-capped
	// scope must still load: zero caps are enforced at admission, not at parse.
	t.Run("zero cap round-trips per scope", func(t *testing.T) {
		s, err := Load(writeTemp(t, `
admission:
  enabled: true
  valkeyAddr: v
  platform:
    maxActiveRequests: 0
  organization:
    maxActiveRequests: 7
`))
		if err != nil {
			t.Fatal(err)
		}
		if got := s.Admission.Platform.MaxActiveRequests; got == nil || *got != 0 {
			t.Fatalf("platform.maxActiveRequests = %v, want explicit zero cap", got)
		}
		if got := s.Admission.Organization.MaxActiveRequests; got == nil || *got != 7 {
			t.Fatalf("organization.maxActiveRequests = %v, want 7", got)
		}
	})
}

func int64ptr(v int64) *int64 { return &v }

func fieldValue(l AdmissionLimits, name string) *int64 {
	switch name {
	case "MaxActiveRequests":
		return l.MaxActiveRequests
	case "MaxPrefillTokens":
		return l.MaxPrefillTokens
	}
	return nil
}

func TestLoadAdmissionRejectsInvalidPolicy(t *testing.T) {
	tests := []string{
		"admission:\n  enabled: true\n  valkeyAddr: v\n  leaseTtl: 999ms\n",
		"admission:\n  enabled: true\n  valkeyAddr: v\n  platform:\n    maxActiveRequests: -1\n",
		"admission:\n  enabled: true\n  valkeyAddr: v\n  lanes:\n    bad:\n      weight: 0\n",
		"admission:\n  enabled: true\n  valkeyAddr: v\n  lanes:\n    default:\n      weight: 1\n      dynamoPriority: 2147483648\n",
		"admission:\n  enabled: true\n  valkeyAddr: v\n  lanes:\n    default:\n      weight: 1\n      dynamoStrictPriority: -1\n",
		"admission:\n  enabled: true\n  valkeyAddr: v\n  organizationLanes:\n    org-a: missing\n",
		"admission:\n  enabled: true\n  valkeyAddr: v\n  platform:\n    totalPromptTokensPerWindow: 10\n    uncachedPromptTokensPerWindow: 11\n",
		"admission:\n  enabled: true\n  valkeyAddr: v\n  platform:\n    maxActiveRequests: 10\n    maxConcurrentPrefills: 11\n",
		"admission:\n  enabled: true\n  valkeyAddr: v\n  platform:\n    maxActiveRequests: 10\n    maxReservedDecodeSlots: 11\n",
		// Lane hints are stamped on shared requests even while admission is
		// disabled, so hint ranges and lane-map integrity validate regardless.
		"admission:\n  enabled: false\n  lanes:\n    default:\n      weight: 1\n      dynamoStrictPriority: -1\n",
		"admission:\n  enabled: false\n  lanes:\n    default:\n      weight: 1\n      dynamoPriority: 2147483648\n",
		"admission:\n  enabled: false\n  organizationLanes:\n    org-a: missing\n",
		"admission:\n  enabled: false\n  lanes:\n    gold:\n      weight: 1\n",
		// Weighted lane shares above the platform limit.
		"admission:\n  enabled: true\n  valkeyAddr: v\n  platform:\n    maxActiveRequests: 2\n  lanes:\n    default:\n      weight: 1\n      limits:\n        maxActiveRequests: 2\n    gold:\n      weight: 1\n      limits:\n        maxActiveRequests: 1\n",
		// weight*value int64 overflow.
		"admission:\n  enabled: true\n  valkeyAddr: v\n  lanes:\n    default:\n      weight: 9223372036854775807\n      limits:\n        maxActiveRequests: 2\n",
		// The platform sets a dimension the lane omits.
		"admission:\n  enabled: true\n  valkeyAddr: v\n  platform:\n    maxActiveRequests: 10\n  lanes:\n    default:\n      weight: 1\n      limits:\n        maxPrefillTokens: 1024\n",
		// Individually valid weighted shares whose sum overflows int64.
		"admission:\n  enabled: true\n  valkeyAddr: v\n  lanes:\n    default:\n      weight: 4611686018427387904\n      limits:\n        maxActiveRequests: 1\n    gold:\n      weight: 4611686018427387904\n      limits:\n        maxActiveRequests: 1\n",
	}
	for _, body := range tests {
		if _, err := Load(writeTemp(t, body)); err == nil {
			t.Fatalf("expected invalid policy rejection for:\n%s", body)
		}
	}
}

// The leaseTtl floor is inclusive: 999ms is rejected above, but exactly one
// second (the minimum) must load.
func TestLoadAdmissionLeaseTTLInclusiveFloor(t *testing.T) {
	s, err := Load(writeTemp(t, "admission:\n  enabled: true\n  valkeyAddr: v\n  leaseTtl: 1s\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Admission.LeaseTTL != time.Second {
		t.Fatalf("LeaseTTL=%s, want 1s", s.Admission.LeaseTTL)
	}
}

// While admission is disabled, valid lanes must still load (weight, lane-share,
// leaseTtl only when no admission store resolves, and valkeyAddr validation
// stay behind the Enabled gate).
func TestLoadAdmissionDisabledWithValidLanes(t *testing.T) {
	s, err := Load(writeTemp(t, "admission:\n  enabled: false\n  lanes:\n    default:\n      weight: 0\n      dynamoPriority: 5\n      dynamoStrictPriority: 2\n  organizationLanes:\n    org-a: default\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Admission.Lanes["default"]; got.DynamoPriority != 5 || got.DynamoStrictPriority != 2 {
		t.Fatalf("disabled-admission lane hints wrong: %+v", got)
	}
}

// Contract limits have no enablement gate (ruling R12 clarification,
// 2026-10-06): the admission store resolves from admission.valkeyAddr, else
// from the install Valkey metering uses, whether or not admission.enabled is
// set. admission.enabled=false clears only the operator capacity tiers.
func TestEffectiveAdmissionStoreAndTiers(t *testing.T) {
	t.Run("disabled, store from emit.valkeyAddr, operator tiers cleared", func(t *testing.T) {
		s, err := Load(writeTemp(t, `
emit:
  valkeyAddr: "valkey:6379"
admission:
  enabled: false
  organization:
    requestsPerWindow: 30
  lanes:
    default:
      weight: 1
      dynamoPriority: 5
  organizationLanes:
    org-a: default
`))
		if err != nil {
			t.Fatal(err)
		}
		cfg, ok := s.EffectiveAdmission()
		if !ok || cfg.ValkeyAddr != "valkey:6379" {
			t.Fatalf("store=(%q, %v), want the metering Valkey", cfg.ValkeyAddr, ok)
		}
		if cfg.Organization.RequestsPerWindow != nil || cfg.Lanes != nil || cfg.OrganizationLanes != nil {
			t.Fatalf("operator tiers survived admission.enabled=false: %+v", cfg)
		}
		if cfg.KeyPrefix != "phoebe:admission" || cfg.LeaseTTL != 15*time.Minute || cfg.DefaultMaxOutputTokens != DefaultMaxOutputTokens || DefaultMaxOutputTokens != 4096 {
			t.Fatalf("store defaults not applied with admission disabled: %+v", cfg)
		}
		// The raw settings keep the lanes: they still supply trusted Dynamo hints.
		if s.Admission.Lanes["default"].DynamoPriority != 5 {
			t.Fatalf("clearing the effective tiers mutated the loaded lanes: %+v", s.Admission.Lanes)
		}
	})
	t.Run("explicit admission.valkeyAddr wins", func(t *testing.T) {
		s, err := Load(writeTemp(t, "emit:\n  valkeyAddr: metering:6379\nadmission:\n  valkeyAddr: admission:6379\n"))
		if err != nil {
			t.Fatal(err)
		}
		if cfg, ok := s.EffectiveAdmission(); !ok || cfg.ValkeyAddr != "admission:6379" {
			t.Fatalf("store=(%q, %v), want admission:6379", cfg.ValkeyAddr, ok)
		}
	})
	t.Run("enabled keeps operator tiers", func(t *testing.T) {
		s, err := Load(writeTemp(t, "admission:\n  enabled: true\n  valkeyAddr: v\n  organization:\n    requestsPerWindow: 30\n"))
		if err != nil {
			t.Fatal(err)
		}
		cfg, ok := s.EffectiveAdmission()
		if !ok || cfg.Organization.RequestsPerWindow == nil || *cfg.Organization.RequestsPerWindow != 30 {
			t.Fatalf("operator tier lost with admission enabled: %+v", cfg.Organization)
		}
	})
	t.Run("no Valkey at all means no store", func(t *testing.T) {
		s, err := Load(writeTemp(t, "debug: false\n"))
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := s.EffectiveAdmission(); ok {
			t.Fatal("reported an admission store with no Valkey configured")
		}
	})
}

// admission.leaseTtl is validated only when it is used: when an admission
// store resolves (admission.valkeyAddr, else emit.valkeyAddr) or admission is
// enabled. A leftover invalid value on an install with no store and admission
// disabled was ignored before contract limits became store-gated, and must
// still load instead of crash-looping the interceptor.
func TestAdmissionLeaseTTLValidatedOnlyWhenStoreConfigured(t *testing.T) {
	loads := []struct{ name, body string }{
		{"disabled, no store, 0s", "admission:\n  enabled: false\n  leaseTtl: 0s\n"},
		{"disabled, no store, 500ms", "admission:\n  enabled: false\n  leaseTtl: 500ms\n"},
		{"disabled, no store, unparsable", "admission:\n  enabled: false\n  leaseTtl: \"15 m\"\n"},
	}
	for _, tc := range loads {
		t.Run(tc.name+" loads", func(t *testing.T) {
			s, err := Load(writeTemp(t, tc.body))
			if err != nil {
				t.Fatalf("unused leaseTtl stopped startup: %v", err)
			}
			if s.Admission.LeaseTTL != 15*time.Minute {
				t.Fatalf("LeaseTTL=%s, want the 15m default when no store is configured", s.Admission.LeaseTTL)
			}
			if _, ok := s.EffectiveAdmission(); ok {
				t.Fatal("reported an admission store with no Valkey configured")
			}
		})
	}
	fails := []struct{ name, body string }{
		{"disabled, emit.valkeyAddr store, 0s", "emit:\n  valkeyAddr: metering:6379\nadmission:\n  enabled: false\n  leaseTtl: 0s\n"},
		{"disabled, emit.valkeyAddr store, 500ms", "emit:\n  valkeyAddr: metering:6379\nadmission:\n  enabled: false\n  leaseTtl: 500ms\n"},
		{"disabled, admission.valkeyAddr store, unitless 15", "admission:\n  enabled: false\n  valkeyAddr: admission:6379\n  leaseTtl: \"15\"\n"},
		{"enabled, 500ms", "admission:\n  enabled: true\n  valkeyAddr: v\n  leaseTtl: 500ms\n"},
	}
	for _, tc := range fails {
		t.Run(tc.name+" fails", func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.body))
			if err == nil || !strings.Contains(err.Error(), "invalid admission.leaseTtl") {
				t.Fatalf("err=%v, want invalid admission.leaseTtl", err)
			}
		})
	}
}

func TestLoadProtection(t *testing.T) {
	s, err := Load(writeTemp(t, "protection:\n  maxRequestBodyBytes: 1024\n  streamStallTimeout: 30s\n  maxStreamDuration: 2h\n"))
	if err != nil {
		t.Fatal(err)
	}
	eff := s.Protection.Effective()
	if eff.MaxRequestBodyBytes != 1024 || eff.StreamStallTimeout != 30*time.Second || eff.MaxStreamDuration != 2*time.Hour {
		t.Fatalf("parsed protection = %+v", eff)
	}
	if eff.MaxResponseBufferBytesTotal != DefaultMaxResponseBufferBytesTotal {
		t.Fatalf("absent fields keep their default, got total=%d", eff.MaxResponseBufferBytesTotal)
	}

	// One case per rule in ProtectionSettings.parse; each error must name
	// the offending protection.<field>.
	for _, tc := range []struct{ name, yaml, field string }{
		{"negative body size", "protection:\n  maxRequestBodyBytes: -1\n", "protection.maxRequestBodyBytes"},
		{"negative per-stream buffer", "protection:\n  maxResponseBufferBytesPerStream: -1\n", "protection.maxResponseBufferBytesPerStream"},
		{"negative total buffer", "protection:\n  maxResponseBufferBytesTotal: -1\n", "protection.maxResponseBufferBytesTotal"},
		{"negative holds", "protection:\n  maxDedicatedColdHoldsPerEndpoint: -1\n", "protection.maxDedicatedColdHoldsPerEndpoint"},
		{"zero body size", "protection:\n  maxRequestBodyBytes: 0\n", "protection.maxRequestBodyBytes"},
		{"zero per-stream buffer", "protection:\n  maxResponseBufferBytesPerStream: 0\n", "protection.maxResponseBufferBytesPerStream"},
		{"zero total buffer", "protection:\n  maxResponseBufferBytesTotal: 0\n", "protection.maxResponseBufferBytesTotal"},
		{"zero holds", "protection:\n  maxDedicatedColdHoldsPerEndpoint: 0\n", "protection.maxDedicatedColdHoldsPerEndpoint"},
		{"unparseable stall timeout", "protection:\n  streamStallTimeout: soon\n", "protection.streamStallTimeout"},
		{"zero stall timeout", "protection:\n  streamStallTimeout: 0s\n", "protection.streamStallTimeout"},
		{"unparseable stream duration", "protection:\n  maxStreamDuration: later\n", "protection.maxStreamDuration"},
		{"zero stream duration", "protection:\n  maxStreamDuration: 0s\n", "protection.maxStreamDuration"},
		{"per-stream over total", "protection:\n  maxResponseBufferBytesPerStream: 2048\n  maxResponseBufferBytesTotal: 1024\n", "protection.maxResponseBufferBytesPerStream"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("err=%v, want a startup error naming %s", err, tc.field)
			}
		})
	}
}

// Every protection limit follows one rule: an absent (or null) key takes the
// shipped default, an explicit zero or a negative value stops startup with an
// error naming the key, and a positive value is kept. There is no "0 means
// unlimited" for any of them.
func TestProtectionLimitsRejectZeroAndNegativeDefaultWhenAbsent(t *testing.T) {
	for _, f := range []struct {
		key      string
		positive string
		get      func(ProtectionSettings) string
		want     string
		def      string
	}{
		{"maxRequestBodyBytes", "1024", func(p ProtectionSettings) string { return fmt.Sprint(p.MaxRequestBodyBytes) }, "1024", fmt.Sprint(DefaultMaxRequestBodyBytes)},
		{"maxResponseBufferBytesPerStream", "1024", func(p ProtectionSettings) string { return fmt.Sprint(p.MaxResponseBufferBytesPerStream) }, "1024", fmt.Sprint(DefaultMaxResponseBufferBytesPerStream)},
		{"maxResponseBufferBytesTotal", "536870912", func(p ProtectionSettings) string { return fmt.Sprint(p.MaxResponseBufferBytesTotal) }, "536870912", fmt.Sprint(DefaultMaxResponseBufferBytesTotal)},
		{"maxDedicatedColdHoldsPerEndpoint", "3", func(p ProtectionSettings) string { return fmt.Sprint(p.MaxDedicatedColdHoldsPerEndpoint) }, "3", fmt.Sprint(DefaultMaxDedicatedColdHoldsPerEndpoint)},
		{"streamStallTimeout", "30s", func(p ProtectionSettings) string { return p.StreamStallTimeout.String() }, "30s", DefaultStreamStallTimeout.String()},
		{"maxStreamDuration", "2h", func(p ProtectionSettings) string { return p.MaxStreamDuration.String() }, "2h0m0s", DefaultMaxStreamDuration.String()},
	} {
		zero, negative := "0", "-1"
		if strings.HasSuffix(f.positive, "s") || strings.HasSuffix(f.positive, "h") {
			zero, negative = "0s", "-1s"
		}
		t.Run(f.key+" absent takes the default", func(t *testing.T) {
			s, err := Load(writeTemp(t, "protection: {}\n"))
			if err != nil {
				t.Fatal(err)
			}
			if got := f.get(s.Protection.Effective()); got != f.def {
				t.Fatalf("%s=%s, want default %s", f.key, got, f.def)
			}
		})
		t.Run(f.key+" null takes the default", func(t *testing.T) {
			s, err := Load(writeTemp(t, "protection:\n  "+f.key+": null\n"))
			if err != nil {
				t.Fatal(err)
			}
			if got := f.get(s.Protection.Effective()); got != f.def {
				t.Fatalf("%s=%s, want default %s", f.key, got, f.def)
			}
		})
		for _, bad := range []struct{ name, value string }{{"zero", zero}, {"negative", negative}} {
			t.Run(f.key+" "+bad.name+" stops startup", func(t *testing.T) {
				_, err := Load(writeTemp(t, "protection:\n  "+f.key+": "+bad.value+"\n"))
				if err == nil || !strings.Contains(err.Error(), "protection."+f.key) {
					t.Fatalf("err=%v, want a startup error naming protection.%s", err, f.key)
				}
			})
		}
		t.Run(f.key+" positive is kept", func(t *testing.T) {
			s, err := Load(writeTemp(t, "protection:\n  "+f.key+": "+f.positive+"\n"))
			if err != nil {
				t.Fatal(err)
			}
			if got := f.get(s.Protection.Effective()); got != f.want {
				t.Fatalf("%s=%s, want %s", f.key, got, f.want)
			}
		})
	}
}

// maxPromptBytes was renamed (and changed unit). A leftover value must stop
// startup rather than be dropped in silence, wherever it appears and whether
// or not admission is enabled; a null leftover (the shipped chart value) is
// absent and harmless.
func TestLoadRejectsRemovedMaxPromptBytes(t *testing.T) {
	for _, yaml := range []string{
		"admission:\n  platform:\n    maxPromptBytes: 1024\n",
		"admission:\n  enabled: true\n  valkeyAddr: v\n  graph:\n    maxPromptBytes: 0\n",
		"admission:\n  lanes:\n    default:\n      weight: 1\n      limits:\n        maxPromptBytes: 5\n",
	} {
		_, err := Load(writeTemp(t, yaml))
		if err == nil || !strings.Contains(err.Error(), "maxPrefillTokens") {
			t.Fatalf("yaml %q: err=%v, want a rename error naming maxPrefillTokens", yaml, err)
		}
	}
	if _, err := Load(writeTemp(t, "admission:\n  platform:\n    maxPromptBytes: null\n")); err != nil {
		t.Fatalf("a null leftover must load: %v", err)
	}
}

// TestLimitValuesListsEveryAdmissionLimitField guards the canonical field
// list: a new *int64 limit on AdmissionLimits that LimitValues misses would
// silently drop out of lane-share validation and of AdmissionLimits.Any (and
// so of per-graph capacity enforcement). RemovedMaxPromptBytes is a rename
// tripwire, not a limit, and is excluded.
func TestLimitValuesListsEveryAdmissionLimitField(t *testing.T) {
	var l AdmissionLimits
	v := reflect.ValueOf(&l).Elem()
	want := map[*int64]string{}
	for i := 0; i < v.NumField(); i++ {
		f := v.Type().Field(i)
		if f.Type != reflect.TypeOf((*int64)(nil)) || f.Name == "RemovedMaxPromptBytes" {
			continue
		}
		p := new(int64)
		v.Field(i).Set(reflect.ValueOf(p))
		want[p] = f.Name
	}
	got := LimitValues(l)
	if len(got) != len(want) {
		t.Fatalf("LimitValues has %d entries, AdmissionLimits has %d limit fields", len(got), len(want))
	}
	for _, p := range got {
		if _, ok := want[p]; !ok {
			t.Fatalf("LimitValues returned a pointer that is not a distinct limit field")
		}
		delete(want, p)
	}
	for _, name := range want {
		t.Errorf("LimitValues does not list %s", name)
	}
}

// TestLimitFieldsNameMatchesYAMLTag guards the name each LimitFields entry
// carries: lane-share errors report that name, so it must be the YAML key of
// the very field whose value the entry points at. A misaligned entry would
// make an error name the wrong field.
func TestLimitFieldsNameMatchesYAMLTag(t *testing.T) {
	var l AdmissionLimits
	v := reflect.ValueOf(&l).Elem()
	tagOf := map[*int64]string{}
	for i := 0; i < v.NumField(); i++ {
		f := v.Type().Field(i)
		if f.Type != reflect.TypeOf((*int64)(nil)) || f.Name == "RemovedMaxPromptBytes" {
			continue
		}
		p := new(int64)
		v.Field(i).Set(reflect.ValueOf(p))
		tagOf[p] = strings.Split(f.Tag.Get("yaml"), ",")[0]
	}
	fields := LimitFields(l)
	values := LimitValues(l)
	if len(fields) != len(tagOf) || len(values) != len(fields) {
		t.Fatalf("LimitFields has %d entries, LimitValues %d, AdmissionLimits has %d limit fields", len(fields), len(values), len(tagOf))
	}
	for i, f := range fields {
		tag, ok := tagOf[f.Value]
		if !ok {
			t.Fatalf("LimitFields[%d] (%s) does not point at a distinct limit field", i, f.Name)
		}
		if f.Name != tag {
			t.Errorf("LimitFields[%d].Name = %q, but its value is the field tagged %q", i, f.Name, tag)
		}
		if values[i] != f.Value {
			t.Errorf("LimitValues[%d] differs from LimitFields[%d].Value", i, i)
		}
	}
}

// TestAdmissionLimitsParseRejectsEachNegativeField guards that parse's
// negative check covers every limit field: each field set to -1 on its own
// must fail to load.
func TestAdmissionLimitsParseRejectsEachNegativeField(t *testing.T) {
	n := len(LimitFields(AdmissionLimits{}))
	for i := 0; i < n; i++ {
		var l AdmissionLimits
		f := LimitFields(l)[i]
		v := reflect.ValueOf(&l).Elem()
		for j := 0; j < v.NumField(); j++ {
			if strings.Split(v.Type().Field(j).Tag.Get("yaml"), ",")[0] == f.Name {
				v.Field(j).Set(reflect.ValueOf(func() *int64 { x := int64(-1); return &x }()))
			}
		}
		if LimitFields(l)[i].Value == nil {
			t.Fatalf("could not set field %s by its yaml tag", f.Name)
		}
		err := l.parse("admission.platform")
		if err == nil || !strings.Contains(err.Error(), "cannot be negative") {
			t.Errorf("%s = -1: err=%v, want a cannot-be-negative error", f.Name, err)
		}
	}
}

// TestAdmissionLimitsAnyReportsEachField: Any is true when any single limit
// field is set (an explicit zero cap included) and false only when all are nil.
func TestAdmissionLimitsAnyReportsEachField(t *testing.T) {
	if (AdmissionLimits{}).Any() {
		t.Fatal("empty limits must report no limit")
	}
	if (AdmissionLimits{RemovedMaxPromptBytes: new(int64)}).Any() {
		t.Fatal("the removed maxPromptBytes tripwire is not a limit")
	}
	v := reflect.ValueOf(AdmissionLimits{})
	for i := 0; i < v.NumField(); i++ {
		f := v.Type().Field(i)
		if f.Type != reflect.TypeOf((*int64)(nil)) || f.Name == "RemovedMaxPromptBytes" {
			continue
		}
		var l AdmissionLimits
		reflect.ValueOf(&l).Elem().Field(i).Set(reflect.ValueOf(new(int64)))
		if !l.Any() {
			t.Errorf("Any() false with only %s set", f.Name)
		}
	}
}

// TestAdmissionLimitsAnyDoesNotAllocate guards that Any stays cheap on the
// proxy hot path, where it runs for every request on several limit sets.
func TestAdmissionLimitsAnyDoesNotAllocate(t *testing.T) {
	set := AdmissionLimits{WakesPerWindow: new(int64)}
	empty := AdmissionLimits{}
	allocs := testing.AllocsPerRun(100, func() {
		_ = set.Any()
		_ = empty.Any()
	})
	if allocs != 0 {
		t.Errorf("Any() allocated %.0f times per call pair, want 0", allocs)
	}
}

// protection.maxStreamDuration bounds the wake hold too, so with wake enabled
// it must be longer than the effective wake.timeout (default 300s).
func TestLoadMaxStreamDurationMustExceedWakeTimeout(t *testing.T) {
	const wake = "gateway:\n  enabled: true\n  namespace: ns\nwake:\n  enabled: true\n"
	for _, tc := range []struct {
		name, yaml string
		ok         bool
	}{
		{"defaults", wake, true},
		{"stream equal to default wake", wake + "protection:\n  maxStreamDuration: 300s\n", false},
		{"stream under set wake", wake + "  timeout: 90m\n", false}, // 60m default stream
		{"stream above set wake", wake + "  timeout: 90m\nprotection:\n  maxStreamDuration: 2h\n", true},
		{"wake disabled", "protection:\n  maxStreamDuration: 10s\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.yaml))
			if (err == nil) != tc.ok {
				t.Fatalf("err=%v, want ok=%v", err, tc.ok)
			}
		})
	}
}
