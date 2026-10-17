package main

import (
	"context"
	"database/sql"
	"flag"
	"os"
	"time"

	// pgx stdlib driver: registers "pgx" with database/sql for the gateway
	// tf_model resolver (same driver/DSN style as the drainer and iolog).
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/emit"
	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/iolog"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
	"github.com/saturncloud/phoebe/internal/proxy"
	"github.com/saturncloud/phoebe/internal/waker"
)

const internalWakeTokenEnv = "TF_INTERNAL_WAKE_TOKEN"

func main() {
	settingsFile := flag.String("f", "/etc/saturn/config/settings.yaml", "Settings YAML file path")
	flag.Parse()

	log := logging.New(logging.INFO)

	settings, err := config.Load(*settingsFile)
	if err != nil {
		log.Error.Fatalf("failed to load settings: %v", err)
	}
	if settings.Debug {
		log.SetLevel(logging.DEBUG)
	}

	// R3 trusted-header registry: parse PHOEBE_TRUSTED_HEADERS (rendered
	// from the phoebe chart's ConfigMap) into the active set the parser's
	// envelope reads resolve through. The pinned 15 already govern from
	// package init; this engages the runtime config once, at startup, and
	// warns loudly if the chart render was empty/malformed.
	identity.LoadTrustedHeaders(log)

	// Gateway wiring FIRST: buildGateway may Fatalf on a fail-closed
	// misconfiguration, and at this point nothing needs cleanup yet.
	gwResolver, closeGateway := buildGateway(settings, log)

	emitter, closeEmitter := buildEmitter(settings, log)
	ioPolicy, ioSink, ioMaxBody, closeIOLog := buildIOLog(settings, log)

	srv := proxy.NewWithIOLog(settings, log, emitter, ioPolicy, ioSink, ioMaxBody)
	admitter, closeAdmission := buildAdmission(settings, log)
	if admitter != nil {
		srv = srv.WithAdmitter(admitter)
	}
	closePolicy := buildAdmissionPolicy(settings, admitter, log)
	defer closePolicy()
	if gwResolver != nil {
		srv = srv.WithGateway(gwResolver, settings.Gateway.Namespace, settings.Gateway.Port)
	}
	topology, closeTopology := buildTopology(settings, log)
	defer closeTopology()
	if topology != nil {
		srv = srv.WithTopology(topology)
	}
	if w := buildWaker(settings, log); w != nil {
		// Timeout 0 = the proxy default (300s — sized above vLLM's measured
		// ~2.5min cold reload). A recognized cold response permits one retry.
		srv = srv.WithWaker(w, settings.Wake.Timeout)
	}
	srvErr := srv.Run()

	// Cleanup must run UNCONDITIONALLY before exit. log.Fatalf here would
	// os.Exit and skip deferred closes, stranding buffered metering events
	// (no WAL flush, no log floor) and unflushed I/O-log batches — so collect
	// the error, close everything, then exit nonzero.
	closeIOLog()
	closeEmitter()
	closeGateway()
	closeAdmission()

	if srvErr != nil {
		log.Error.Printf("server error: %v", srvErr)
		os.Exit(1)
	}
}

func buildAdmission(s *config.Settings, log *logging.Logger) (*admission.RedisAdmitter, func()) {
	admitter, client, ok := admission.FromSettings(s)
	if !ok {
		// No Valkey at all (admission.valkeyAddr and emit.valkeyAddr both
		// empty). Contract rate limits on the trusted envelope cannot be
		// enforced without the distributed store; say so loudly rather than
		// letting a stamped limit pass silently.
		log.Error.Printf("admission: no admission store configured (admission.valkeyAddr and emit.valkeyAddr are empty); contract rate limits (Atlas UsageLimits) are NOT enforced")
		return nil, func() {}
	}
	cfg := admitter.Settings()
	if s.Admission.ValkeyAddr == "" {
		// The store fell back to the metering Valkey. Admission then shares
		// memory and the noeviction cap with the durable metering stream, so
		// a metering backlog can make admission fail with OOM (see
		// docs/shared-tier-admission.md).
		log.Info.Printf("admission: admission.valkeyAddr is empty; using the metering Valkey from emit.valkeyAddr (%s) as the admission store", cfg.ValkeyAddr)
	}
	mode := "contract limits only (admission.enabled=false: operator capacity tiers off, envelope-less shared routes allowed)"
	if cfg.Enabled {
		mode = "contract limits + operator capacity tiers (admission.enabled=true: every shared request must carry the trusted envelope)"
	}
	// Admission is a fairness/capacity gate, not an authorization or billing
	// authority. Start serving when Valkey is unavailable and bypass only this
	// gate until it recovers; metering has its own durable path.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Error.Printf("admission: Valkey unavailable at %s; starting with distributed gate bypassed until recovery: %v", cfg.ValkeyAddr, err)
	} else {
		log.Info.Printf("admission: enforcing %s (valkey %s, lease ttl %s)", mode, cfg.ValkeyAddr, cfg.LeaseTTL)
	}
	return admitter, func() { _ = client.Close() }
}

// buildAdmissionPolicy starts the watcher that applies the Atlas-written
// admission-policy ConfigMap (the shared-tier admin page's gateway limits) to
// the admitter, live. It runs only where the operator tiers are on
// (admission.enabled) and the registry resolver runs, because it watches the
// same gateway namespace with the same access. With no admitter or with
// admission.enabled=false it returns quietly: buildAdmission already logged
// that mode. In every other case where it does not watch (gateway off, legacy
// resolver, no namespace, no kubernetes config) it logs a warning naming the
// reason, because Atlas still publishes caps that this replica then ignores.
//
// At startup it waits up to admissionPolicySyncWait for the first read of the
// ConfigMap, so a restarted replica does not serve on the Helm values while
// tighter admin caps are already published. It never fails startup: on a
// timeout it logs an error and serves on the Helm values until the read lands.
//
// admissionPolicySyncWait and newAdmissionPolicyKubeClient are variables so
// tests can shorten the wait and inject a client.
var (
	admissionPolicySyncWait      = 10 * time.Second
	newAdmissionPolicyKubeClient = gateway.NewKubeClient
)

func buildAdmissionPolicy(s *config.Settings, admitter *admission.RedisAdmitter, log *logging.Logger) func() {
	if admitter == nil || !s.Admission.Enabled {
		return func() {}
	}
	if reason := gatewayWatchDisabledReason(s); reason != "" {
		log.Warn.Printf("admission policy: not watching ConfigMap %s (%s); admin-page gateway limits are ignored and the Helm admission values stay in force", gateway.AdmissionPolicyConfigMap, reason)
		return func() {}
	}
	client, err := newAdmissionPolicyKubeClient(s.Gateway.Kubeconfig)
	if err != nil {
		log.Warn.Printf("admission policy: kubernetes config unavailable (%v); admin-page gateway limits are ignored and the Helm admission values stay in force", err)
		return func() {}
	}
	watcher := gateway.NewAdmissionPolicyWatcher(client, s.Gateway.Namespace, admitter, log)
	ctx, cancel := context.WithCancel(context.Background())
	go watcher.Run(ctx)
	// Bounded wait for the first read so a restarted replica enforces the
	// admin caps already published, not the Helm values. Keep serving either
	// way: a failed read leaves the Helm values, which is logged as an error.
	wctx, wcancel := context.WithTimeout(ctx, admissionPolicySyncWait)
	if !watcher.WaitSynced(wctx) {
		log.Error.Printf("admission policy: ConfigMap %s/%s not read within %s; the Helm admission values apply, and any tighter limits set on the admin page are NOT enforced until it syncs", s.Gateway.Namespace, gateway.AdmissionPolicyConfigMap, admissionPolicySyncWait)
	} else {
		log.Info.Printf("admission policy: watching ConfigMap %s/%s; %s in force", s.Gateway.Namespace, gateway.AdmissionPolicyConfigMap, watcher.InForce())
	}
	wcancel()
	return cancel
}

// buildTopology starts the graph topology index (internal/gateway/topology.go)
// that stamps each usage event with its graph's topology revision and region.
// It watches ConfigMaps in the gateway namespace, the same access the
// registry resolver already uses, so it runs only where the registry
// resolver runs. Anywhere else, or when the kubernetes config is missing,
// it logs a warning naming the reason and returns nil, and events carry no
// topology: the fields are margin evidence, so their absence is never a
// reason to stop serving.
//
// topologySyncWait and newTopologyKubeClient are variables so tests can
// shorten the wait and inject a client that never syncs.
var (
	topologySyncWait      = 10 * time.Second
	newTopologyKubeClient = gateway.NewKubeClient
)

func buildTopology(s *config.Settings, log *logging.Logger) (*gateway.TopologyIndex, func()) {
	// Each disabled case says so at Warn: without the index every usage
	// event, dedicated traffic included, records no topology revision or
	// region, and the margin pipeline sees revision-0 rollups.
	if reason := gatewayWatchDisabledReason(s); reason != "" {
		log.Warn.Printf("gateway: graph topology index disabled (%s); usage events, including dedicated traffic, carry no topology revision or region", reason)
		return nil, func() {}
	}
	client, err := newTopologyKubeClient(s.Gateway.Kubeconfig)
	if err != nil {
		log.Warn.Printf("gateway: graph topology index disabled (%v); usage events carry no topology revision or region", err)
		return nil, func() {}
	}
	index := gateway.NewTopologyIndex(client, s.Gateway.Namespace, log)
	ctx, cancel := context.WithCancel(context.Background())
	go index.Run(ctx)
	// Bounded wait for the first list so requests right after a pod start
	// carry their revision. Never fail serving on it: topology is evidence.
	wctx, wcancel := context.WithTimeout(ctx, topologySyncWait)
	if !index.WaitSynced(wctx) {
		log.Warn.Printf("gateway: graph topology index not synced within %s; usage events carry topology revision 0 until it syncs", topologySyncWait)
	}
	wcancel()
	return index, cancel
}

// gatewayWatchDisabledReason names the setting that keeps the gateway-namespace
// ConfigMap watchers (the topology index and the admission-policy watcher)
// off, or returns "" when they can run. Both reuse the registry resolver's
// ConfigMap access, so they run only where that resolver runs.
func gatewayWatchDisabledReason(s *config.Settings) string {
	switch {
	case !s.Gateway.Enabled:
		return "gateway.enabled=false"
	case s.Gateway.LegacyPostgresResolver:
		return "gateway.legacyPostgresResolver=true"
	case s.Gateway.Namespace == "":
		return "gateway.namespace is empty"
	}
	return ""
}

// buildGateway constructs the TF gateway (org, model) resolver. DEFAULT: the
// k8s-watch registry resolver (label-selected ConfigMaps in the workload
// namespace — see internal/gateway/registry.go; no database, no per-request
// I/O, no TTL cache: the informer IS the cache). The DEPRECATED direct
// Atlas-DB path stays behind gateway.legacyPostgresResolver for one release
// so installs can roll forward/back. Returns (nil, no-op) when the gateway is
// disabled — the proxy then refuses gateway-marked requests fail-closed.
//
// FAIL CLOSED at startup, both modes: an enabled gateway whose resolver
// cannot be BUILT (no kubernetes config / no usable DSN) is a Fatalf — an
// interceptor silently serving gateway 503s while claiming the feature is on
// would be a misconfiguration trap. RUNTIME reachability is deliberately not
// gated at startup: registry mode serves 503 until its first sync and then
// last-known state across API-server flaps; legacy mode degrades to
// per-request 503s on DB outage. Neither may crashloop an interceptor that
// also serves header-routed traffic.
func buildGateway(s *config.Settings, log *logging.Logger) (gateway.Resolver, func()) {
	if !s.Gateway.Enabled {
		return nil, func() {}
	}

	if s.Gateway.LegacyPostgresResolver {
		log.Warn.Printf("gateway: using the DEPRECATED legacyPostgresResolver (direct Atlas-DB lookups); migrate to the registry resolver — this path is removed next release")
		return buildLegacyPGGateway(s, log)
	}

	client, err := gateway.NewKubeClient(s.Gateway.Kubeconfig)
	if err != nil {
		log.Error.Fatalf("gateway: kubernetes config unavailable for the registry resolver (%v); set gateway.kubeconfig for dev, or gateway.legacyPostgresResolver to roll back", err)
	}
	resolver := gateway.NewRegistryResolver(client, s.Gateway.Namespace, log)
	ctx, cancel := context.WithCancel(context.Background())
	// Atlas publishes per-worker admission limits; the graph limit is that
	// times the graph's ready workers plus one (Q4(b)), read from the DGDs.
	dyn, err := gateway.NewDynamicClient(s.Gateway.Kubeconfig)
	if err != nil {
		log.Error.Fatalf("gateway: kubernetes config unavailable for the ready-worker informer (%v)", err)
	}
	workers := gateway.NewReadyWorkers(dyn, s.Gateway.Namespace, log)
	resolver.WithReadyWorkers(workers)
	go workers.Run(ctx)
	go resolver.Run(ctx)
	log.Info.Printf("gateway: enabled (registry resolver, namespace=%s port=%d) — serving 503 until the registry syncs",
		s.Gateway.Namespace, s.Gateway.Port)
	return resolver, cancel
}

// buildLegacyPGGateway is the deprecated Atlas-DB resolution path (TTL cache
// over tf_model lookups). Remove with gateway.PGResolver after the registry
// transition release.
func buildLegacyPGGateway(s *config.Settings, log *logging.Logger) (gateway.Resolver, func()) {
	dsn := s.Gateway.DatabaseURL
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		log.Error.Fatalf("gateway.legacyPostgresResolver=true requires gateway.databaseUrl (or the DATABASE_URL env var)")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Error.Fatalf("gateway: invalid database DSN: %v", err)
	}
	// Modest pool: the hot path is served from the TTL cache; the DB sees at
	// most one lookup per (org, model) per TTL plus cold-start bursts.
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	resolver := gateway.NewCache(gateway.NewPGResolver(db), gateway.DefaultPositiveTTL, gateway.DefaultNegativeTTL)
	log.Info.Printf("gateway: enabled (LEGACY postgres resolver, namespace=%s port=%d, cache %s/%s pos/neg)",
		s.Gateway.Namespace, s.Gateway.Port, gateway.DefaultPositiveTTL, gateway.DefaultNegativeTTL)
	return resolver, func() { _ = db.Close() }
}

// buildWaker sends shared and dedicated wakes to Atlas, which restores the
// workers before the Planner (Planner ruling 2026-10-12).
func buildWaker(s *config.Settings, log *logging.Logger) proxy.Waker {
	if !s.Wake.Enabled {
		return nil
	}
	atlasWaker, err := waker.NewAtlas(s.Wake.AtlasURL, os.Getenv(internalWakeTokenEnv), log)
	if err != nil {
		log.Error.Printf("wake: Atlas actuator unavailable (%v); wake-from-zero is disabled", err)
		return nil
	}
	log.Info.Printf("wake: enabled through Atlas %s", s.Wake.AtlasURL)
	return atlasWaker
}

// buildEmitter constructs the durable metering emitter. When ValkeyAddr is set
// it dials Valkey for the hot path; the WAL fallback and log floor are always
// active. Returns a close function for graceful shutdown.
func buildEmitter(s *config.Settings, log *logging.Logger) (metering.Emitter, func()) {
	cfg := emit.DefaultConfig()
	cfg.StreamName = s.Emit.StreamName
	if s.Emit.WALPath != "" {
		cfg.WALPath = s.Emit.WALPath
	}

	var rdb redis.Cmdable
	if s.Emit.ValkeyAddr != "" {
		cfg.ValkeyAddr = s.Emit.ValkeyAddr
		rdb = redis.NewClient(&redis.Options{Addr: s.Emit.ValkeyAddr})
		log.Info.Printf("emitter: durable (valkey %s, stream %s, wal %s)", cfg.ValkeyAddr, cfg.StreamName, cfg.WALPath)
	} else {
		log.Warn.Printf("emitter: WAL-only (no valkeyAddr configured), wal %s", cfg.WALPath)
	}

	em, err := emit.New(cfg, log, rdb)
	if err != nil {
		// A failed durable emitter must not take down serving. Fall back to the
		// log emitter so events are at least recoverable from logs.
		log.Error.Printf("durable emitter init failed (%v); falling back to log emitter", err)
		return &metering.LogEmitter{Log: log}, func() {}
	}

	return em, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		em.Close(ctx)
	}
}

// buildIOLog constructs the M5 I/O-logging policy + sink. It FAILS CLOSED: when
// ioLog.enabled is false (the default), it returns a deny-all policy and a
// NopSink, so the proxy buffers no bodies and the subsystem is fully inert.
//
// Only when enabled does it construct the StaticPolicy (the interim opt-in +
// sampling gate — see iolog.StaticPolicy for the control-plane TODO) and the
// PostgresSink. If the PostgresSink can't be built, logging degrades to off
// rather than taking down serving — I/O logging is best-effort debug telemetry,
// never a reason to fail a billable request.
//
// Returns the policy, sink, response-body cap, and a close function.
func buildIOLog(s *config.Settings, log *logging.Logger) (iolog.Policy, iolog.Sink, int, func()) {
	c := s.IOLog
	if !c.Enabled {
		log.Info.Printf("iolog: disabled (default) — no request/response bodies captured")
		return nil, nil, 0, func() {}
	}

	policy := iolog.NewStaticPolicy(c.Enabled, c.SampleRate, c.AllowAuthIDs, c.AllowGroupIDs, c.AllowAllTenants)

	cfg := iolog.DefaultConfig()
	cfg.DatabaseURL = c.DatabaseURL
	if c.MaxBodyBytes > 0 {
		cfg.MaxBodyBytes = c.MaxBodyBytes
	}

	sink, err := iolog.NewPostgresSink(cfg, log)
	if err != nil {
		// Degrade to off: never let a logging-store failure stop serving.
		log.Error.Printf("iolog: postgres sink init failed (%v); disabling I/O logging", err)
		return nil, nil, 0, func() {}
	}

	log.Info.Printf("iolog: enabled (sampleRate=%.3f, table=%s, maxBodyBytes=%d)", c.SampleRate, cfg.Table, cfg.MaxBodyBytes)
	return policy, sink, cfg.MaxBodyBytes, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = sink.Close(ctx)
	}
}
