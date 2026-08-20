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

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/emit"
	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/iolog"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
	"github.com/saturncloud/phoebe/internal/proxy"
	"github.com/saturncloud/phoebe/internal/waker"
)

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

	// Gateway wiring FIRST: buildGateway may Fatalf on a fail-closed
	// misconfiguration, and at this point nothing needs cleanup yet.
	gwResolver, closeGateway := buildGateway(settings, log)

	emitter, closeEmitter := buildEmitter(settings, log)
	ioPolicy, ioSink, ioMaxBody, closeIOLog := buildIOLog(settings, log)

	srv := proxy.NewWithIOLog(settings, log, emitter, ioPolicy, ioSink, ioMaxBody)
	if gwResolver != nil {
		srv = srv.WithGateway(gwResolver, settings.Gateway.Namespace, settings.Gateway.Port)
	}
	if w := buildWaker(settings, log); w != nil {
		// 0/0 = the proxy's own defaults (120s wake ceiling, 3 tries).
		srv = srv.WithWaker(w, 0, 0)
	}
	srvErr := srv.Run()

	// Cleanup must run UNCONDITIONALLY before exit. log.Fatalf here would
	// os.Exit and skip deferred closes, stranding buffered metering events
	// (no WAL flush, no log floor) and unflushed I/O-log batches — so collect
	// the error, close everything, then exit nonzero.
	closeIOLog()
	closeEmitter()
	closeGateway()

	if srvErr != nil {
		log.Error.Printf("server error: %v", srvErr)
		os.Exit(1)
	}
}

// buildGateway constructs the TF gateway (org, model) → tf_model resolver:
// Atlas Postgres lookup behind a TTL cache. Returns (nil, no-op) when the
// gateway is disabled (the default) — the proxy then refuses gateway-marked
// requests fail-closed.
//
// FAIL CLOSED at startup: enabled with no usable DSN (neither
// gateway.databaseUrl nor the DATABASE_URL env, Atlas convention) or an
// unparseable DSN is a Fatalf — an interceptor that silently served gateway
// 503s while claiming the feature is on would be a misconfiguration trap.
// A REACHABILITY failure is deliberately NOT checked here (sql.Open does not
// dial): a DB outage must degrade to per-request 503s on the gateway path
// only, not crashloop an interceptor that also serves header-routed traffic.
func buildGateway(s *config.Settings, log *logging.Logger) (gateway.Resolver, func()) {
	if !s.Gateway.Enabled {
		return nil, func() {}
	}

	dsn := s.Gateway.DatabaseURL
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		log.Error.Fatalf("gateway.enabled=true requires gateway.databaseUrl (or the DATABASE_URL env var)")
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
	log.Info.Printf("gateway: enabled (namespace=%s port=%d, cache %s/%s pos/neg)",
		s.Gateway.Namespace, s.Gateway.Port, gateway.DefaultPositiveTTL, gateway.DefaultNegativeTTL)
	return resolver, func() { _ = db.Close() }
}

// buildWaker constructs the client-go DGDSA waker (wake-from-zero actuation
// in the MAIN phoebe container — no separate waker pod; see
// deploy/rbac-waker.yaml for the RBAC it needs). Returns nil when wake is
// disabled (the default) OR when the kubernetes config is unavailable: the
// proxy then runs with wake off — cold responses pass through exactly as
// before — because a broken wake path must degrade the cold-start UX, never
// crash or block the proxy (which also serves warm traffic).
func buildWaker(s *config.Settings, log *logging.Logger) proxy.Waker {
	if !s.Wake.Enabled {
		return nil
	}
	w, err := waker.New(waker.Config{
		Namespace:  s.Gateway.Namespace,
		Kubeconfig: s.Wake.Kubeconfig,
	}, log)
	if err != nil {
		log.Error.Printf("wake: kubernetes client unavailable (%v); wake-from-zero DISABLED — cold responses pass through", err)
		return nil
	}
	log.Info.Printf("wake: enabled (DGDSA namespace=%s)", s.Gateway.Namespace)
	return w
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
