package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/emit"
	"github.com/saturncloud/phoebe/internal/iolog"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
	"github.com/saturncloud/phoebe/internal/proxy"
	"github.com/saturncloud/phoebe/internal/registry"
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

	resolver, err := buildResolver(settings, log)
	if err != nil {
		log.Error.Fatalf("failed to build resolver: %v", err)
	}

	emitter, closeEmitter := buildEmitter(settings, log)
	ioPolicy, ioSink, ioMaxBody, closeIOLog := buildIOLog(settings, log)

	srv := proxy.NewWithIOLog(settings, log, resolver, emitter, ioPolicy, ioSink, ioMaxBody)
	srvErr := srv.Run()

	// Cleanup must run UNCONDITIONALLY before exit. log.Fatalf here would
	// os.Exit and skip deferred closes, stranding buffered metering events
	// (no WAL flush, no log floor) and unflushed I/O-log batches — so collect
	// the error, close everything, then exit nonzero.
	closeIOLog()
	closeEmitter()

	if srvErr != nil {
		log.Error.Printf("server error: %v", srvErr)
		os.Exit(1)
	}
}

// buildResolver constructs the model→upstream resolver per the configured
// strategy. "static" uses the single DefaultUpstream (M0/M1 behaviour);
// "convention", "cached", and "chain" enable dynamic dispatch (M4).
//
// NOTE: the "cached" and "chain" strategies are meant to resolve via a control
// plane (Atlas) lookup. That control-plane API is the still-unverified seam
// (does auth-server already resolve model resources via X-Saturn-Resource-Id?),
// so until it's wired the LookupFunc degrades to the naming convention — a
// reasonable guess that needs no redeploy. Replace conventionLookup with the
// real Atlas call once the resource-resolution path is confirmed.
func buildResolver(s *config.Settings, log *logging.Logger) (registry.Resolver, error) {
	rs := s.Registry
	switch rs.Strategy {
	case "", "static":
		log.Info.Printf("resolver: static (default upstream %s)", s.DefaultUpstream)
		return registry.NewStatic(s.Default), nil

	case "convention":
		log.Info.Printf("resolver: convention (%s)", rs.ConventionTemplate)
		return registry.NewConventionResolver(registry.ConventionConfig{
			Template: rs.ConventionTemplate,
		})

	case "cached":
		lookup, err := k8sLookup(rs)
		if err != nil {
			return nil, err
		}
		log.Info.Printf("resolver: cached (k8s label lookup in namespace %q)", rs.K8sNamespace)
		return registry.NewCachedResolver(lookup, registry.CacheConfig{
			Size:        rs.CacheSize,
			PositiveTTL: rs.PositiveTTL,
			NegativeTTL: rs.NegativeTTL,
		})

	case "chain":
		lookup, err := k8sLookup(rs)
		if err != nil {
			return nil, err
		}
		cached, err := registry.NewCachedResolver(lookup, registry.CacheConfig{
			Size:        rs.CacheSize,
			PositiveTTL: rs.PositiveTTL,
			NegativeTTL: rs.NegativeTTL,
		})
		if err != nil {
			return nil, err
		}
		conv, err := registry.NewConventionResolver(registry.ConventionConfig{Template: rs.ConventionTemplate})
		if err != nil {
			return nil, err
		}
		// Cached k8s label lookup first; convention fallback only if the k8s API
		// is unreachable (transient). The convention template is a best-effort
		// guess at the Service name and is NOT guaranteed correct for Saturn's
		// pd-{identity5}-{name}-{id} scheme — prefer plain "cached" unless your
		// convention template matches your cluster.
		log.Info.Printf("resolver: chain (cached k8s → convention fallback)")
		return registry.ChainResolver{cached, conv}, nil

	default:
		return registry.NewStatic(s.Default), nil
	}
}

// k8sLookup builds the real control-plane LookupFunc: resolve a deployment id to
// its model Service via the saturncloud.io/resource-id label, using the
// in-cluster Kubernetes API. Requires an RBAC Role granting get/list on services
// in rs.K8sNamespace.
func k8sLookup(rs config.RegistrySettings) (registry.LookupFunc, error) {
	if rs.K8sNamespace == "" {
		return nil, fmt.Errorf("registry.k8sNamespace is required for the %q strategy "+
			"(the namespace inference Services live in, e.g. \"main-namespace\")", rs.Strategy)
	}
	client, err := registry.InClusterClient()
	if err != nil {
		return nil, fmt.Errorf("registry: %w", err)
	}
	return registry.NewK8sLookup(registry.K8sLookupConfig{
		Namespace: rs.K8sNamespace,
		Client:    client,
	})
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
