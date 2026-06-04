package main

import (
	"context"
	"flag"
	"net/url"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/emit"
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
	defer closeEmitter()

	srv := proxy.New(settings, log, resolver, emitter)
	if err := srv.Run(); err != nil {
		log.Error.Fatalf("server error: %v", err)
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
		conv, err := registry.NewConventionResolver(registry.ConventionConfig{Template: rs.ConventionTemplate})
		if err != nil {
			return nil, err
		}
		log.Info.Printf("resolver: cached (lookup degrades to convention until control-plane wired)")
		return registry.NewCachedResolver(conventionLookup(conv), registry.CacheConfig{
			Size:        rs.CacheSize,
			PositiveTTL: rs.PositiveTTL,
			NegativeTTL: rs.NegativeTTL,
		})

	case "chain":
		conv, err := registry.NewConventionResolver(registry.ConventionConfig{Template: rs.ConventionTemplate})
		if err != nil {
			return nil, err
		}
		cached, err := registry.NewCachedResolver(conventionLookup(conv), registry.CacheConfig{
			Size:        rs.CacheSize,
			PositiveTTL: rs.PositiveTTL,
			NegativeTTL: rs.NegativeTTL,
		})
		if err != nil {
			return nil, err
		}
		// Cached control-plane lookup first, naming-convention fallback if it
		// errors — graceful degradation when the control plane is unreachable.
		log.Info.Printf("resolver: chain (cached → convention)")
		return registry.ChainResolver{cached, conv}, nil

	default:
		return registry.NewStatic(s.Default), nil
	}
}

// conventionLookup adapts a ConventionResolver to a registry.LookupFunc so it
// can stand in for the (not-yet-wired) control-plane lookup.
func conventionLookup(conv *registry.ConventionResolver) registry.LookupFunc {
	return func(_ context.Context, resourceID string) (*url.URL, error) {
		return conv.Resolve(resourceID)
	}
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
