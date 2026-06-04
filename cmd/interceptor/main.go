package main

import (
	"flag"

	"github.com/saturncloud/phoebe/internal/config"
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

	resolver := registry.NewStatic(settings.Default)
	emitter := &metering.LogEmitter{Log: log}

	srv := proxy.New(settings, log, resolver, emitter)
	if err := srv.Run(); err != nil {
		log.Error.Fatalf("server error: %v", err)
	}
}
