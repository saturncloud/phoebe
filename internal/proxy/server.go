package proxy

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Run starts the interceptor HTTP server and blocks until a shutdown signal.
//
// Timeouts are tuned for long-lived token streams: ReadHeaderTimeout bounds
// the initial header read, but there is NO WriteTimeout — a streamed response
// may run far longer than any fixed deadline, and a WriteTimeout would sever
// it mid-stream. IdleTimeout bounds idle keep-alive connections.
func (s *Server) Run() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	return s.serve(stop)
}

// serve runs the proxy and metrics listeners until one fails or stop fires.
func (s *Server) serve(stop <-chan os.Signal) error {
	srv := &http.Server{
		Addr:              s.settings.ListenAddr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       s.settings.IdleTimeout,
	}

	errCh := make(chan error, 2)
	go func() {
		s.log.Info.Printf("interceptor listening on %s", s.settings.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// /metrics gets its own listener so it is never reachable through the
	// proxy port (metrics.go, POSTURE). A failure to bind it stops phoebe like
	// a failure to bind the proxy port: a silently missing metrics port would
	// look like an idle install.
	var metricsSrv *http.Server
	if s.metrics != nil {
		mux := http.NewServeMux()
		mux.Handle("/metrics", s.metrics.Handler())
		metricsSrv = &http.Server{
			Addr:              s.settings.MetricsAddr,
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
			WriteTimeout:      30 * time.Second,
		}
		go func() {
			s.log.Info.Printf("metrics listening on %s", s.settings.MetricsAddr)
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}()
	}

	select {
	case err := <-errCh:
		return err
	case sig := <-stop:
		s.log.Info.Printf("received %s, shutting down", sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if metricsSrv != nil {
		defer func() { _ = metricsSrv.Shutdown(ctx) }()
	}
	return srv.Shutdown(ctx)
}
