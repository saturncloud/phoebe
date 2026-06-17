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
	srv := &http.Server{
		Addr:              s.settings.ListenAddr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       s.settings.IdleTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		s.log.Info.Printf("interceptor listening on %s", s.settings.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case sig := <-stop:
		s.log.Info.Printf("received %s, shutting down", sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}
