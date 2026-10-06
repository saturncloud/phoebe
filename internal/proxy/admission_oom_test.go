package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/logging"
)

// oomStoreHook fails every admission script the way a full noeviction Valkey
// does (the shared install Valkey once a metering backlog fills its cap).
type oomStoreHook struct{}

func (oomStoreHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (oomStoreHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (oomStoreHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if name := cmd.Name(); name == "evalsha" || name == "eval" {
			err := errors.New("OOM command not allowed when used memory > 'maxmemory'.") //nolint:revive // verbatim Redis/Valkey OOM reply the code under test matches
			cmd.SetErr(err)
			return err
		}
		return next(ctx, cmd)
	}
}

// A full admission store bypasses the soft gate like any unavailable store,
// but the bypass is logged under its own label so a metering backlog filling
// the shared Valkey is visible apart from a network outage.
func TestAdmissionBypassLabelsStoreOutOfMemoryDistinctly(t *testing.T) {
	var hits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hits++; w.WriteHeader(200) }))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = c.Close() })
	c.AddHook(oomStoreHook{})

	var buf bytes.Buffer
	logger := &logging.Logger{Error: log.New(&buf, "", 0), Warn: log.New(io.Discard, "", 0)}
	s := New(&config.Settings{Admission: cfg}, logger, &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))

	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, sharedRequest(up))
	if rr.Code != http.StatusOK || hits != 1 {
		t.Fatalf("status=%d hits=%d, want the OOM store to bypass the soft gate (200, 1 upstream hit)", rr.Code, hits)
	}
	out := buf.String()
	if !strings.Contains(out, "admission store out of memory (Valkey OOM)") {
		t.Fatalf("OOM bypass not labelled distinctly: %q", out)
	}
	if strings.Contains(out, "distributed gate unavailable") {
		t.Fatalf("OOM bypass logged as generic unavailability: %q", out)
	}
}
