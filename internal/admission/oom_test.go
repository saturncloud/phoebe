package admission

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/config"
)

// valkeyOOMReply is the error Valkey returns for a write past maxmemory under
// the noeviction policy the shared install Valkey runs with.
const valkeyOOMReply = "OOM command not allowed when used memory > 'maxmemory'."

// oomHook makes every script call fail the way a full noeviction Valkey does.
type oomHook struct{}

func (oomHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (oomHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (oomHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if name := cmd.Name(); name == "evalsha" || name == "eval" {
			err := errors.New(valkeyOOMReply)
			cmd.SetErr(err)
			return err
		}
		return next(ctx, cmd)
	}
}

// An admission store at its memory cap is still an unavailable store (the
// soft gate bypasses), but the error must be recognisable as an OOM so the
// bypass log can tell a full shared Valkey apart from a network outage.
func TestStoreOutOfMemoryIsDistinguishableFromNetworkUnavailability(t *testing.T) {
	cfg := config.AdmissionSettings{Platform: limits(1), LeaseTTL: time.Minute, KeyPrefix: "oom"}

	full := miniredis.RunT(t)
	fullClient := redis.NewClient(&redis.Options{Addr: full.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = fullClient.Close() })
	fullClient.AddHook(oomHook{})
	_, err := New(fullClient, cfg).Admit(context.Background(), request("a", "m"))
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v, want ErrUnavailable (an OOM store still bypasses the soft gate)", err)
	}
	if !IsStoreOutOfMemory(err) {
		t.Fatalf("OOM admission error not recognised as store out of memory: %v", err)
	}

	down := miniredis.RunT(t)
	downClient := redis.NewClient(&redis.Options{Addr: down.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = downClient.Close() })
	down.Close()
	_, err = New(downClient, cfg).Admit(context.Background(), request("a", "m"))
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v, want ErrUnavailable", err)
	}
	if IsStoreOutOfMemory(err) {
		t.Fatalf("network unavailability misreported as store out of memory: %v", err)
	}
	if IsStoreOutOfMemory(nil) {
		t.Fatal("nil error reported as out of memory")
	}
}
