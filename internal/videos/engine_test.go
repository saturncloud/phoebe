package videos

import (
	"errors"
	"net"
	"os"
	"testing"
)

// TestIsRefusedCountsADialTimeout: a Service with no ready engine pod can drop
// the connection attempt instead of refusing it (uk2, issue #111). Nothing was
// sent, so the engine is not ready, and the attempt must not count.
func TestIsRefusedCountsADialTimeout(t *testing.T) {
	if !isRefused(&net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}) {
		t.Fatal("a dial timeout is not treated as engine not ready")
	}
	if isRefused(&net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}) {
		t.Fatal("a read failure after connecting is treated as engine not ready")
	}
}
