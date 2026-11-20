package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/meteredusage"
)

// scriptedPoster answers each Post with the next error in script (the last
// repeats); a nil entry is a 200 that accepts every record.
type scriptedPoster struct {
	script []error
	calls  int
}

func (s *scriptedPoster) Post(_ context.Context, req meteredusage.Request) (meteredusage.Response, error) {
	s.calls++
	err := s.script[len(s.script)-1]
	if s.calls <= len(s.script) {
		err = s.script[s.calls-1]
	}
	if err != nil {
		return meteredusage.Response{}, err
	}
	return meteredusage.Response{Accepted: int64(len(req.Records)), WindowStart: req.WindowStart, Kinds: req.Kinds}, nil
}

// dialRefused is what meteredusage.Client returns when every attempt's
// connection is refused (egress blocked with a reject rule, or nothing
// listening).
func dialRefused() error {
	return &meteredusage.TransientError{Attempts: meteredusage.DefaultMaxAttempts, Last: fmt.Errorf("POST https://manager.example/customer/metered-usage: %w",
		&url.Error{Op: "Post", URL: "https://manager.example/customer/metered-usage",
			Err: &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}})}
}

// serverDown is a 5xx that outlasted the retries: the manager answered.
func serverDown() error {
	return &meteredusage.TransientError{Attempts: meteredusage.DefaultMaxAttempts, Last: errors.New("POST https://manager.example/customer/metered-usage: status 503: unavailable")}
}

// tenHours is a store with usage in ten consecutive hours.
func tenHours() (*fakeStore, []time.Time) {
	f := newFake()
	var hours []time.Time
	for i := 0; i < 10; i++ {
		h := win.Add(time.Duration(i-9) * time.Hour)
		f.rollups[h] = fixtureRollups()
		hours = append(hours, h)
	}
	return f, hours
}

func errorLog() (*logging.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	l := logging.New(logging.ERROR)
	l.Error = log.New(&buf, "", 0)
	return l, &buf
}

// Blocked egress must fail fast and loud: after maxConsecutiveUnreachable
// hours in a row that cannot connect, the run stops (it does not spend its
// deadline trying every remaining hour), exits non-zero, and says which URL it
// cannot reach and why.
func TestPushWindows_UnreachableManagerStopsTheRunWithAClearError(t *testing.T) {
	f, hours := tenHours()
	l, logs := errorLog()
	post := &scriptedPoster{script: []error{dialRefused()}}
	p := &pusher{store: f, log: l, client: post, managerURL: "https://manager.example"}
	if code := p.pushWindows(context.Background(), hours, now, false); code != exitFatal {
		t.Fatalf("exit %d, want exitFatal(%d)", code, exitFatal)
	}
	if post.calls != maxConsecutiveUnreachable {
		t.Fatalf("%d hours attempted, want the run to stop after %d unreachable ones", post.calls, maxConsecutiveUnreachable)
	}
	want := "cannot reach manager at https://manager.example: "
	if !strings.Contains(logs.String(), want) || !strings.Contains(logs.String(), "connection refused") {
		t.Fatalf("log does not name the URL and the cause (want %q ... connection refused):\n%s", want, logs.String())
	}
	if len(f.accepted) != 0 || len(f.refused) != 0 {
		t.Fatalf("accepted %v, parked %v; an unreachable manager accepts and parks nothing", f.accepted, f.refused)
	}
}

// The count is of hours IN A ROW: an hour the manager answers resets it, and
// a 5xx is an answer (today's retry behaviour: the run goes on to every hour).
func TestPushWindows_ServerErrorsAndIntermittentConnectFailuresDoNotStopTheRun(t *testing.T) {
	f, hours := tenHours()
	post := &scriptedPoster{script: []error{serverDown()}}
	if code := newPusher(f, post).pushWindows(context.Background(), hours, now, false); code != exitFatal || post.calls != len(hours) {
		t.Fatalf("503 every hour: exit %d after %d hours; want exitFatal after all %d", code, post.calls, len(hours))
	}

	f, hours = tenHours()
	u := dialRefused()
	post = &scriptedPoster{script: []error{u, u, nil, u, u, serverDown(), u, u, nil, nil}}
	if code := newPusher(f, post).pushWindows(context.Background(), hours, now, false); code != exitFatal || post.calls != len(hours) {
		t.Fatalf("never 3 unreachable in a row: exit %d after %d hours; want exitFatal after all %d", code, post.calls, len(hours))
	}
	if len(f.accepted) != 3 {
		t.Fatalf("%d hours accepted, want 3", len(f.accepted))
	}
}
