package meteredusage

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

func noSleep(context.Context, time.Duration) error { return nil }

// The production client bounds connecting separately from the whole request:
// short dial and TLS-handshake timeouts, the configured request timeout for a
// slow response, and the environment's proxy settings kept.
func TestNewHTTPClient_SetsShortConnectTimeoutsAndKeepsTheRequestTimeout(t *testing.T) {
	if DialTimeout > 15*time.Second || TLSHandshakeTimeout > 15*time.Second {
		t.Fatalf("connect timeouts dial=%s handshake=%s; want about 10 s", DialTimeout, TLSHandshakeTimeout)
	}
	c := NewHTTPClient(60 * time.Second)
	if c.Timeout != 60*time.Second {
		t.Fatalf("request timeout %s, want 60s", c.Timeout)
	}
	tr, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T, want *http.Transport", c.Transport)
	}
	if tr.TLSHandshakeTimeout != TLSHandshakeTimeout {
		t.Fatalf("TLSHandshakeTimeout %s, want %s", tr.TLSHandshakeTimeout, TLSHandshakeTimeout)
	}
	if tr.DialContext == nil || tr.Proxy == nil {
		t.Fatal("the transport has no bounded dialer or lost the environment proxy")
	}
}

// A server that accepts the TCP connection and never answers the TLS
// handshake (an egress proxy that swallows the connection, for instance) fails
// at the handshake timeout, not at the much longer request timeout, and the
// failure counts as "cannot reach the manager".
func TestHTTPClient_SilentServerFailsAtTheHandshakeTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var held []net.Conn
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range held {
			_ = c.Close()
		}
	})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, conn) // hold it open, say nothing
			mu.Unlock()
		}
	}()
	c := &Client{BaseURL: "https://" + ln.Addr().String(), Token: "tok", MaxAttempts: 1, Sleep: noSleep,
		HTTP: newHTTPClient(30*time.Second, time.Second, 300*time.Millisecond)}
	start := time.Now()
	_, err = c.Post(context.Background(), Request{WindowStart: "2026-10-22T14:00:00+00:00", Kinds: []string{KindToken}})
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("took %s; want the handshake timeout (300ms), not the request timeout (30s)", took)
	}
	if !Unreachable(err) {
		t.Fatalf("Unreachable(%v) = false; a handshake that never completes means the manager cannot be reached", err)
	}
}

// Unreachable separates "nothing answered" from "the manager answered".
func TestUnreachable_DialDNSAndHandshakeFailuresButNotServerErrors(t *testing.T) {
	gone := httptest.NewServer(http.NotFoundHandler())
	gone.Close()
	c := &Client{BaseURL: gone.URL, Token: "tok", MaxAttempts: 2, Sleep: noSleep, HTTP: NewHTTPClient(5 * time.Second)}
	_, refused := c.Post(context.Background(), Request{WindowStart: "2026-10-22T14:00:00+00:00", Kinds: []string{KindToken}})
	var te *TransientError
	if !errors.As(refused, &te) || !Unreachable(refused) {
		t.Fatalf("connection refused: %v (Unreachable %v); want a TransientError that is unreachable", refused, Unreachable(refused))
	}

	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer down.Close()
	c.BaseURL = down.URL
	_, unavailable := c.Post(context.Background(), Request{WindowStart: "2026-10-22T14:00:00+00:00", Kinds: []string{KindToken}})
	if unavailable == nil || Unreachable(unavailable) {
		t.Fatalf("503: %v (Unreachable %v); a 5xx is an answer from the manager, not unreachable", unavailable, Unreachable(unavailable))
	}

	tlsSrv := httptest.NewTLSServer(http.NotFoundHandler()) // self-signed: does not verify
	defer tlsSrv.Close()
	c.BaseURL = tlsSrv.URL
	_, untrusted := c.Post(context.Background(), Request{WindowStart: "2026-10-22T14:00:00+00:00", Kinds: []string{KindToken}})
	if !Unreachable(untrusted) {
		t.Fatalf("certificate that does not verify: %v; want unreachable (an intercepting proxy, not the manager)", untrusted)
	}

	for _, err := range []error{
		&TransientError{Attempts: 5, Last: fmt.Errorf("POST x: %w", &net.DNSError{Err: "no such host", Name: "manager.example", IsNotFound: true})},
		fmt.Errorf("POST x: %w", &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ETIMEDOUT}}),
		fmt.Errorf("POST x: %w", &os.SyscallError{Syscall: "connect", Err: syscall.ENETUNREACH}),
	} {
		if !Unreachable(err) {
			t.Fatalf("Unreachable(%v) = false", err)
		}
	}
	for _, err := range []error{
		nil,
		errors.New("POST x: status 502: bad gateway"),
		&RejectedError{Status: 401, Body: "bad token"},
		fmt.Errorf("POST x: %w", &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}),
	} {
		if Unreachable(err) {
			t.Fatalf("Unreachable(%v) = true; the manager was reached", err)
		}
	}
}
