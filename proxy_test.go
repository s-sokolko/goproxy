package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseAddr(t *testing.T) {
	ok := []struct {
		in      string
		network string
		address string
	}{
		{"tcp://127.0.0.1:80", "tcp", "127.0.0.1:80"},
		{"tcp:127.0.0.1:80", "tcp", "127.0.0.1:80"},
		{"127.0.0.1:80", "tcp", "127.0.0.1:80"},
		{"example.com:http", "tcp", "example.com:http"},
		{":8080", "tcp", ":8080"},
		{"8080", "tcp", ":8080"},
		{"TCP6://[::1]:53", "tcp6", "[::1]:53"},
		{"tcp4://0.0.0.0:1", "tcp4", "0.0.0.0:1"},
		{"  8080  ", "tcp", ":8080"},
	}
	for _, tc := range ok {
		got, err := ParseAddr(tc.in)
		if err != nil {
			t.Errorf("ParseAddr(%q): unexpected error: %v", tc.in, err)
			continue
		}
		if got.Network != tc.network || got.Address != tc.address {
			t.Errorf("ParseAddr(%q) = %s %s, want %s %s", tc.in, got.Network, got.Address, tc.network, tc.address)
		}
	}

	bad := []string{"", "   ", "localhost", "udp://1.2.3.4:53", "unix:/tmp/x.sock", "1.2.3.4:99999", "1.2.3.4:"}
	for _, in := range bad {
		if got, err := ParseAddr(in); err == nil {
			t.Errorf("ParseAddr(%q) = %v, want an error", in, got)
		}
	}
}

// echoServer answers every connection by echoing what it reads.
func echoServer(t *testing.T) net.Addr {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				io.Copy(c, c)
			}()
		}
	}()
	return ln.Addr()
}

// startProxy runs a proxy in front of remote and returns the address it listens
// on plus a function that shuts it down and reports serve's error.
func startProxy(t *testing.T, remote net.Addr, tune func(*config)) (net.Addr, func() error) {
	t.Helper()
	cfg := config{
		remote:         Addr{Network: "tcp", Address: remote.String()},
		connectTimeout: 2 * time.Second,
		drainTimeout:   time.Second,
	}
	if tune != nil {
		tune(&cfg)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := newProxy(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- p.serve(ctx, ln) }()

	var stopped bool
	stop := func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		select {
		case err := <-errc:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("serve did not return after cancellation")
			return nil
		}
	}
	t.Cleanup(func() { stop() })
	return ln.Addr(), stop
}

func TestProxyRoundTrip(t *testing.T) {
	front, _ := startProxy(t, echoServer(t), nil)

	c, err := net.Dial("tcp", front.String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))

	payload := []byte("hello socat")
	if _, err := c.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("got %q, want %q", got, payload)
	}
}

func TestProxyLargeTransfer(t *testing.T) {
	front, _ := startProxy(t, echoServer(t), nil)

	c, err := net.Dial("tcp", front.String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))

	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i)
	}
	done := make(chan error, 1)
	go func() {
		_, err := c.Write(payload)
		done <- err
	}()

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("write: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatal("echoed payload differs from what was sent")
	}
}

// TestProxyHalfClose checks that a client shutting down its write side still
// gets the answer the remote sends afterwards.
func TestProxyHalfClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		req, _ := io.ReadAll(c) // returns only once the client half-closes
		c.Write(append([]byte("got:"), req...))
	}()

	front, _ := startProxy(t, ln.Addr(), nil)
	c, err := net.Dial("tcp", front.String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))

	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := c.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}
	resp, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(resp) != "got:ping" {
		t.Fatalf("got %q, want %q", resp, "got:ping")
	}
}

func TestProxyMaxConns(t *testing.T) {
	front, _ := startProxy(t, echoServer(t), func(c *config) { c.maxConns = 1 })

	first, err := net.Dial("tcp", front.String())
	if err != nil {
		t.Fatalf("dial first: %v", err)
	}
	defer first.Close()
	first.SetDeadline(time.Now().Add(5 * time.Second))
	// Round-trip so the first connection is certainly counted.
	if _, err := first.Write([]byte("x")); err != nil {
		t.Fatalf("write first: %v", err)
	}
	if _, err := io.ReadFull(first, make([]byte, 1)); err != nil {
		t.Fatalf("read first: %v", err)
	}

	second, err := net.Dial("tcp", front.String())
	if err != nil {
		t.Fatalf("dial second: %v", err)
	}
	defer second.Close()
	second.SetDeadline(time.Now().Add(5 * time.Second))
	if n, err := second.Read(make([]byte, 1)); err == nil {
		t.Fatalf("read %d bytes on the over-limit connection, want it closed", n)
	}
}

// TestProxyRemoteUnreachable: the client connection is closed when the remote
// cannot be dialled.
func TestProxyRemoteUnreachable(t *testing.T) {
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := dead.Addr()
	dead.Close() // nothing is listening there now

	front, _ := startProxy(t, addr, nil)
	c, err := net.Dial("tcp", front.String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if n, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatalf("read %d bytes, want the connection closed", n)
	}
}

// TestProxyShutdownClosesOpenConnections: an idle but open connection is torn
// down once the drain timeout passes, and serve returns.
func TestProxyShutdownClosesOpenConnections(t *testing.T) {
	front, stop := startProxy(t, echoServer(t), func(c *config) { c.drainTimeout = 50 * time.Millisecond })

	c, err := net.Dial("tcp", front.String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := io.ReadFull(c, make([]byte, 1)); err != nil {
		t.Fatalf("read: %v", err)
	}

	if err := stop(); err != nil {
		t.Fatalf("serve returned %v, want nil", err)
	}
	if n, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatalf("read %d bytes after shutdown, want the connection closed", n)
	}
	if _, err := net.Dial("tcp", front.String()); err == nil {
		t.Fatal("proxy still accepts connections after shutdown")
	}
}

func TestProxyIdleTimeout(t *testing.T) {
	front, _ := startProxy(t, echoServer(t), func(c *config) { c.idleTimeout = 150 * time.Millisecond })

	c, err := net.Dial("tcp", front.String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("idle connection was not dropped")
	}
}

// TestProxyIdleTimeoutRefreshes checks that the deadline tracks progress: a
// connection busy for longer than the timeout, with every gap shorter than it,
// must survive.
func TestProxyIdleTimeoutRefreshes(t *testing.T) {
	const idle = 300 * time.Millisecond
	front, _ := startProxy(t, echoServer(t), func(c *config) { c.idleTimeout = idle })

	c, err := net.Dial("tcp", front.String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))

	started := time.Now()
	for i := range 5 {
		time.Sleep(idle / 2)
		roundTrip(t, c, fmt.Sprintf("tick-%d", i))
	}
	if elapsed := time.Since(started); elapsed < idle {
		t.Fatalf("test ran for %v, less than the %v idle timeout it means to outlast", elapsed, idle)
	}
}

func TestRunArgumentErrors(t *testing.T) {
	cases := [][]string{
		{},
		{"8080"},
		{"-l", "8080"},
		{"8080", "127.0.0.1:9", "extra"},
		{"udp://:53", "127.0.0.1:9"},
		{"8080", "not-an-address"},
		{"-max-conns", "-1", "8080", "127.0.0.1:9"},
	}
	for _, args := range cases {
		if err := run(args); err == nil {
			t.Errorf("run(%q) = nil, want an error", args)
		}
	}
	if err := run([]string{"-version"}); err != nil {
		t.Errorf("run(-version) = %v, want nil", err)
	}
	if err := run([]string{"-h"}); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("run(-h) = %v, want nil", err)
	}
}

// TestServeReturnsOnAcceptError guards against serve hanging when the accept
// loop ends for a reason other than cancellation.
func TestServeReturnsOnAcceptError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := newProxy(config{
		remote:       Addr{Network: "tcp", Address: "127.0.0.1:9"},
		drainTimeout: 50 * time.Millisecond,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	errc := make(chan error, 1)
	go func() { errc <- p.serve(context.Background(), ln) }()

	ln.Close() // pull the listener out from under Accept; ctx stays alive

	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("serve returned nil, want the accept error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve hung after the accept loop ended")
	}
}

// TestProxyDrainKeepsConnectionsAlive pins down that cancelling the context
// stops the listener but leaves transfers already in flight running for the
// whole drain window.
func TestProxyDrainKeepsConnectionsAlive(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := newProxy(config{
		remote:         Addr{Network: "tcp", Address: echoServer(t).String()},
		connectTimeout: 2 * time.Second,
		drainTimeout:   3 * time.Second,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- p.serve(ctx, ln) }()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	roundTrip(t, c, "before shutdown")

	cancel()

	// Wait until new connections are refused, which proves the cancellation
	// has reached the listener and the drain window is open.
	deadline := time.Now().Add(3 * time.Second)
	for {
		probe, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			break
		}
		probe.Close()
		if time.Now().After(deadline) {
			t.Fatal("listener still accepting after cancellation")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The connection accepted earlier must still carry data.
	roundTrip(t, c, "during drain")

	c.Close()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("serve returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return")
	}
}

func roundTrip(t *testing.T, c net.Conn, payload string) {
	t.Helper()
	if _, err := c.Write([]byte(payload)); err != nil {
		t.Fatalf("write %q: %v", payload, err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("read %q: %v", payload, err)
	}
	if string(got) != payload {
		t.Fatalf("got %q, want %q", got, payload)
	}
}

// TestProxyConcurrentConnections checks that simultaneous clients each get
// their own connection to the remote, with no bytes crossing between them.
func TestProxyConcurrentConnections(t *testing.T) {
	front, _ := startProxy(t, echoServer(t), nil)

	const clients = 25
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := range clients {
		wg.Go(func() {
			c, err := net.Dial("tcp", front.String())
			if err != nil {
				errs <- fmt.Errorf("client %d: dial: %w", i, err)
				return
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(10 * time.Second))

			want := fmt.Sprintf("client-%03d-payload", i)
			if _, err := c.Write([]byte(want)); err != nil {
				errs <- fmt.Errorf("client %d: write: %w", i, err)
				return
			}
			got := make([]byte, len(want))
			if _, err := io.ReadFull(c, got); err != nil {
				errs <- fmt.Errorf("client %d: read: %w", i, err)
				return
			}
			if string(got) != want {
				errs <- fmt.Errorf("client %d: got %q, want %q", i, got, want)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestProxyMaxConnsReleasesSlots: a closed connection frees its slot, so the
// proxy does not wedge after max-conns connections have come and gone.
func TestProxyMaxConnsReleasesSlots(t *testing.T) {
	front, _ := startProxy(t, echoServer(t), func(c *config) { c.maxConns = 1 })

	for i := range 3 {
		payload := fmt.Sprintf("slot-%d", i)
		// The slot is released slightly after the client sees the close, so
		// retry until it frees up rather than racing the handler.
		deadline := time.Now().Add(3 * time.Second)
		for {
			if proxyRoundTrips(front, payload) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("slot never became free for connection %d", i)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func proxyRoundTrips(front net.Addr, payload string) bool {
	c, err := net.Dial("tcp", front.String())
	if err != nil {
		return false
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte(payload)); err != nil {
		return false
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(c, got); err != nil {
		return false
	}
	return string(got) == payload
}

// TestProxyShutdownIsPromptWhenIdle: with nothing to drain, shutdown must not
// sit out the drain window.
func TestProxyShutdownIsPromptWhenIdle(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := newProxy(config{
		remote:       Addr{Network: "tcp", Address: echoServer(t).String()},
		drainTimeout: 30 * time.Second,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- p.serve(ctx, ln) }()

	cancel()
	started := time.Now()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("serve returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve waited out the drain window with nothing to drain")
	}
	t.Logf("shutdown took %v with a 30s drain timeout", time.Since(started).Round(time.Millisecond))
}

// TestProxyClientDisconnectReachesRemote: a client that closes outright, not
// just half-closes, still shows up as EOF on the remote.
func TestProxyClientDisconnectReachesRemote(t *testing.T) {
	type result struct {
		data []byte
		err  error
	}
	got := make(chan result, 1)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		b, err := io.ReadAll(c)
		got <- result{b, err}
	}()

	front, _ := startProxy(t, ln.Addr(), nil)
	c, err := net.Dial("tcp", front.String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte("partial request")); err != nil {
		t.Fatalf("write: %v", err)
	}
	c.Close() // the client vanishes mid-conversation

	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("remote read: %v", r.err)
		}
		if string(r.data) != "partial request" {
			t.Fatalf("remote got %q, want %q", r.data, "partial request")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the remote never saw the client go away")
	}
}

// TestRunListenFailure covers the wiring from run to the listener, through
// both the flag form and the positional form.
func TestRunListenFailure(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer busy.Close()
	taken := busy.Addr().String()

	for _, args := range [][]string{
		{"-l", taken, "-r", "127.0.0.1:9"},
		{taken, "127.0.0.1:9"},
	} {
		err := run(args)
		if err == nil {
			t.Errorf("run(%q) = nil, want a listen error", args)
			continue
		}
		if !strings.Contains(err.Error(), "listen on") {
			t.Errorf("run(%q) = %v, want it to name the listen failure", args, err)
		}
	}
}
