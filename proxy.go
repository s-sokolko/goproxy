package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// closeWriter is implemented by *net.TCPConn: it shuts down the write half
// only, so the peer sees EOF while we can still read its answer.
type closeWriter interface{ CloseWrite() error }

// proxy accepts TCP connections and splices each one onto a fresh connection
// to the remote address.
type proxy struct {
	cfg config
	log *slog.Logger

	sem  chan struct{} // bounds concurrent connections when max-conns is set
	wg   sync.WaitGroup
	seq  atomic.Uint64
	live atomic.Int64
}

func newProxy(cfg config, log *slog.Logger) *proxy {
	p := &proxy{cfg: cfg, log: log}
	if cfg.maxConns > 0 {
		p.sem = make(chan struct{}, cfg.maxConns)
	}
	return p
}

// run listens on the configured address and serves until ctx is cancelled.
func (p *proxy) run(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, p.cfg.listen.Network, p.cfg.listen.Address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", p.cfg.listen, err)
	}
	return p.serve(ctx, ln)
}

// serve runs the accept loop on an existing listener. Once ctx is cancelled it
// stops accepting, lets open connections finish for up to drain-timeout, and
// then closes whatever is left.
func (p *proxy) serve(ctx context.Context, ln net.Listener) error {
	defer ln.Close() // panic safety; the orderly path closes below

	// Tears down connections that are still running once the drain period is
	// over. Deliberately rooted in Background and not in ctx: inheriting ctx
	// would fire this the moment the signal arrives, killing every transfer in
	// flight and making -drain-timeout meaningless. Only drain cancels it.
	forceCtx, force := context.WithCancel(context.Background())
	defer force()

	// Cancellation closes the listener, which unblocks Accept. AfterFunc only
	// starts a goroutine once ctx is done, and stop unregisters it when we
	// leave the loop for any other reason.
	stopCloser := context.AfterFunc(ctx, func() { ln.Close() })
	defer stopCloser()

	p.log.Info("listening", "listen", ln.Addr().String(), "remote", p.cfg.remote.String())

	var acceptErr error
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			acceptErr = fmt.Errorf("accept: %w", err)
			break
		}
		if !p.acquire() {
			p.log.Warn("refusing connection: max-conns reached",
				"client", conn.RemoteAddr().String(), "max", p.cfg.maxConns)
			conn.Close()
			continue
		}
		p.wg.Go(func() {
			defer p.release()
			p.handle(forceCtx, conn)
		})
	}

	// Stop listening before draining: the deferred Close would not run until
	// drain returns, and until then the kernel keeps completing handshakes
	// into the accept backlog, so clients would connect and then be dropped.
	// Already closed when we got here through cancellation, which is harmless.
	ln.Close()
	p.log.Info("shutting down", "open_connections", p.live.Load())
	p.drain(force)
	return acceptErr
}

// drain waits for open connections, then forces the stragglers closed.
func (p *proxy) drain(force context.CancelFunc) {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	if p.cfg.drainTimeout > 0 {
		timer := time.NewTimer(p.cfg.drainTimeout)
		defer timer.Stop()
		select {
		case <-done:
			return
		case <-timer.C:
			p.log.Warn("drain timed out, closing open connections", "open_connections", p.live.Load())
		}
	}
	force()
	<-done
}

func (p *proxy) acquire() bool {
	if p.sem == nil {
		return true
	}
	select {
	case p.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (p *proxy) release() {
	if p.sem != nil {
		<-p.sem
	}
}

// handle dials the remote and copies data in both directions until both are
// done. Each direction is half-closed on its own, so a client that shuts down
// its write side still receives the rest of the response.
func (p *proxy) handle(ctx context.Context, client net.Conn) {
	defer client.Close()

	log := p.log.With("id", p.seq.Add(1), "client", client.RemoteAddr().String())

	dialer := net.Dialer{Timeout: p.cfg.connectTimeout}
	remote, err := dialer.DialContext(ctx, p.cfg.remote.Network, p.cfg.remote.Address)
	if err != nil {
		log.Error("cannot reach remote", "remote", p.cfg.remote.String(), "err", err)
		return
	}
	defer remote.Close()

	p.live.Add(1)
	defer p.live.Add(-1)
	started := time.Now()
	log.Debug("connection open", "remote", remote.RemoteAddr().String())

	// Hard close both ends if the proxy is forced down mid-transfer. Nothing
	// runs until that happens, so an open connection costs no extra goroutine.
	stopKiller := context.AfterFunc(ctx, func() {
		client.Close()
		remote.Close()
	})
	defer stopKiller()

	// io.Copy returning nil means src sent FIN: that peer is done writing.
	// Forward it with CloseWrite rather than Close, so dst learns the input is
	// over while the reply path stays open -- servers that read until EOF
	// before answering deadlock otherwise. The connection is only closed once
	// both directions are finished, by the defers above. A real error instead
	// takes the whole connection down; there is no reply left to wait for.
	pipe := func(dst, src net.Conn) (int64, error) {
		n, err := io.Copy(dst, src)
		if err != nil {
			client.Close()
			remote.Close()
			return n, err
		}
		if cw, ok := dst.(closeWriter); !ok || cw.CloseWrite() != nil {
			dst.Close() // no half-close available, or it was refused
		}
		return n, nil
	}

	var (
		wg               sync.WaitGroup
		sent, received   int64
		sendErr, recvErr error
	)
	wg.Go(func() { sent, sendErr = pipe(remote, client) })
	wg.Go(func() { received, recvErr = pipe(client, remote) })
	wg.Wait()

	logCopyErr(log, "client->remote", sendErr)
	logCopyErr(log, "remote->client", recvErr)
	log.Debug("connection closed",
		"sent", sent, "received", received,
		"duration", time.Since(started).Round(time.Millisecond).String())
}

func logCopyErr(log *slog.Logger, direction string, err error) {
	if err == nil {
		return
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, context.Canceled) {
		log.Debug("copy stopped", "direction", direction, "err", err)
		return
	}
	log.Warn("copy failed", "direction", direction, "err", err)
}
