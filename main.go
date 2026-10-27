// Command goproxy is a small socat-like TCP proxy: it listens on one address
// and forwards every connection to another one.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const version = "0.1.0"

const usageHeader = `goproxy %s - a small socat-like TCP proxy.

Usage:
  goproxy [options] -l <listen> -r <remote>
  goproxy [options] <listen> <remote>

Address forms:
  tcp://host:port        tcp4:// and tcp6:// pin the IP version
  host:port              tcp is assumed
  8080                   all interfaces, port 8080

Options:
`

const usageFooter = `
Examples:
  goproxy 8080 10.0.0.5:80                     expose an internal web server
  goproxy -v 127.0.0.1:5432 db.internal:5432   forward a database port, logging each connection
  goproxy -max-conns 100 -idle-timeout 5m :2222 ssh.internal:22
`

type config struct {
	listen         Addr
	remote         Addr
	connectTimeout time.Duration
	idleTimeout    time.Duration
	drainTimeout   time.Duration
	maxConns       int
	verbose        bool
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "goproxy: "+err.Error())
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("goproxy", flag.ContinueOnError)
	var (
		listenArg   string
		remoteArg   string
		cfg         config
		showVersion bool
	)
	fs.StringVar(&listenArg, "l", "", "address to listen on")
	fs.StringVar(&listenArg, "listen", "", "address to listen on")
	fs.StringVar(&remoteArg, "r", "", "address to forward connections to")
	fs.StringVar(&remoteArg, "remote", "", "address to forward connections to")
	fs.DurationVar(&cfg.connectTimeout, "connect-timeout", 10*time.Second, "give up dialling the remote after this long")
	fs.DurationVar(&cfg.idleTimeout, "idle-timeout", 0, "drop a connection after this long without traffic (0 disables it)")
	fs.DurationVar(&cfg.drainTimeout, "drain-timeout", 10*time.Second, "on shutdown, wait this long for open connections before closing them")
	fs.IntVar(&cfg.maxConns, "max-conns", 0, "maximum number of concurrent connections (0 is unlimited)")
	fs.BoolVar(&cfg.verbose, "v", false, "log every connection and copy error")
	fs.BoolVar(&showVersion, "version", false, "print the version and exit")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), usageHeader, version)
		fs.PrintDefaults()
		fmt.Fprint(fs.Output(), usageFooter)
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if showVersion {
		fmt.Println("goproxy " + version)
		return nil
	}

	// Addresses may also be given positionally, socat style.
	rest := fs.Args()
	if listenArg == "" && len(rest) > 0 {
		listenArg, rest = rest[0], rest[1:]
	}
	if remoteArg == "" && len(rest) > 0 {
		remoteArg, rest = rest[0], rest[1:]
	}
	if len(rest) > 0 {
		return fmt.Errorf("unexpected argument %q", rest[0])
	}
	if listenArg == "" || remoteArg == "" {
		fs.Usage()
		return errors.New("a listen address and a remote address are required")
	}

	var err error
	if cfg.listen, err = ParseAddr(listenArg); err != nil {
		return fmt.Errorf("listen address: %w", err)
	}
	if cfg.remote, err = ParseAddr(remoteArg); err != nil {
		return fmt.Errorf("remote address: %w", err)
	}
	if cfg.maxConns < 0 {
		return errors.New("-max-conns cannot be negative")
	}

	level := slog.LevelInfo
	if cfg.verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return newProxy(cfg, logger).run(ctx)
}
