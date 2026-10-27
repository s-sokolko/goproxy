package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Addr is a parsed TCP endpoint: a Go network name plus an address in the form
// net.Dial and net.Listen expect. The host is deliberately left unresolved so
// that every connection re-resolves it.
type Addr struct {
	Network string // tcp, tcp4 or tcp6
	Address string // host:port
}

var networks = map[string]bool{"tcp": true, "tcp4": true, "tcp6": true}

// ParseAddr understands tcp://host:port, tcp:host:port, a bare host:port and a
// bare port number (which listens on, or dials, every interface). tcp4 and tcp6
// pin the IP version. The port may be a name from /etc/services, such as http.
func ParseAddr(s string) (Addr, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Addr{}, fmt.Errorf("empty address")
	}

	network, rest := "tcp", s
	if scheme, tail, ok := strings.Cut(s, "://"); ok {
		network, rest = strings.ToLower(scheme), tail
	} else if scheme, tail, ok := strings.Cut(s, ":"); ok && networks[strings.ToLower(scheme)] {
		network, rest = strings.ToLower(scheme), tail
	}
	if !networks[network] {
		return Addr{}, fmt.Errorf("%q: unknown network %q, want tcp, tcp4 or tcp6", s, network)
	}

	host, port, err := net.SplitHostPort(rest)
	if err != nil {
		// A bare port number is allowed and means every interface.
		if _, numeric := strconv.ParseUint(rest, 10, 16); numeric != nil {
			return Addr{}, fmt.Errorf("%q: expected host:port: %w", s, err)
		}
		host, port = "", rest
	}
	if port == "" {
		return Addr{}, fmt.Errorf("%q: missing port", s)
	}
	// LookupPort accepts a number or a service name and range-checks both.
	if _, err := net.LookupPort(network, port); err != nil {
		return Addr{}, fmt.Errorf("%q: %w", s, err)
	}
	return Addr{Network: network, Address: net.JoinHostPort(host, port)}, nil
}

func (a Addr) String() string { return a.Network + "://" + a.Address }
