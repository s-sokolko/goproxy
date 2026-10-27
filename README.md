# goproxy

A small socat-like TCP proxy: it listens on one address and forwards every
connection to another one.

```
go build -o goproxy .
```

## Usage

```
goproxy [options] -l <listen> -r <remote>
goproxy [options] <listen> <remote>
```

Addresses can be written as `tcp://host:port` (`tcp4://` and `tcp6://` pin the
IP version), as a bare `host:port`, or as a bare port number, which means all
interfaces. The port may be a service name from `/etc/services`, e.g. `http`.

| Option | Default | Meaning |
| --- | --- | --- |
| `-l`, `-listen` | — | address to listen on |
| `-r`, `-remote` | — | address to forward connections to |
| `-connect-timeout` | `10s` | give up dialling the remote after this long |
| `-idle-timeout` | `0` | drop a connection after this long without traffic (0 disables it) |
| `-drain-timeout` | `10s` | on shutdown, wait this long for open connections before closing them |
| `-max-conns` | `0` | maximum number of concurrent connections (0 is unlimited) |
| `-v` | off | log every connection and copy error |
| `-version` | | print the version and exit |

## Examples

```sh
# Expose an internal web server on port 8080.
goproxy 8080 10.0.0.5:80

# Forward a database port, logging each connection.
goproxy -v 127.0.0.1:5432 db.internal:5432

# Bounded number of clients, drop sessions idle for five minutes.
goproxy -max-conns 100 -idle-timeout 5m :2222 ssh.internal:22
```

## Behaviour

- Each accepted connection gets its own connection to the remote; data is
  copied in both directions until both sides are done.
- Half-close is preserved: when one side shuts down its write half, the other
  side sees EOF but can still send the rest of its answer. Protocols that
  signal "end of request" that way keep working.
- With no `-idle-timeout`, copying is handed to `io.Copy`, so on Linux the
  kernel moves the bytes straight between the two sockets with `splice(2)` and
  they never reach user space.
- `-idle-timeout` applies per direction and is pushed back on every chunk, so a
  slow but progressing transfer is never cut off, while a silent one is. It
  rules out the `splice(2)` fast path, because the deadlines have to be set
  around each read and write; the copy then runs through a pooled buffer.
- Left off, a peer that vanishes without a FIN holds its connection until TCP
  keepalive gives up on it, and forever if it is reachable but silent.
- On `SIGINT`/`SIGTERM` the listener stops accepting, open connections get
  `-drain-timeout` to finish, and whatever is still running is then closed.
- With `-max-conns`, connections over the limit are closed right after accept
  and logged; they are never passed to the remote.

## Tests

```sh
go test -race ./...
```
