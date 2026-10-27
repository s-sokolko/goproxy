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

# Bounded number of clients.
goproxy -max-conns 100 :2222 ssh.internal:22
```

## Behaviour

- Each accepted connection gets its own connection to the remote; data is
  copied in both directions until both sides are done.
- Half-close is preserved: when one side shuts down its write half, the other
  side sees EOF but can still send the rest of its answer. Protocols that
  signal "end of request" that way keep working.
- Copying is handed to `io.Copy`, so on Linux the kernel moves the bytes
  straight between the two sockets with `splice(2)`; they never reach user
  space.
- There is no idle timeout: a connection lives until one side closes it. TCP
  keepalive, which Go enables by default, reaps peers that became unreachable,
  but not a peer that is reachable and merely silent.
- On `SIGINT`/`SIGTERM` the listener stops accepting, open connections get
  `-drain-timeout` to finish, and whatever is still running is then closed.
- With `-max-conns`, connections over the limit are closed right after accept
  and logged; they are never passed to the remote.

## Tests

```sh
go test -race ./...
```
