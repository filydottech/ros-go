# ros-go

A RouterOS native API client for Go, over TCP and TLS.

`Dial` connects over TLS or plaintext, verifies the certificate, and logs in
with one `/login` message, the modern RouterOS API login used since 6.43. `Do`
runs one command and returns records in wire order with the final `!done`
attributes, bounded by `MaxResultRecords` and `MaxResultBytes`; `!trap` becomes
a `*routeros.TrapError` that carries those attributes, and a command whose
result exceeds its limit while it is active, times out, or whose context ends
sends `/cancel` and returns a `*routeros.ResourceLimitError`,
`*routeros.TimeoutError`, or `*routeros.CancelledError`. A path ending in
`/add` without a `ret` in `!done` is a non-fatal
`*routeros.ProtocolError`. `Stream` receives ordered `!re` records through
`Recv`, returns `io.EOF` for a normal `!done`, and uses bounded queues with TCP
backpressure; cancelling its context or calling `Close` sends `/cancel`.
`Reconnect` replaces the current connection generation without replaying a
command or resubscribing a stream: pending work and work attempted while it is
in progress return `*routeros.ReconnectError`. A failed reconnect leaves the
client unavailable for normal work but can be retried explicitly; `Close` is
final.

## Install

```sh
go get github.com/filydottech/ros-go
```

The package name is `routeros`:

```go
import "github.com/filydottech/ros-go"
```

## TLS quick start

TLS is the default. The client uses port 8729, verifies the certificate chain,
and verifies the hostname in `Host`.

<!-- docs:compile -->
```go
package main

import (
	"context"
	"os"

	routeros "github.com/filydottech/ros-go"
)

func main() {
	client, err := routeros.Dial(context.Background(), routeros.Config{
		Host:     "router.example.com",
		Username: "api",
		Password: []byte(os.Getenv("ROS_PASSWORD")),
	})
	if err != nil {
		panic(err)
	}
	defer client.Close()
}
```
<!-- /docs:compile -->

For a private certificate authority, set `Config.TLSConfig.RootCAs`. Do not use
`InsecureSkipVerify` outside controlled testing.

## Plaintext connection

Plaintext is an explicit opt-in. It sends the login on an unencrypted TCP
connection and defaults to port 8728, so use it only on a network you trust.

<!-- docs:compile -->
```go
package main

import (
	"context"
	"os"

	routeros "github.com/filydottech/ros-go"
)

func main() {
	client, err := routeros.Dial(context.Background(), routeros.Config{
		Host:      "router.example.com",
		Username:  "api",
		Password:  []byte(os.Getenv("ROS_PASSWORD")),
		Plaintext: true,
	})
	if err != nil {
		panic(err)
	}
	defer client.Close()
}
```
<!-- /docs:compile -->

## Commands and queries

`Do` accepts any RouterOS menu path. A `Request` keeps attributes, a property
list, and query words in the order supplied by the caller.

<!-- docs:compile -->
```go
package main

import (
	"context"

	routeros "github.com/filydottech/ros-go"
)

func printEthernet(ctx context.Context, client *routeros.Client) (routeros.Result, error) {
	request := routeros.Request{
		Proplist: []string{"name", "type", "comment"},
		Query: routeros.And(
			routeros.Equal("type", "ether"),
			routeros.Absent("comment"),
		).Words(),
	}
	return client.Do(ctx, "/interface/print", request)
}

func main() {}
```
<!-- /docs:compile -->

Every path is valid at runtime; the client ships no command catalogue. A
request carries ordered `Attribute` values, an optional `Proplist`, and ordered
query words. Build query words with `Equal`, `LessThan`, `GreaterThan`,
`Exists`, `Absent`, `Not`, `And`, and `Or`; arbitrary field names and values
remain valid inputs. `Query` also accepts raw words beginning with `?` when a
caller needs the RouterOS query language directly.

`Do` retains its whole result only within `MaxResultRecords` and
`MaxResultBytes`; use `Stream` for a long-running or unbounded command.

## Streams

`Stream` is for long-running commands such as `/interface/listen`. It keeps a
bounded record queue. A slow consumer can delay other commands on the same
connection, so use a dedicated `Client` for a high-volume listener.

<!-- docs:compile -->
```go
package main

import (
	"context"
	"errors"
	"io"

	routeros "github.com/filydottech/ros-go"
)

func listen(ctx context.Context, client *routeros.Client, consume func(routeros.Record)) error {
	stream, err := client.Stream(ctx, "/interface/listen", routeros.Request{})
	if err != nil {
		return err
	}
	defer stream.Close()

	for {
		record, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		consume(record)
	}
}

func main() {}
```
<!-- /docs:compile -->

Cancel the context or call `Stream.Close` to send `/cancel`. `Recv` returns
`io.EOF` only when the router finishes normally.

## Errors and lifecycle

Errors are typed. Use `errors.As` or `errors.Is` to distinguish a router trap,
a command timeout, cancellation, a reconnect interruption, or a final client
close.

<!-- docs:compile -->
```go
package main

import (
	"context"
	"errors"

	routeros "github.com/filydottech/ros-go"
)

func readIdentity(ctx context.Context, client *routeros.Client) error {
	_, err := client.Do(ctx, "/system/identity/print", routeros.Request{})
	if err == nil {
		return nil
	}

	var trap *routeros.TrapError
	if errors.As(err, &trap) {
		return trap
	}
	if errors.Is(err, &routeros.TimeoutError{}) {
		return err
	}
	return err
}

func main() {}
```
<!-- /docs:compile -->

`Reconnect` accepts a complete new `Config`. It closes the current generation,
does not replay commands or resubscribe streams, and installs a new generation
only after login succeeds. Pending operations and new work while reconnecting
return `*routeros.ReconnectError`. A failed reconnect is retryable. `Close` is
final.

<!-- docs:compile -->
```go
package main

import (
	"context"

	routeros "github.com/filydottech/ros-go"
)

func reconnect(ctx context.Context, client *routeros.Client, cfg routeros.Config) error {
	oldGeneration := client.Done()
	if err := client.Reconnect(ctx, cfg); err != nil {
		return err
	}
	<-oldGeneration
	_, err := client.Do(ctx, "/system/identity/print", routeros.Request{})
	return err
}

func main() {}
```
<!-- /docs:compile -->

## Security defaults

- TLS is the default, on port 8729, with certificate and hostname verification
  enabled.
- Plaintext requires an explicit `Plaintext: true` and then defaults to port
  8728. It is never the zero-value behaviour.
- `Config.TLSConfig` is cloned before use and its `ServerName` is filled from
  `Host` when empty. The caller's config is not modified.
- `InsecureSkipVerify` is possible only when the caller supplies it in
  `TLSConfig`; it is a testing mode, not a fallback.
- Passwords are held as `[]byte` and are never written to a log or an error.

## Limits

Every limit has a safe default and may be lowered or raised explicitly.

| Setting | Default |
| --- | ---: |
| `MaxWordBytes` | 32 MiB |
| `MaxSentenceBytes` | 64 MiB |
| `MaxWordsPerSentence` | 4096 |
| `MaxResultRecords` | 1024 |
| `MaxResultBytes` | 8 MiB |
| `StreamMaxRecords` | 1024 |
| `StreamMaxBytes` | 8 MiB |
| `MaxBufferedStreamBytes` | 64 MiB |
| `MaxOpenStreams` | 16 |
| `CancelGrace` | 2 seconds |

`ConnectTimeout` and `CommandTimeout` default to 10 seconds each.

## Compatibility and tests

The client targets RouterOS 6.43+ because it uses the modern one-message
`/login` exchange. It does not contain a command catalogue or version gate.
The live matrix records only versions exercised by this module's integration
tests.

| RouterOS image | Plaintext read-only | Write CRUD | Verified TLS | Restricted API user |
| --- | --- | --- | --- | --- |
| `evilfreelancer/docker-routeros:6.49` | PASS | PASS | PASS | PASS |
| `evilfreelancer/docker-routeros:7.23` | PASS | PASS | PASS | PASS |

Run the unit, race, and documentation-example checks from the module root:

```sh
go test -count=1 ./...
go test -race -count=1 ./...
```

Read-only integration tests require a dedicated router and connection values in
the environment. They do not modify RouterOS configuration:

```sh
ROS_HOST=router.example.com ROS_PORT=8728 ROS_USER=api ROS_PASSWORD=... \
  go test -tags=integration -count=1 .
```

The verified-TLS test additionally needs the TLS endpoint and a PEM file for
the CA that signed the router certificate. It uses the normal TLS verification
path and does not opt into `InsecureSkipVerify`:

```sh
ROS_TLS_HOST=router.example.com ROS_TLS_PORT=8729 ROS_TLS_CA_FILE=router-ca.pem \
  ROS_USER=api ROS_PASSWORD=... \
  go test -tags=integration,integration_tls -count=1 -run '^TestRouterOSTLSWithVerifiedCA$' .
```

The restricted-user test requires an API user assigned to a dedicated group
with `read,api` policy. It checks a read-only command and does not attempt a
mutation:

```sh
ROS_HOST=router.example.com ROS_PORT=8728 \
  ROS_RESTRICTED_USER=readonly ROS_RESTRICTED_PASSWORD=... \
  go test -tags=integration,integration_restricted -count=1 -run '^TestRouterOSRestrictedAPIUserReadsIdentity$' .
```

The hotspot-user CRUD test requires the additional `integration_write` build
tag. It creates a uniquely named disabled user, round-trips a UTF-8 comment,
and removes the user through both the normal path and deferred cleanup. Run it
only against a disposable test router:

```sh
ROS_HOST=router.example.com ROS_PORT=8728 ROS_USER=api ROS_PASSWORD=... \
  go test -tags=integration,integration_write -count=1 .
```

The TLS and restricted-user checks are opt-in because they require dedicated
lab provisioning. Do not run the write test against a production router.

## Go version

The minimum supported version is Go 1.24. The minimum and the latest stable Go
release are both tested before a release, and the minimum is raised only
together with a compatibility note.

## Licence

MIT.
