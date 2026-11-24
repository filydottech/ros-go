# ros-go

A RouterOS native API client for Go, over TCP and TLS.

`Dial` connects over TLS or plaintext, verifies the certificate, and logs in
with one `/login` message, the modern RouterOS API login used since 6.43. `Do`
runs one command and returns every record in wire order with the final `!done`
attributes; `!trap` becomes a `*routeros.TrapError` that carries those
attributes, a command that times out or whose context ends sends `/cancel` and
returns a `*routeros.TimeoutError` or `*routeros.CancelledError`, and a path
ending in `/add` without a `ret` in `!done` is a non-fatal
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

## Usage

```go
cfg := routeros.Config{
	Host:     "router.example.com",
	Username: "api",
	Password: []byte(os.Getenv("ROS_PASSWORD")),
}

client, err := routeros.Dial(context.Background(), cfg)
if err != nil {
	return err
}
defer client.Close()

request := routeros.Request{
	Query: routeros.And(
		routeros.Equal("type", "ether"),
		routeros.Absent("comment"),
	).Words(),
}

result, err := client.Do(context.Background(), "/interface/print", request)
```

Every path is valid at runtime; the client ships no command catalogue. A
request carries ordered `Attribute` values, an optional `Proplist`, and ordered
query words. Build query words with `Equal`, `LessThan`, `GreaterThan`,
`Exists`, `Absent`, `Not`, `And`, and `Or`; arbitrary field names and values
remain valid inputs. `Query` also accepts raw words beginning with `?` when a
caller needs the RouterOS query language directly.

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
| `StreamMaxRecords` | 1024 |
| `StreamMaxBytes` | 8 MiB |
| `MaxBufferedStreamBytes` | 64 MiB |
| `MaxOpenStreams` | 16 |
| `CancelGrace` | 2 seconds |

`ConnectTimeout` and `CommandTimeout` default to 10 seconds each.

## Go version

The minimum supported version is Go 1.24. The minimum and the latest stable Go
release are both tested before a release, and the minimum is raised only
together with a compatibility note.

## Licence

MIT.
