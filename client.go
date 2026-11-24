// Package routeros is a RouterOS native API client over TCP and TLS.
//
// A Client owns one connection. Requests are tagged and multiplexed, results
// keep their wire order, and streams are bounded. Dial connects and logs in;
// Do runs one command, Stream receives a long-running command, and Reconnect
// is not implemented yet.
package routeros

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/filydottech/ros-go/internal/transport"
	"github.com/filydottech/ros-go/internal/wire"
)

var (
	// errNoReconnect marks Reconnect, whose implementation lands in the
	// lifecycle phase.
	errNoReconnect = errors.New("reconnect is not implemented yet")
)

// openTransport is a test seam. Production always dials over the network.
var openTransport = transport.Open

// State is the observable lifecycle state of a Client. It is informational:
// no method forces a state apart from the documented lifecycle methods.
type State int

const (
	// Connecting is reported while a dial is in progress.
	Connecting State = iota
	// LoggingIn is reported while the login exchange is in progress.
	LoggingIn
	// Ready is reported while the connection accepts commands.
	Ready
	// Reconnecting is reported while Reconnect replaces the connection.
	Reconnecting
	// Closing is reported while Close releases the connection.
	Closing
	// Closed is reported after the connection generation ended.
	Closed
)

func (s State) String() string {
	switch s {
	case Connecting:
		return "connecting"
	case LoggingIn:
		return "logging in"
	case Ready:
		return "ready"
	case Reconnecting:
		return "reconnecting"
	case Closing:
		return "closing"
	case Closed:
		return "closed"
	}
	return fmt.Sprintf("State(%d)", int(s))
}

// Client is one RouterOS API connection.
type Client struct {
	mu       sync.Mutex
	cfg      Config
	state    State
	conn     *transport.Conn
	done     chan struct{}
	doneOnce sync.Once
	err      error
	closed   bool
}

// Dial validates cfg and opens one TLS or plaintext connection to the router.
//
// TLS is the default: the port is 8729, the certificate chain is verified, and
// the hostname in cfg.Host is checked. A caller who sets Plaintext opts into an
// unencrypted connection, which then defaults to port 8728.
//
// Dial dials, completes the TLS handshake when TLS is enabled, then logs in
// with one /login message. The password is not retained by the client after a
// successful login. A failed dial returns a *ConnectionError; a login !trap
// returns a *LoginError. A context that has already ended returns a
// *CancelledError and sends nothing.
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	cfg, err := cfg.normalize()
	if err != nil {
		return nil, &ConnectionError{Op: "dial", Err: err}
	}
	if err := ctx.Err(); err != nil {
		return nil, &CancelledError{Cause: err}
	}

	options := transport.Options{
		Address:        net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Username:       cfg.Username,
		Password:       cfg.Password,
		ConnectTimeout: cfg.ConnectTimeout,
		CancelGrace:    cfg.CancelGrace,
		Limits: wire.Limits{
			MaxWordBytes:        cfg.MaxWordBytes,
			MaxSentenceBytes:    cfg.MaxSentenceBytes,
			MaxWordsPerSentence: cfg.MaxWordsPerSentence,
		},
		StreamMaxRecords:       cfg.StreamMaxRecords,
		StreamMaxBytes:         cfg.StreamMaxBytes,
		MaxBufferedStreamBytes: cfg.MaxBufferedStreamBytes,
		MaxOpenStreams:         cfg.MaxOpenStreams,
	}
	if !cfg.Plaintext {
		options.TLSConfig = cfg.TLSConfig
	}

	client := newClient(cfg)
	client.setState(LoggingIn)
	conn, err := openTransport(ctx, options)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, &CancelledError{Cause: ctxErr}
		}
		return nil, mapTransportError("dial", cfg.ConnectTimeout, err)
	}

	client.mu.Lock()
	client.conn = conn
	client.cfg.Password = nil
	client.state = Ready
	client.mu.Unlock()

	go client.watch(conn, cfg.CommandTimeout)
	return client, nil
}

// Do sends one command and returns every !re record in arrival order together
// with the final !done attributes.
//
// The context owns the command lifecycle: one that has ended before the write
// sends nothing, and one that ends after the write starts remote cancellation
// but returns a *CancelledError without waiting for the router. A command that
// exceeds Config.CommandTimeout starts the same cancellation and returns a
// *TimeoutError.
//
// A !trap is reported as a *TrapError that carries the !done attributes. A path
// ending in /add without a ret attribute in !done is a *ProtocolError for that
// command only; the connection stays usable. A request that fails validation
// returns a *ConnectionError wrapping the validation error and sends nothing.
func (c *Client) Do(ctx context.Context, path string, req Request) (Result, error) {
	words, err := req.words(path)
	if err != nil {
		return Result{}, &ConnectionError{Op: "do", Err: err}
	}

	c.mu.Lock()
	conn := c.conn
	closed := c.closed
	terminal := c.err
	timeout := c.cfg.CommandTimeout
	c.mu.Unlock()

	if closed {
		return Result{}, &ClosedError{}
	}
	if terminal != nil {
		return Result{}, terminal
	}
	if conn == nil {
		return Result{}, &ConnectionError{Op: "do", Err: errors.New("client has no connection")}
	}

	pending, err := conn.Submit(ctx, words)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Result{}, &CancelledError{Cause: ctxErr}
		}
		return Result{}, mapTransportError("do", timeout, err)
	}

	select {
	case writeErr := <-pending.Written():
		if writeErr != nil {
			<-pending.Done()
			return finishCommand(path, pending, nil, timeout)
		}
	case <-ctx.Done():
		conn.Cancel(pending, ctx.Err())
		return Result{}, &CancelledError{Cause: ctx.Err()}
	case <-pending.Done():
		// The reply can beat the write acknowledgement on a fast link, so a
		// finished request is completed rather than treated as a write failure.
		return finishCommand(path, pending, nil, timeout)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	var records []Record
	for {
		select {
		case reply := <-pending.Replies():
			records = append(records, Record(reply.Attributes))
		case <-pending.Done():
			return finishCommand(path, pending, records, timeout)
		case <-ctx.Done():
			conn.Cancel(pending, ctx.Err())
			return Result{}, &CancelledError{Cause: ctx.Err()}
		case <-timer.C:
			timeoutErr := &TimeoutError{Phase: "command", Duration: timeout}
			conn.Cancel(pending, timeoutErr)
			return Result{}, timeoutErr
		}
	}
}

// finishCommand turns a resolved pending request into a Result or its typed
// error. It runs after Done closes, when the retained trap and done replies and
// the resolution error are stable.
func finishCommand(path string, pending *transport.Pending, records []Record, timeout time.Duration) (Result, error) {
	// Every record queued before the terminal reply belongs to the result, even
	// when the terminal reply arrived before the caller started reading.
drain:
	for {
		select {
		case reply := <-pending.Replies():
			records = append(records, Record(reply.Attributes))
		default:
			break drain
		}
	}

	if err := pending.Err(); err != nil {
		return Result{}, mapTransportError("do", timeout, err)
	}

	done := Record{}
	if reply, ok := pending.DoneReply(); ok {
		done = Record(reply.Attributes)
	}
	if trap, ok := pending.Trap(); ok {
		return Result{}, &TrapError{
			Category: trap.Attributes["category"],
			Message:  trap.Attributes["message"],
			Done:     done,
		}
	}
	if strings.HasSuffix(path, "/add") {
		if _, ok := done["ret"]; !ok {
			return Result{}, &ProtocolError{Detail: path + " reply has no ret attribute"}
		}
	}
	return Result{Records: records, Done: done}, nil
}

// Stream sends one command and returns a bounded stream of !re records.
//
// The context owns the stream lifecycle. Recv returns io.EOF only for a normal
// !done; a trap, cancellation, peer close, and Close each return a distinct
// typed error.
func (c *Client) Stream(ctx context.Context, path string, req Request) (*Stream, error) {
	words, err := req.words(path)
	if err != nil {
		return nil, &ConnectionError{Op: "stream", Err: err}
	}

	c.mu.Lock()
	conn := c.conn
	closed := c.closed
	terminal := c.err
	timeout := c.cfg.CommandTimeout
	c.mu.Unlock()

	if closed {
		return nil, &ClosedError{}
	}
	if terminal != nil {
		return nil, terminal
	}
	if conn == nil {
		return nil, &ConnectionError{Op: "stream", Err: errors.New("client has no connection")}
	}

	pending, err := conn.SubmitStream(ctx, words)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, &CancelledError{Cause: ctxErr}
		}
		return nil, mapTransportError("stream", timeout, err)
	}

	select {
	case writeErr := <-pending.Written():
		if writeErr != nil {
			<-pending.Done()
			return nil, mapTransportError("stream", timeout, pending.Err())
		}
	case <-ctx.Done():
		conn.Cancel(pending, ctx.Err())
		return nil, &CancelledError{Cause: ctx.Err()}
	case <-pending.Done():
		select {
		case writeErr := <-pending.Written():
			if writeErr != nil {
				return nil, mapTransportError("stream", timeout, pending.Err())
			}
		case <-conn.Done():
			return nil, mapTransportError("stream", timeout, pending.Err())
		}
	}

	stream := newStream(conn, pending, timeout)
	go stream.watchContext(ctx)
	return stream, nil
}

// Reconnect replaces the connection using cfg. It rejects a concurrent
// reconnect, fails the old generation with *ReconnectError, then dials and logs
// in again. It never replays a command or resubscribes a stream.
//
// A failed reconnect leaves the client closed but retryable. Close during a
// reconnect cancels the attempt and is final.
func (c *Client) Reconnect(ctx context.Context, cfg Config) error {
	if _, err := cfg.normalize(); err != nil {
		return &ConnectionError{Op: "reconnect", Err: err}
	}
	if err := ctx.Err(); err != nil {
		return &CancelledError{Cause: err}
	}
	return &ConnectionError{Op: "reconnect", Err: errNoReconnect}
}

// Close releases the connection. It is final and idempotent: later calls
// return nil and every operation on the client fails with *ClosedError.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.state = Closing
	conn := c.conn
	c.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	c.shutdown(&ClosedError{})
	return nil
}

// State returns the current lifecycle state.
func (c *Client) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// Done returns the channel for the connection generation active when the
// method is called. The channel closes when that generation ends.
func (c *Client) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// Err returns the terminal error for the active connection generation, or nil
// while the generation is healthy.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// watch reports the end of the connection generation on the client.
func (c *Client) watch(conn *transport.Conn, timeout time.Duration) {
	<-conn.Done()
	c.shutdown(mapTransportError("connection", timeout, conn.Err()))
}

// shutdown records the terminal error and closes Done exactly once.
func (c *Client) shutdown(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.err = err
	}
	c.state = Closed
	c.doneOnce.Do(func() { close(c.done) })
}

func (c *Client) setState(state State) {
	c.mu.Lock()
	c.state = state
	c.mu.Unlock()
}

// mapTransportError converts a transport value into the public error category.
// It never adds configuration values, so no credential can appear in a
// message.
func mapTransportError(op string, timeout time.Duration, err error) error {
	if err == nil {
		return nil
	}

	var protocolErr *wire.ProtocolError
	if errors.As(err, &protocolErr) {
		return &ProtocolError{Detail: protocolErr.Detail}
	}
	var limitErr *wire.LimitError
	if errors.As(err, &limitErr) {
		return &ResourceLimitError{Scope: limitErr.Scope, Limit: limitErr.Limit, Observed: limitErr.Observed}
	}
	var streamLimit *transport.LimitError
	if errors.As(err, &streamLimit) {
		return &ResourceLimitError{Scope: streamLimit.Scope, Limit: streamLimit.Limit, Observed: streamLimit.Observed}
	}
	var loginTrap *transport.LoginTrap
	if errors.As(err, &loginTrap) {
		return &LoginError{Message: loginTrap.Message}
	}
	var fatal *transport.Fatal
	if errors.As(err, &fatal) {
		return &FatalError{Reason: fatal.Reason}
	}
	if errors.Is(err, transport.ErrClosed) {
		return &ClosedError{}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return &TimeoutError{Phase: op, Duration: timeout}
	}
	return &ConnectionError{Op: op, Err: err}
}

// newClient builds the client a successful dial returns. It exists so the
// lifecycle can be tested without a connection.
func newClient(cfg Config) *Client {
	return &Client{cfg: cfg, state: Connecting, done: make(chan struct{})}
}
