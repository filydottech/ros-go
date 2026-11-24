// Package transport owns one RouterOS connection: dialling, login, the reader
// and writer goroutines, and tag routing. It sits below package routeros and
// reports its own error values; the client layer converts them.
package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/filydottech/ros-go/internal/wire"
)

// ErrClosed reports work rejected or ended because the connection was closed
// locally. The client layer turns it into a *routeros.ClosedError.
var ErrClosed = errors.New("transport: connection closed")

// ErrReconnected reports work displaced because the client replaced this
// connection generation. The client layer turns it into a *routeros.ReconnectError.
var ErrReconnected = errors.New("transport: connection replaced by reconnect")

// LimitError reports a stream resource limit. The client layer turns it into a
// *routeros.ResourceLimitError.
type LimitError struct {
	Scope    string
	Limit    int64
	Observed int64
}

func (e *LimitError) Error() string {
	return fmt.Sprintf("transport: %s exceeds limit %d (observed %d)", e.Scope, e.Limit, e.Observed)
}

// LoginTrap reports a !trap reply to the login exchange. Message is the router
// text and never contains the submitted password.
type LoginTrap struct {
	Message string
}

func (e *LoginTrap) Error() string { return "transport: login rejected: " + e.Message }

// Fatal reports a !fatal reply. It ends the connection generation.
type Fatal struct {
	Reason string
}

func (e *Fatal) Error() string { return "transport: fatal: " + e.Reason }

// Options is everything Open needs. The client layer normalizes a public
// Config into these values, so the transport never sees a public type.
type Options struct {
	// Address is host:port.
	Address string
	// TLSConfig enables TLS when non-nil. Its ServerName must be set.
	TLSConfig *tls.Config
	// Username and Password are sent by the one-message login. Password is
	// only referenced during Open.
	Username string
	Password []byte
	// ConnectTimeout bounds dialling, the TLS handshake, and login.
	ConnectTimeout time.Duration
	// CancelGrace bounds how long a cancelled tag is remembered before the
	// cancellation is considered expired. A zero value keeps the tag until the
	// router answers or the connection ends.
	CancelGrace time.Duration
	// Limits bounds the codec in both directions.
	Limits wire.Limits
	// StreamMaxRecords and StreamMaxBytes bound one stream's record queue.
	StreamMaxRecords int
	StreamMaxBytes   int
	// MaxBufferedStreamBytes bounds every queued stream record on this
	// connection together.
	MaxBufferedStreamBytes int
	// MaxOpenStreams bounds concurrent stream commands.
	MaxOpenStreams int
	// Dial replaces the TCP dialer. Tests use it to supply an in-memory
	// connection; nil dials Address over TCP.
	Dial func(ctx context.Context, address string) (net.Conn, error)
}

// Pending is one submitted sentence awaiting its replies. A command uses the
// bounded Replies channel; a stream uses its bounded queue through NextStream.
// Done closes once the tag is resolved or the connection ends.
type Pending struct {
	tag     string
	replies chan wire.Reply
	acked   chan error
	flushed chan struct{}
	once    sync.Once
	err     error
	trap    *wire.Reply
	done    *wire.Reply
	// cancelled is guarded by Conn.mu. The writer checks it before committing
	// the sentence to the wire, so a cancellation that wins skips the write.
	cancelled     bool
	writeStarted  bool
	graceTimer    *time.Timer
	stream        *streamQueue
	cancelledDone chan struct{}
	cancelOnce    sync.Once
}

// Tag returns the tag the sentence was sent with.
func (p *Pending) Tag() string { return p.tag }

// Replies returns the channel of replies addressed to this tag. It is never
// closed, so read it together with Done.
func (p *Pending) Replies() <-chan wire.Reply { return p.replies }

// Written reports the result of writing the sentence: a nil value means the
// whole sentence reached the connection. If the connection ends before the
// writer starts the job, no value arrives, so read Written together with Done.
func (p *Pending) Written() <-chan error { return p.acked }

// Done closes when the request is resolved: normally by !done, otherwise by
// the connection's terminal error.
func (p *Pending) Done() <-chan struct{} { return p.flushed }

// Err returns the resolution error. It is only meaningful after Done closes
// and is nil for a normal !done resolution.
func (p *Pending) Err() error { return p.err }

// Trap returns the last !trap reply received before the terminating !done. It
// is only meaningful after Done closes. A trap is retained rather than
// delivered so that its attributes can be read together with the !done record.
func (p *Pending) Trap() (wire.Reply, bool) {
	if p.trap == nil {
		return wire.Reply{}, false
	}
	return *p.trap, true
}

// DoneReply returns the !done reply that resolved the request. It is only
// meaningful after Done closes and Err is nil.
func (p *Pending) DoneReply() (wire.Reply, bool) {
	if p.done == nil {
		return wire.Reply{}, false
	}
	return *p.done, true
}

func (p *Pending) finish(err error) {
	p.once.Do(func() {
		p.err = err
		close(p.flushed)
	})
}

func (p *Pending) finishCancellation() {
	p.cancelOnce.Do(func() { close(p.cancelledDone) })
}

type writeJob struct {
	sentence []byte
	ack      chan error
	// pending is nil for the client's own /cancel sentence.
	pending      *Pending
	cancelTarget *Pending
}

const (
	replyBuffer = 64
	writeBuffer = 64
	readBuffer  = 32 << 10
)

// Conn is one authenticated connection. One reader goroutine owns the decoder
// and one writer goroutine owns every Write; both end when the connection
// terminates.
type Conn struct {
	conn    net.Conn
	limits  wire.Limits
	encoder *wire.Encoder

	mu      sync.Mutex
	pending map[string]*Pending
	// cancelling holds tags whose caller was already resolved by Cancel. Their
	// replies are absorbed until !done or the grace period expires.
	cancelling map[string]*Pending
	// zombies holds expired or resolved cancellation tags. A late reply for one
	// is absorbed instead of being reported as desynchronization.
	zombies    map[string]struct{}
	grace      time.Duration
	counter    uint64
	cancelAcks uint64

	streamMaxRecords       int
	streamMaxBytes         int
	maxBufferedStreamBytes int
	maxOpenStreams         int
	openStreams            int
	bufferedStreamBytes    int
	pauseCount             int
	aggregatePaused        bool
	pauseGate              chan struct{}

	terminal error

	done      chan struct{}
	closeOnce sync.Once
	writes    chan *writeJob
	wg        sync.WaitGroup
}

// Open dials, optionally completes the TLS handshake, logs in, and starts the
// reader and writer goroutines. A returned error means no connection is left
// open.
func Open(ctx context.Context, opts Options) (*Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.TLSConfig != nil && opts.TLSConfig.ServerName == "" {
		return nil, errors.New("transport: TLS config has no server name")
	}
	conn, err := dial(ctx, opts)
	if err != nil {
		return nil, err
	}
	if err := login(ctx, conn, opts); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("transport: clear deadline: %w", err)
	}

	c := &Conn{
		conn:                   conn,
		limits:                 opts.Limits,
		encoder:                wire.NewEncoder(opts.Limits),
		pending:                make(map[string]*Pending),
		cancelling:             make(map[string]*Pending),
		zombies:                make(map[string]struct{}),
		grace:                  opts.CancelGrace,
		streamMaxRecords:       opts.StreamMaxRecords,
		streamMaxBytes:         opts.StreamMaxBytes,
		maxBufferedStreamBytes: opts.MaxBufferedStreamBytes,
		maxOpenStreams:         opts.MaxOpenStreams,
		done:                   make(chan struct{}),
		writes:                 make(chan *writeJob, writeBuffer),
		pauseGate:              make(chan struct{}),
	}
	c.wg.Add(2)
	go c.readLoop()
	go c.writeLoop()
	return c, nil
}

// Close ends the connection generation and waits for the reader and writer.
// Every pending request fails with ErrClosed. It is safe to call more than
// once.
func (c *Conn) Close() error {
	return c.CloseWith(ErrClosed)
}

// CloseWith ends the connection generation with err and waits for the reader
// and writer. A nil err is treated as ErrClosed.
func (c *Conn) CloseWith(err error) error {
	if err == nil {
		err = ErrClosed
	}
	c.terminate(err)
	c.wg.Wait()
	return nil
}

// Done closes when the connection generation ends.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Err returns the terminal error, or nil while the connection is healthy.
func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.terminal
}

// Submit registers one sentence for writing and returns its Pending. The word
// slice must not already contain a tag: Submit appends one. A sentence whose
// context has already ended is not registered or sent.
func (c *Conn) Submit(ctx context.Context, words []string) (*Pending, error) {
	return c.submit(ctx, words, false)
}

// SubmitStream registers one long-running sentence with a bounded reply queue.
// Its returned Pending must be consumed with NextStream.
func (c *Conn) SubmitStream(ctx context.Context, words []string) (*Pending, error) {
	return c.submit(ctx, words, true)
}

func (c *Conn) submit(ctx context.Context, words []string, streaming bool) (*Pending, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.terminal != nil {
		err := c.terminal
		c.mu.Unlock()
		return nil, err
	}
	if streaming {
		if c.maxOpenStreams < 1 || c.streamMaxRecords < 1 || c.streamMaxBytes < 1 || c.maxBufferedStreamBytes < 1 {
			c.mu.Unlock()
			return nil, errors.New("transport: stream limits are not configured")
		}
		if c.openStreams >= c.maxOpenStreams {
			err := &LimitError{Scope: "open streams", Limit: int64(c.maxOpenStreams), Observed: int64(c.openStreams + 1)}
			c.mu.Unlock()
			return nil, err
		}
	}
	c.counter++
	tag := strconv.FormatUint(c.counter, 10)
	p := &Pending{
		tag:           tag,
		replies:       make(chan wire.Reply, replyBuffer),
		acked:         make(chan error, 1),
		flushed:       make(chan struct{}),
		cancelledDone: make(chan struct{}),
	}
	if streaming {
		p.stream = newStreamQueue(c, c.streamMaxRecords, c.streamMaxBytes)
		c.openStreams++
	}
	c.pending[tag] = p
	c.mu.Unlock()

	tagged := make([]string, len(words)+1)
	copy(tagged, words)
	tagged[len(words)] = ".tag=" + tag
	sentence, err := c.encoder.Sentence(tagged)
	if err != nil {
		c.discard(p, err)
		return nil, err
	}

	select {
	case c.writes <- &writeJob{sentence: sentence, ack: p.acked, pending: p}:
		return p, nil
	case <-c.done:
		c.discard(p, c.Err())
		return nil, c.Err()
	case <-ctx.Done():
		c.discard(p, ctx.Err())
		return nil, ctx.Err()
	}
}

// Cancel resolves p with cause and asks the router to cancel its tag. The tag
// is retained as a zombie until !done arrives, so a late reply is absorbed
// instead of being treated as desynchronization. Cancelling a request that is
// already resolved does nothing.
func (c *Conn) Cancel(p *Pending, cause error) <-chan struct{} {
	c.mu.Lock()
	if _, ok := c.pending[p.tag]; !ok {
		if _, cancelling := c.cancelling[p.tag]; cancelling {
			wait := p.cancelledDone
			c.mu.Unlock()
			return wait
		}
		wait := p.flushed
		c.mu.Unlock()
		return wait
	}
	c.removePendingLocked(p)
	p.cancelled = true
	c.cancelling[p.tag] = p
	grace := c.grace
	if grace > 0 {
		p.graceTimer = time.AfterFunc(grace, func() { c.expireCancel(p.tag) })
	}
	if p.stream != nil {
		p.stream.failLocked(cause)
	}
	c.mu.Unlock()

	p.finish(cause)
	c.sendCancel(p)
	return p.cancelledDone
}

func (c *Conn) expireCancel(tag string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p, ok := c.cancelling[tag]; ok {
		delete(c.cancelling, tag)
		c.zombies[tag] = struct{}{}
		p.finishCancellation()
	}
}

// sendCancel queues a tagless /cancel sentence. Its untagged !done is counted
// separately from replies for the command being cancelled.
func (c *Conn) sendCancel(p *Pending) {
	select {
	case c.writes <- &writeJob{ack: make(chan error, 1), cancelTarget: p}:
	case <-c.done:
	}
}

func (c *Conn) forgetUnsentCancel(p *Pending) {
	c.mu.Lock()
	delete(c.cancelling, p.tag)
	delete(c.zombies, p.tag)
	if p.graceTimer != nil {
		p.graceTimer.Stop()
		p.graceTimer = nil
	}
	p.finishCancellation()
	c.mu.Unlock()
}

func (c *Conn) discard(p *Pending, err error) {
	c.mu.Lock()
	c.removePendingLocked(p)
	p.cancelled = true
	if p.stream != nil {
		p.stream.failLocked(err)
	}
	c.mu.Unlock()
	p.finish(err)
	if err == nil {
		err = ErrClosed
	}
	p.acked <- err
}

func (c *Conn) readLoop() {
	defer c.wg.Done()

	decoder := wire.NewDecoder(c.limits)
	buf := make([]byte, readBuffer)
	for {
		if !c.waitForReadGate() {
			return
		}
		n, readErr := c.conn.Read(buf)
		if n > 0 {
			sentences, err := decoder.Feed(buf[:n])
			if err != nil {
				c.terminate(err)
				return
			}
			for _, words := range sentences {
				reply, err := wire.ParseReply(words)
				if err != nil {
					c.terminate(err)
					return
				}
				if !c.route(reply) {
					return
				}
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				readErr = io.ErrUnexpectedEOF
			}
			c.terminate(fmt.Errorf("transport: read: %w", readErr))
			return
		}
	}
}

func (c *Conn) route(reply wire.Reply) bool {
	if reply.Kind == wire.KindFatal {
		c.terminate(&Fatal{Reason: reply.FatalReason})
		return false
	}

	c.mu.Lock()
	if !reply.HasTag {
		isCancelAck := reply.Kind == wire.KindDone && c.cancelAcks > 0
		if isCancelAck {
			c.cancelAcks--
		}
		c.mu.Unlock()
		if isCancelAck {
			return true
		}
		c.terminate(&wire.ProtocolError{Detail: fmt.Sprintf("untagged %s reply", reply.Kind)})
		return false
	}

	pending := c.pending[reply.Tag]
	cancellingPending, cancelling := c.cancelling[reply.Tag]
	_, zombie := c.zombies[reply.Tag]
	if reply.Kind == wire.KindDone {
		delete(c.cancelling, reply.Tag)
		delete(c.zombies, reply.Tag)
		if cancellingPending != nil {
			if cancellingPending.graceTimer != nil {
				cancellingPending.graceTimer.Stop()
				cancellingPending.graceTimer = nil
			}
			cancellingPending.finishCancellation()
		}
	}
	c.mu.Unlock()

	if pending == nil {
		if cancelling || zombie {
			return true
		}
		c.terminate(&wire.ProtocolError{Detail: fmt.Sprintf("reply for unknown tag %q", reply.Tag)})
		return false
	}
	return c.deliver(pending, reply)
}

func (c *Conn) deliver(p *Pending, reply wire.Reply) bool {
	switch reply.Kind {
	case wire.KindReply:
		if p.stream != nil {
			return c.deliverStream(p, reply)
		}
		select {
		case p.replies <- reply:
		case <-p.flushed:
			// The caller was resolved, for example by cancellation, and may
			// never read again. Drop the record instead of blocking the reader
			// on a full buffer and stalling every later reply.
		case <-c.done:
			return false
		}
	case wire.KindTrap:
		stored := reply
		p.trap = &stored
	case wire.KindDone:
		stored := reply
		p.done = &stored
	}
	if reply.Kind != wire.KindDone {
		return true
	}

	c.mu.Lock()
	c.removePendingLocked(p)
	if p.stream != nil {
		p.stream.closeLocked()
	}
	c.mu.Unlock()
	p.finish(nil)
	return true
}

func (c *Conn) deliverStream(p *Pending, reply wire.Reply) bool {
	bytes := streamReplyBytes(reply)
	queue := p.stream
	if bytes > queue.maxBytes {
		c.Cancel(p, &LimitError{Scope: "stream bytes", Limit: int64(queue.maxBytes), Observed: int64(bytes)})
		return true
	}

	for {
		c.mu.Lock()
		if c.terminal != nil {
			c.mu.Unlock()
			return false
		}
		if p.cancelled || queue.closed {
			c.mu.Unlock()
			return true
		}
		if !queue.paused && c.bufferedStreamBytes+bytes <= c.maxBufferedStreamBytes {
			queue.appendLocked(reply, bytes)
			c.mu.Unlock()
			return true
		}
		if c.bufferedStreamBytes+bytes > c.maxBufferedStreamBytes {
			c.setAggregatePausedLocked(true)
		}
		gate := c.pauseGate
		c.mu.Unlock()

		select {
		case <-gate:
		case <-c.done:
			return false
		}
	}
}

func (c *Conn) writeLoop() {
	defer c.wg.Done()

	for {
		select {
		case job := <-c.writes:
			if job.pending != nil {
				c.mu.Lock()
				abandoned := job.pending.cancelled
				if !abandoned {
					job.pending.writeStarted = true
				}
				c.mu.Unlock()
				if abandoned {
					// The caller already has its cancellation result and no
					// acknowledgement is owed, so a cancelled mutation never
					// reaches the router.
					continue
				}
			}
			if job.cancelTarget != nil {
				c.mu.Lock()
				sent := job.cancelTarget.writeStarted
				c.mu.Unlock()
				if !sent {
					c.forgetUnsentCancel(job.cancelTarget)
					continue
				}
				sentence, err := c.encoder.Sentence([]string{"/cancel", "=tag=" + job.cancelTarget.tag})
				if err != nil {
					continue
				}
				job.sentence = sentence
				c.mu.Lock()
				c.cancelAcks++
				c.mu.Unlock()
			}
			err := writeFull(c.conn, job.sentence)
			job.ack <- err
			if err != nil {
				c.terminate(fmt.Errorf("transport: write: %w", err))
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *Conn) terminate(err error) {
	c.mu.Lock()
	if c.terminal != nil {
		c.mu.Unlock()
		return
	}
	c.terminal = err
	pending := make([]*Pending, 0, len(c.pending))
	for _, p := range c.pending {
		pending = append(pending, p)
		c.removePendingLocked(p)
		if p.stream != nil {
			p.stream.failLocked(err)
		}
	}
	cancelling := make([]*Pending, 0, len(c.cancelling))
	for _, p := range c.cancelling {
		cancelling = append(cancelling, p)
		if p.graceTimer != nil {
			p.graceTimer.Stop()
			p.graceTimer = nil
		}
	}
	clear(c.cancelling)
	clear(c.zombies)
	c.cancelAcks = 0
	close(c.done)
	c.mu.Unlock()

	for _, p := range pending {
		p.finish(err)
	}
	for _, p := range cancelling {
		p.finishCancellation()
	}
	c.closeOnce.Do(func() {
		_ = c.conn.Close()
	})
}

func (c *Conn) removePendingLocked(p *Pending) {
	if c.pending[p.tag] != p {
		return
	}
	delete(c.pending, p.tag)
	if p.stream != nil {
		c.openStreams--
	}
}

func (c *Conn) waitForReadGate() bool {
	for {
		c.mu.Lock()
		if c.terminal != nil {
			c.mu.Unlock()
			return false
		}
		if c.pauseCount == 0 {
			c.mu.Unlock()
			return true
		}
		gate := c.pauseGate
		c.mu.Unlock()

		select {
		case <-gate:
		case <-c.done:
			return false
		}
	}
}

func (c *Conn) updateQueuePauseLocked(queue *streamQueue) {
	if queue.paused {
		if queue.isDrainedLocked() {
			c.setQueuePausedLocked(queue, false)
		}
		return
	}
	if queue.isFullLocked() {
		c.setQueuePausedLocked(queue, true)
	}
}

func (c *Conn) setQueuePausedLocked(queue *streamQueue, paused bool) {
	if queue.paused == paused {
		return
	}
	queue.paused = paused
	if paused {
		c.pauseCount++
	} else {
		c.pauseCount--
	}
	c.signalPauseLocked()
}

func (c *Conn) updateAggregatePauseLocked() {
	if c.aggregatePaused {
		if c.bufferedStreamBytes <= c.maxBufferedStreamBytes/2 {
			c.setAggregatePausedLocked(false)
		}
		return
	}
	if c.bufferedStreamBytes >= c.maxBufferedStreamBytes {
		c.setAggregatePausedLocked(true)
	}
}

func (c *Conn) setAggregatePausedLocked(paused bool) {
	if c.aggregatePaused == paused {
		return
	}
	c.aggregatePaused = paused
	if paused {
		c.pauseCount++
	} else {
		c.pauseCount--
	}
	c.signalPauseLocked()
}

func (c *Conn) signalPauseLocked() {
	close(c.pauseGate)
	c.pauseGate = make(chan struct{})
}

func dial(ctx context.Context, opts Options) (net.Conn, error) {
	var (
		conn net.Conn
		err  error
	)
	if opts.Dial != nil {
		conn, err = opts.Dial(ctx, opts.Address)
	} else {
		dialer := net.Dialer{Timeout: opts.ConnectTimeout}
		conn, err = dialer.DialContext(ctx, "tcp", opts.Address)
	}
	if err != nil {
		return nil, err
	}

	if opts.TLSConfig == nil {
		return conn, nil
	}

	secure := tls.Client(conn, opts.TLSConfig)
	deadline := time.Now().Add(opts.ConnectTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := secure.SetDeadline(deadline); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("transport: TLS deadline: %w", err)
	}
	if err := secure.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("transport: TLS handshake: %w", err)
	}
	return secure, nil
}

func login(ctx context.Context, conn net.Conn, opts Options) error {
	deadline := time.Now().Add(opts.ConnectTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return fmt.Errorf("transport: login deadline: %w", err)
	}

	sentence, err := wire.NewEncoder(opts.Limits).Sentence([]string{
		"/login",
		"=name=" + opts.Username,
		"=password=" + string(opts.Password),
	})
	if err != nil {
		return err
	}
	if err := writeFull(conn, sentence); err != nil {
		return fmt.Errorf("transport: write login: %w", err)
	}

	decoder := wire.NewDecoder(opts.Limits)
	buf := make([]byte, readBuffer)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := conn.Read(buf)
		if n > 0 {
			sentences, err := decoder.Feed(buf[:n])
			if err != nil {
				return err
			}
			for _, words := range sentences {
				reply, err := wire.ParseReply(words)
				if err != nil {
					return err
				}
				switch reply.Kind {
				case wire.KindDone:
					return nil
				case wire.KindTrap:
					message := reply.Attributes["message"]
					if message == "" {
						message = "login rejected"
					}
					return &LoginTrap{Message: message}
				case wire.KindFatal:
					return &Fatal{Reason: reply.FatalReason}
				default:
					return fmt.Errorf("transport: unexpected %s during login", reply.Kind)
				}
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				readErr = io.ErrUnexpectedEOF
			}
			return fmt.Errorf("transport: read login reply: %w", readErr)
		}
	}
}

// writeFull writes every byte of sentence, looping over short writes. A write
// that reports no progress without an error is io.ErrShortWrite.
func writeFull(conn net.Conn, sentence []byte) error {
	for len(sentence) > 0 {
		n, err := conn.Write(sentence)
		if n > 0 {
			sentence = sentence[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
