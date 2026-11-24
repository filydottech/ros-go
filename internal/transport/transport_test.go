package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/filydottech/ros-go/internal/testkit"
	"github.com/filydottech/ros-go/internal/wire"
)

const testPassword = "not-a-real-password"

type fakeServer struct {
	t       *testing.T
	conn    net.Conn
	decoder *wire.Decoder
	queue   [][]string
}

func (s *fakeServer) next() []string {
	s.t.Helper()
	for len(s.queue) == 0 {
		if err := s.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			s.t.Fatalf("server read deadline: %v", err)
		}
		buf := make([]byte, 4096)
		n, err := s.conn.Read(buf)
		if err != nil {
			s.t.Fatalf("server read: %v", err)
		}
		sentences, err := s.decoder.Feed(buf[:n])
		if err != nil {
			s.t.Fatalf("server decode: %v", err)
		}
		s.queue = append(s.queue, sentences...)
	}
	words := s.queue[0]
	s.queue = s.queue[1:]
	return words
}

func (s *fakeServer) send(words ...string) {
	s.t.Helper()
	if err := s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		s.t.Fatalf("server write deadline: %v", err)
	}
	if _, err := s.conn.Write(testkit.EncodeSentence(words)); err != nil {
		s.t.Fatalf("server write: %v", err)
	}
}

// drain blocks until the peer closes, keeping a healthy test connection open
// for as long as the client under test needs it.
func (s *fakeServer) drain() {
	buf := make([]byte, 512)
	for {
		if _, err := s.conn.Read(buf); err != nil {
			return
		}
	}
}

func (s *fakeServer) login() []string {
	s.t.Helper()
	words := s.next()
	if len(words) != 3 || words[0] != "/login" {
		s.t.Fatalf("login sentence = %q, want /login with name and password", words)
	}
	s.send("!done")
	return words
}

func newOptions(t *testing.T, script func(*fakeServer)) Options {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	server := &fakeServer{t: t, conn: serverConn, decoder: wire.NewDecoder(wire.DefaultLimits())}

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer serverConn.Close()
		script(server)
	}()
	t.Cleanup(func() {
		_ = clientConn.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("fake server did not finish")
		}
	})

	return Options{
		Address:                "router.test:8729",
		Username:               "alice",
		Password:               []byte(testPassword),
		ConnectTimeout:         5 * time.Second,
		Limits:                 wire.DefaultLimits(),
		StreamMaxRecords:       1024,
		StreamMaxBytes:         8 << 20,
		MaxBufferedStreamBytes: 64 << 20,
		MaxOpenStreams:         16,
		Dial: func(context.Context, string) (net.Conn, error) {
			return clientConn, nil
		},
	}
}

func openConn(t *testing.T, script func(*fakeServer)) *Conn {
	t.Helper()
	conn, err := Open(context.Background(), newOptions(t, script))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func tagOf(t *testing.T, words []string) string {
	t.Helper()
	for _, word := range words {
		if strings.HasPrefix(word, ".tag=") {
			return strings.TrimPrefix(word, ".tag=")
		}
	}
	t.Fatalf("sentence %q has no tag", words)
	return ""
}

func collect(t *testing.T, p *Pending) ([]string, error) {
	t.Helper()
	var values []string
	for {
		select {
		case reply := <-p.Replies():
			if reply.Kind == wire.KindReply {
				values = append(values, reply.Attributes["value"])
			}
		case <-p.Done():
			for {
				select {
				case reply := <-p.Replies():
					if reply.Kind == wire.KindReply {
						values = append(values, reply.Attributes["value"])
					}
				default:
					return values, p.Err()
				}
			}
		}
	}
}

func TestLoginSendsNameAndPasswordInOneSentence(t *testing.T) {
	conn := openConn(t, func(s *fakeServer) {
		words := s.login()
		if words[1] != "=name=alice" {
			t.Errorf("login name word = %q, want =name=alice", words[1])
		}
		if words[2] != "=password="+testPassword {
			t.Errorf("login password word does not carry the configured password")
		}
		s.drain()
	})

	select {
	case <-conn.Done():
		t.Fatalf("connection ended after login: %v", conn.Err())
	default:
	}
	if err := conn.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func TestRoutesInterleavedReplies(t *testing.T) {
	conn := openConn(t, func(s *fakeServer) {
		s.login()
		first := tagOf(t, s.next())
		second := tagOf(t, s.next())
		s.send("!re", "=value=a1", ".tag="+first)
		s.send("!re", "=value=b1", ".tag="+second)
		s.send("!re", "=value=a2", ".tag="+first)
		s.send("!done", "=ret=*1", ".tag="+first)
		s.send("!done", ".tag="+second)
		s.drain()
	})

	first, err := conn.Submit(context.Background(), []string{"/print", "=detail="})
	if err != nil {
		t.Fatalf("Submit(first) error = %v", err)
	}
	second, err := conn.Submit(context.Background(), []string{"/interface/print"})
	if err != nil {
		t.Fatalf("Submit(second) error = %v", err)
	}
	if first.Tag() == second.Tag() {
		t.Fatalf("both requests received tag %q", first.Tag())
	}

	for _, p := range []*Pending{first, second} {
		if err := <-p.Written(); err != nil {
			t.Fatalf("write of tag %s error = %v", p.Tag(), err)
		}
	}

	gotFirst, err := collect(t, first)
	if err != nil {
		t.Fatalf("first request error = %v", err)
	}
	if strings.Join(gotFirst, ",") != "a1,a2" {
		t.Errorf("first request values = %q, want a1,a2", gotFirst)
	}

	gotSecond, err := collect(t, second)
	if err != nil {
		t.Fatalf("second request error = %v", err)
	}
	if strings.Join(gotSecond, ",") != "b1" {
		t.Errorf("second request values = %q, want b1", gotSecond)
	}
	if err := conn.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func TestStreamPauseGateWaitsForEveryFullQueue(t *testing.T) {
	opts := newOptions(t, func(s *fakeServer) {
		s.login()
		s.drain()
	})
	opts.StreamMaxRecords = 1
	conn, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	first, err := conn.SubmitStream(context.Background(), []string{"/first/listen"})
	if err != nil {
		t.Fatalf("SubmitStream(first) error = %v", err)
	}
	second, err := conn.SubmitStream(context.Background(), []string{"/second/listen"})
	if err != nil {
		t.Fatalf("SubmitStream(second) error = %v", err)
	}
	for _, pending := range []*Pending{first, second} {
		if err := <-pending.Written(); err != nil {
			t.Fatalf("stream write error = %v", err)
		}
	}

	reply := func(value string) wire.Reply {
		return wire.Reply{Kind: wire.KindReply, Attributes: map[string]string{"value": value}}
	}
	if !conn.deliverStream(first, reply("first")) {
		t.Fatal("deliverStream(first) returned false")
	}
	if !conn.deliverStream(second, reply("second")) {
		t.Fatal("deliverStream(second) returned false")
	}

	conn.mu.Lock()
	if conn.pauseCount != 2 {
		t.Errorf("pauseCount = %d, want 2 full stream queues", conn.pauseCount)
	}
	conn.mu.Unlock()

	if _, err := first.NextStream(); err != nil {
		t.Fatalf("first NextStream() error = %v", err)
	}
	conn.mu.Lock()
	if conn.pauseCount != 1 {
		t.Errorf("pauseCount after draining first queue = %d, want 1", conn.pauseCount)
	}
	conn.mu.Unlock()

	if _, err := second.NextStream(); err != nil {
		t.Fatalf("second NextStream() error = %v", err)
	}
	conn.mu.Lock()
	if conn.pauseCount != 0 {
		t.Errorf("pauseCount after draining both queues = %d, want 0", conn.pauseCount)
	}
	conn.mu.Unlock()
}

func TestStreamAggregateByteBudgetPausesAndDrains(t *testing.T) {
	opts := newOptions(t, func(s *fakeServer) {
		s.login()
		s.drain()
	})
	opts.StreamMaxRecords = 2
	opts.StreamMaxBytes = 4
	opts.MaxBufferedStreamBytes = 4
	conn, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	first, err := conn.SubmitStream(context.Background(), []string{"/first/listen"})
	if err != nil {
		t.Fatalf("SubmitStream(first) error = %v", err)
	}
	second, err := conn.SubmitStream(context.Background(), []string{"/second/listen"})
	if err != nil {
		t.Fatalf("SubmitStream(second) error = %v", err)
	}
	for _, pending := range []*Pending{first, second} {
		if err := <-pending.Written(); err != nil {
			t.Fatalf("stream write error = %v", err)
		}
	}

	reply := wire.Reply{Kind: wire.KindReply, Attributes: map[string]string{"v": "x"}}
	if !conn.deliverStream(first, reply) || !conn.deliverStream(second, reply) {
		t.Fatal("deliverStream() returned false")
	}

	conn.mu.Lock()
	if conn.bufferedStreamBytes != 4 {
		t.Errorf("bufferedStreamBytes = %d, want 4", conn.bufferedStreamBytes)
	}
	if !conn.aggregatePaused || conn.pauseCount != 1 {
		t.Errorf("aggregate pause = %v with count %d, want true with count 1", conn.aggregatePaused, conn.pauseCount)
	}
	conn.mu.Unlock()

	if _, err := first.NextStream(); err != nil {
		t.Fatalf("first NextStream() error = %v", err)
	}
	conn.mu.Lock()
	if conn.bufferedStreamBytes != 2 {
		t.Errorf("bufferedStreamBytes after drain = %d, want 2", conn.bufferedStreamBytes)
	}
	if conn.aggregatePaused || conn.pauseCount != 0 {
		t.Errorf("aggregate pause after drain = %v with count %d, want false with count 0", conn.aggregatePaused, conn.pauseCount)
	}
	conn.mu.Unlock()
}

func TestCancellingAFullStreamWakesItsReader(t *testing.T) {
	opts := newOptions(t, func(s *fakeServer) {
		s.login()
		command := s.next()
		tag := tagOf(t, command)
		cancel := s.next()
		if len(cancel) != 2 || cancel[0] != "/cancel" || cancel[1] != "=tag="+tag {
			t.Errorf("cancel sentence = %q, want /cancel =tag=%s", cancel, tag)
			return
		}
		s.send("!done")
		s.send("!done", ".tag="+tag)
		s.drain()
	})
	opts.StreamMaxRecords = 1
	conn, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	pending, err := conn.SubmitStream(context.Background(), []string{"/interface/listen"})
	if err != nil {
		t.Fatalf("SubmitStream() error = %v", err)
	}
	if err := <-pending.Written(); err != nil {
		t.Fatalf("stream write error = %v", err)
	}
	if !conn.deliverStream(pending, wire.Reply{Kind: wire.KindReply, Attributes: map[string]string{"value": "full"}}) {
		t.Fatal("deliverStream() returned false")
	}

	conn.mu.Lock()
	if conn.pauseCount != 1 {
		t.Errorf("pauseCount = %d, want 1 for the full stream", conn.pauseCount)
	}
	conn.mu.Unlock()

	cause := errors.New("consumer stopped")
	wait := conn.Cancel(pending, cause)
	if _, err := pending.NextStream(); !errors.Is(err, cause) {
		t.Errorf("NextStream() error = %v, want cancellation cause", err)
	}
	conn.mu.Lock()
	if conn.pauseCount != 0 {
		t.Errorf("pauseCount after cancellation = %d, want 0", conn.pauseCount)
	}
	conn.mu.Unlock()

	select {
	case <-wait:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not receive its terminal reply")
	}
}

type chunkConn struct {
	net.Conn
	chunk int
}

func (c *chunkConn) Write(b []byte) (int, error) {
	if len(b) > c.chunk {
		b = b[:c.chunk]
	}
	return c.Conn.Write(b)
}

func TestWriterCompletesShortWrites(t *testing.T) {
	received := make(chan []string, 1)
	opts := newOptions(t, func(s *fakeServer) {
		s.login()
		words := s.next()
		received <- words
		s.send("!done", ".tag="+tagOf(t, words))
		s.drain()
	})
	clientConn, err := opts.Dial(context.Background(), opts.Address)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	opts.Dial = func(context.Context, string) (net.Conn, error) {
		return &chunkConn{Conn: clientConn, chunk: 3}, nil
	}

	conn, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	p, err := conn.Submit(context.Background(), []string{"/print", "=detail="})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := <-p.Written(); err != nil {
		t.Fatalf("Written() error = %v", err)
	}

	select {
	case words := <-received:
		if len(words) != 3 || words[0] != "/print" || words[1] != "=detail=" {
			t.Fatalf("server received %q, want /print with detail and a tag", words)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive the command")
	}

	select {
	case <-p.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("request did not resolve")
	}
	if p.Err() != nil {
		t.Fatalf("Err() = %v, want nil", p.Err())
	}
}

func TestPeerCloseEndsPendingAndFutureWork(t *testing.T) {
	conn := openConn(t, func(s *fakeServer) {
		s.login()
		s.next()
	})

	if _, err := conn.Submit(context.Background(), []string{"/print"}); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	select {
	case <-conn.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("connection did not end after peer close")
	}
	if conn.Err() == nil {
		t.Fatal("Err() = nil, want a terminal error")
	}
	if _, err := conn.Submit(context.Background(), []string{"/print"}); err == nil {
		t.Fatal("Submit() after peer close error = nil, want the terminal error")
	}
}

func TestMalformedFrameIsTerminal(t *testing.T) {
	conn := openConn(t, func(s *fakeServer) {
		s.login()
		s.next()
		_, _ = s.conn.Write([]byte{0xf8})
	})

	p, err := conn.Submit(context.Background(), []string{"/print"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	select {
	case <-p.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("pending request did not fail")
	}
	var protocolErr *wire.ProtocolError
	if !errors.As(p.Err(), &protocolErr) {
		t.Fatalf("pending error = %v, want *wire.ProtocolError", p.Err())
	}
}

func TestFatalReplyIsTerminal(t *testing.T) {
	conn := openConn(t, func(s *fakeServer) {
		s.login()
		s.next()
		s.send("!fatal", "session", "closed")
	})

	p, err := conn.Submit(context.Background(), []string{"/print"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	select {
	case <-p.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("pending request did not fail")
	}
	var fatal *Fatal
	if !errors.As(p.Err(), &fatal) {
		t.Fatalf("pending error = %v, want *Fatal", p.Err())
	}
	if fatal.Reason != "session closed" {
		t.Errorf("Reason = %q, want %q", fatal.Reason, "session closed")
	}
}

func TestUntaggedReplyIsDesynchronization(t *testing.T) {
	conn := openConn(t, func(s *fakeServer) {
		s.login()
		s.next()
		s.send("!re", "=value=x")
	})

	p, err := conn.Submit(context.Background(), []string{"/print"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	<-p.Done()

	var protocolErr *wire.ProtocolError
	if !errors.As(p.Err(), &protocolErr) || !strings.Contains(protocolErr.Detail, "untagged") {
		t.Fatalf("pending error = %v, want an untagged protocol error", p.Err())
	}
}

func TestUnknownTagIsDesynchronization(t *testing.T) {
	conn := openConn(t, func(s *fakeServer) {
		s.login()
		s.next()
		s.send("!re", "=value=x", ".tag=999")
	})

	p, err := conn.Submit(context.Background(), []string{"/print"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	<-p.Done()

	var protocolErr *wire.ProtocolError
	if !errors.As(p.Err(), &protocolErr) || !strings.Contains(protocolErr.Detail, "unknown tag") {
		t.Fatalf("pending error = %v, want an unknown-tag protocol error", p.Err())
	}
}

func TestCloseFailsPendingAndRejectsWork(t *testing.T) {
	release := make(chan struct{})
	conn := openConn(t, func(s *fakeServer) {
		s.login()
		<-release
	})

	p, err := conn.Submit(context.Background(), []string{"/print"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	close(release)

	select {
	case <-p.Done():
	default:
		t.Fatal("pending request was not failed by Close")
	}
	if !errors.Is(p.Err(), ErrClosed) {
		t.Fatalf("pending error = %v, want ErrClosed", p.Err())
	}
	if err := conn.Close(); err != nil {
		t.Errorf("second Close() error = %v, want nil", err)
	}
	if _, err := conn.Submit(context.Background(), []string{"/print"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit() after Close error = %v, want ErrClosed", err)
	}
}

func TestLoginTrapDoesNotLeakPassword(t *testing.T) {
	_, err := Open(context.Background(), newOptions(t, func(s *fakeServer) {
		s.next()
		s.send("!trap", "=message=cannot log in")
	}))

	var trap *LoginTrap
	if !errors.As(err, &trap) {
		t.Fatalf("Open() error = %v, want *LoginTrap", err)
	}
	if trap.Message != "cannot log in" {
		t.Errorf("Message = %q, want %q", trap.Message, "cannot log in")
	}
	if strings.Contains(err.Error(), testPassword) {
		t.Errorf("login error %q must not contain the password", err)
	}
}

func TestLoginPeerCloseIsReported(t *testing.T) {
	_, err := Open(context.Background(), newOptions(t, func(s *fakeServer) {
		s.next()
	}))
	if err == nil {
		t.Fatal("Open() error = nil, want a read failure")
	}
	if strings.Contains(err.Error(), testPassword) {
		t.Errorf("login error %q must not contain the password", err)
	}
}

type zeroWriter struct{ net.Conn }

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

func TestWriteFullRejectsNoProgress(t *testing.T) {
	err := writeFull(zeroWriter{}, []byte("sentence"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeFull() error = %v, want io.ErrShortWrite", err)
	}
}

func TestEmptyReplyDoesNotResolveBeforeDone(t *testing.T) {
	emptySent := make(chan struct{})
	releaseDone := make(chan struct{})
	conn := openConn(t, func(s *fakeServer) {
		s.login()
		tag := tagOf(t, s.next())
		s.send("!re", "=value=one", ".tag="+tag)
		s.send("!empty", ".tag="+tag)
		close(emptySent)
		<-releaseDone
		s.send("!done", ".tag="+tag)
		s.drain()
	})

	p, err := conn.Submit(context.Background(), []string{"/print"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := <-p.Written(); err != nil {
		t.Fatalf("Written() error = %v", err)
	}
	<-emptySent

	select {
	case <-p.Done():
		t.Fatal("pending resolved on !empty, want it to wait for !done")
	default:
	}

	select {
	case reply := <-p.Replies():
		if reply.Kind != wire.KindReply || reply.Attributes["value"] != "one" {
			t.Fatalf("reply = %+v, want the !re record", reply)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("!re reply was not delivered")
	}

	close(releaseDone)
	select {
	case <-p.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("pending did not resolve after !done")
	}
	if err := p.Err(); err != nil {
		t.Errorf("Err() = %v, want nil", err)
	}
}

func TestTrapIsRetainedUntilDone(t *testing.T) {
	trapSent := make(chan struct{})
	releaseDone := make(chan struct{})
	conn := openConn(t, func(s *fakeServer) {
		s.login()
		tag := tagOf(t, s.next())
		s.send("!trap", "=category=0", "=message=no such item", ".tag="+tag)
		close(trapSent)
		<-releaseDone
		s.send("!done", "=ret=*1", ".tag="+tag)
		s.drain()
	})

	p, err := conn.Submit(context.Background(), []string{"/interface/print"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := <-p.Written(); err != nil {
		t.Fatalf("Written() error = %v", err)
	}
	<-trapSent

	select {
	case <-p.Done():
		t.Fatal("pending resolved on !trap, want it to wait for !done")
	default:
	}

	close(releaseDone)
	select {
	case <-p.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("pending did not resolve after !done")
	}
	if err := p.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil for a trap resolved by !done", err)
	}
	trap, ok := p.Trap()
	if !ok {
		t.Fatal("Trap() = false, want the retained !trap")
	}
	if trap.Attributes["category"] != "0" || trap.Attributes["message"] != "no such item" {
		t.Errorf("trap attributes = %+v, want category 0 and the message", trap.Attributes)
	}
	done, ok := p.DoneReply()
	if !ok || done.Attributes["ret"] != "*1" {
		t.Errorf("DoneReply() = %+v, %v, want ret=*1", done.Attributes, ok)
	}
}

func TestCancelResolvesCallerAndAbsorbsLateReplies(t *testing.T) {
	cancelSeen := make(chan string, 1)
	conn := openConn(t, func(s *fakeServer) {
		s.login()
		tag := tagOf(t, s.next())
		cancel := s.next()
		if len(cancel) != 2 || cancel[0] != "/cancel" || cancel[1] != "=tag="+tag {
			t.Errorf("cancel sentence = %q, want /cancel =tag=%s", cancel, tag)
			return
		}
		cancelSeen <- tag
		s.send("!done")
		s.send("!empty", ".tag="+tag)
		s.send("!done", ".tag="+tag)
		next := s.next()
		s.send("!done", ".tag="+tagOf(t, next))
		s.drain()
	})

	p, err := conn.Submit(context.Background(), []string{"/ping"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := <-p.Written(); err != nil {
		t.Fatalf("Written() error = %v", err)
	}

	cause := errors.New("caller went away")
	conn.Cancel(p, cause)

	select {
	case <-p.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Cancel did not resolve the pending request")
	}
	if !errors.Is(p.Err(), cause) {
		t.Fatalf("Err() = %v, want the cancellation cause", p.Err())
	}

	select {
	case tag := <-cancelSeen:
		if tag != p.Tag() {
			t.Errorf("cancel target = %q, want %q", tag, p.Tag())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive /cancel")
	}

	// The zombie replies must leave the connection usable, not desynchronized.
	next, err := conn.Submit(context.Background(), []string{"/print"})
	if err != nil {
		t.Fatalf("Submit() after cancellation error = %v", err)
	}
	select {
	case <-next.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("connection stopped routing after zombie replies")
	}
	if err := next.Err(); err != nil {
		t.Fatalf("next request error = %v, want nil", err)
	}
	if err := conn.Err(); err != nil {
		t.Fatalf("Err() = %v, want the zombie replies to be absorbed", err)
	}
}

func TestUntaggedReplyDuringCancellationIsDesynchronization(t *testing.T) {
	conn := openConn(t, func(s *fakeServer) {
		s.login()
		tag := tagOf(t, s.next())
		cancel := s.next()
		if len(cancel) != 2 || cancel[0] != "/cancel" || cancel[1] != "=tag="+tag {
			t.Errorf("cancel sentence = %q, want /cancel =tag=%s", cancel, tag)
			return
		}
		s.send("!re", "=value=unexpected")
		s.drain()
	})

	p, err := conn.Submit(context.Background(), []string{"/ping"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := <-p.Written(); err != nil {
		t.Fatalf("Written() error = %v", err)
	}
	conn.Cancel(p, errors.New("caller went away"))

	select {
	case <-conn.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("connection remained open after an untagged reply during cancellation")
	}
	var protocolErr *wire.ProtocolError
	if !errors.As(conn.Err(), &protocolErr) || !strings.Contains(protocolErr.Detail, "untagged") {
		t.Fatalf("connection error = %v, want an untagged protocol error", conn.Err())
	}
}

func TestOnlyOneUntaggedDoneIsAcceptedPerCancel(t *testing.T) {
	conn := openConn(t, func(s *fakeServer) {
		s.login()
		tag := tagOf(t, s.next())
		cancel := s.next()
		if len(cancel) != 2 || cancel[0] != "/cancel" || cancel[1] != "=tag="+tag {
			t.Errorf("cancel sentence = %q, want /cancel =tag=%s", cancel, tag)
			return
		}
		s.send("!done")
		s.send("!done")
		s.drain()
	})

	p, err := conn.Submit(context.Background(), []string{"/ping"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := <-p.Written(); err != nil {
		t.Fatalf("Written() error = %v", err)
	}
	conn.Cancel(p, errors.New("caller went away"))

	select {
	case <-conn.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("connection remained open after an extra untagged !done")
	}
	var protocolErr *wire.ProtocolError
	if !errors.As(conn.Err(), &protocolErr) || !strings.Contains(protocolErr.Detail, "untagged") {
		t.Fatalf("connection error = %v, want an untagged protocol error", conn.Err())
	}
}

func TestCancelledTagExpiresIntoAZombie(t *testing.T) {
	opts := newOptions(t, func(s *fakeServer) {
		s.login()
		tag := tagOf(t, s.next())
		s.next()
		s.send("!done")
		time.Sleep(100 * time.Millisecond)
		s.send("!empty", ".tag="+tag)
		s.send("!done", ".tag="+tag)
		next := s.next()
		s.send("!done", ".tag="+tagOf(t, next))
		s.drain()
	})
	opts.CancelGrace = 20 * time.Millisecond

	conn, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	p, err := conn.Submit(context.Background(), []string{"/ping"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := <-p.Written(); err != nil {
		t.Fatalf("Written() error = %v", err)
	}
	conn.Cancel(p, errors.New("expire"))

	time.Sleep(60 * time.Millisecond)
	select {
	case <-p.Done():
	default:
		t.Fatal("cancelled pending is still open")
	}

	next, err := conn.Submit(context.Background(), []string{"/print"})
	if err != nil {
		t.Fatalf("Submit() after grace expiry error = %v", err)
	}
	select {
	case <-next.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("connection stopped routing after the late cancelled !done")
	}
	if err := next.Err(); err != nil {
		t.Fatalf("next request error = %v, want nil", err)
	}
	if err := conn.Err(); err != nil {
		t.Fatalf("Err() = %v, want the late !done to be absorbed by the zombie", err)
	}
}

func TestCancelAfterDoneDoesNothing(t *testing.T) {
	conn := openConn(t, func(s *fakeServer) {
		s.login()
		words := s.next()
		s.send("!done", ".tag="+tagOf(t, words))
		s.drain()
	})

	p, err := conn.Submit(context.Background(), []string{"/print"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	<-p.Done()
	conn.Cancel(p, errors.New("too late"))

	if err := conn.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("pending was reopened by Cancel")
	}
}

func TestCancelBeforeWriteSkipsCommandAndCancel(t *testing.T) {
	release := make(chan struct{})
	seen := make(chan [][]string, 1)
	conn := openConn(t, func(s *fakeServer) {
		s.login()
		<-release
		first := s.next()
		next := s.next()
		seen <- [][]string{first, next}
		s.send("!done", ".tag="+tagOf(t, first))
		if next[0] == "/cancel" {
			s.send("!done")
			next = s.next()
		}
		s.send("!done", ".tag="+tagOf(t, next))
		s.drain()
	})

	first, err := conn.Submit(context.Background(), []string{"/first/print"})
	if err != nil {
		t.Fatalf("Submit(first) error = %v", err)
	}
	target, err := conn.Submit(context.Background(), []string{"/ip/address/add"})
	if err != nil {
		t.Fatalf("Submit(target) error = %v", err)
	}
	conn.Cancel(target, errors.New("caller cancelled before the write"))
	next, err := conn.Submit(context.Background(), []string{"/system/identity/print"})
	if err != nil {
		t.Fatalf("Submit(next) error = %v", err)
	}

	select {
	case <-target.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled target is still open")
	}
	close(release)

	words := <-seen
	firstWords, nextWords := words[0], words[1]
	if got := tagOf(t, firstWords); got != first.Tag() {
		t.Errorf("first tag = %q, want %q", got, first.Tag())
	}
	if len(nextWords) == 0 || nextWords[0] != "/system/identity/print" {
		t.Errorf("second sentence = %q, want the next command and no /cancel", nextWords)
	} else if got := tagOf(t, nextWords); got != next.Tag() {
		t.Errorf("next tag = %q, want %q", got, next.Tag())
	}

	select {
	case <-next.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("connection stopped routing after a skipped write")
	}
	if err := next.Err(); err != nil {
		t.Fatalf("next request error = %v", err)
	}
}

func TestCancelledCommandDoesNotBlockTheReader(t *testing.T) {
	filled := make(chan struct{})
	conn := openConn(t, func(s *fakeServer) {
		s.login()
		tag := tagOf(t, s.next())
		for i := 0; i < replyBuffer; i++ {
			s.send("!re", "=n=filled", ".tag="+tag)
		}
		close(filled)
		// The queue is full: this record blocks the reader until it is cancelled.
		s.send("!re", "=n=overflow", ".tag="+tag)
		for i := 0; i < 4; i++ {
			s.send("!re", "=n=late", ".tag="+tag)
		}
		cancel := s.next()
		if len(cancel) != 2 || cancel[0] != "/cancel" || cancel[1] != "=tag="+tag {
			t.Errorf("cancel sentence = %q, want /cancel for %s", cancel, tag)
			return
		}
		s.send("!done")
		s.send("!done", ".tag="+tag)
		next := s.next()
		s.send("!done", ".tag="+tagOf(t, next))
		s.drain()
	})

	target, err := conn.Submit(context.Background(), []string{"/tool/ping"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	<-filled
	time.Sleep(50 * time.Millisecond)
	conn.Cancel(target, errors.New("stop reading"))

	select {
	case <-target.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled target is still open")
	}

	next, err := conn.Submit(context.Background(), []string{"/system/resource/print"})
	if err != nil {
		t.Fatalf("Submit() after cancellation error = %v", err)
	}
	select {
	case <-next.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("reader stayed blocked on the cancelled command's full reply buffer")
	}
	if err := next.Err(); err != nil {
		t.Fatalf("next request error = %v", err)
	}
	if err := conn.Err(); err != nil {
		t.Fatalf("Err() = %v, want a healthy connection", err)
	}
}
