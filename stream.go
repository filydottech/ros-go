package routeros

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/filydottech/ros-go/internal/transport"
)

var errStreamClosed = errors.New("stream closed")

// Stream receives the !re records of a long-running command. A stream is
// bounded by its record count and byte limit; when either is full the reader
// stops consuming the connection until the queue drains, which is real TCP
// backpressure. A slow listener can therefore delay other commands on the same
// connection, so a high-volume listener deserves its own Client.
//
// A Stream is obtained from Client.Stream and is not valid after Close.
type Stream struct {
	conn    *transport.Conn
	pending *transport.Pending
	timeout time.Duration

	closeOnce sync.Once
	closeErr  error
}

// Recv returns the next record. It returns io.EOF only for a normal !done; a
// trap, context cancellation, peer close, and local Close each return a
// distinct typed error.
func (s *Stream) Recv() (Record, error) {
	reply, err := s.pending.NextStream()
	if err == nil {
		return Record(reply.Attributes), nil
	}
	if errors.Is(err, io.EOF) {
		if trap, ok := s.pending.Trap(); ok {
			done := Record{}
			if terminal, ok := s.pending.DoneReply(); ok {
				done = Record(terminal.Attributes)
			}
			return nil, &TrapError{
				Category: trap.Attributes["category"],
				Message:  trap.Attributes["message"],
				Done:     done,
			}
		}
		return nil, io.EOF
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errStreamClosed) {
		return nil, &CancelledError{Cause: err}
	}
	return nil, mapTransportError("recv", s.timeout, err)
}

// Close cancels the command on the router and waits for its !done or the
// cancel grace period. It is idempotent, wakes a waiting Recv, and returns nil
// once the stream is finished.
func (s *Stream) Close() error {
	s.closeOnce.Do(func() {
		wait := s.conn.Cancel(s.pending, errStreamClosed)
		select {
		case <-wait:
			s.closeErr = nil
		case <-s.conn.Done():
			s.closeErr = mapTransportError("close stream", s.timeout, s.conn.Err())
		}
	})
	return s.closeErr
}

func newStream(conn *transport.Conn, pending *transport.Pending, timeout time.Duration) *Stream {
	return &Stream{conn: conn, pending: pending, timeout: timeout}
}

func (s *Stream) watchContext(ctx context.Context) {
	select {
	case <-ctx.Done():
		s.conn.Cancel(s.pending, ctx.Err())
	case <-s.pending.Done():
	}
}
