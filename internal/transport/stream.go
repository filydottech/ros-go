package transport

import (
	"errors"
	"io"

	"github.com/filydottech/ros-go/internal/wire"
)

type queuedReply struct {
	reply wire.Reply
	bytes int
}

type streamQueue struct {
	conn       *Conn
	maxRecords int
	maxBytes   int
	records    []queuedReply
	bytes      int
	paused     bool
	closed     bool
	err        error
	changed    chan struct{}
}

func newStreamQueue(conn *Conn, maxRecords, maxBytes int) *streamQueue {
	return &streamQueue{
		conn:       conn,
		maxRecords: maxRecords,
		maxBytes:   maxBytes,
		changed:    make(chan struct{}),
	}
}

// NextStream returns the next queued !re reply, io.EOF after a normal !done,
// or the terminal transport error. It is only valid for a Pending returned by
// SubmitStream.
func (p *Pending) NextStream() (wire.Reply, error) {
	queue := p.stream
	if queue == nil {
		return wire.Reply{}, errors.New("transport: pending is not a stream")
	}

	for {
		queue.conn.mu.Lock()
		if len(queue.records) > 0 {
			record := queue.records[0]
			queue.records = queue.records[1:]
			queue.bytes -= record.bytes
			queue.conn.bufferedStreamBytes -= record.bytes
			queue.conn.updateQueuePauseLocked(queue)
			queue.conn.updateAggregatePauseLocked()
			queue.signalLocked()
			queue.conn.mu.Unlock()
			return record.reply, nil
		}
		if queue.err != nil {
			err := queue.err
			queue.conn.mu.Unlock()
			return wire.Reply{}, err
		}
		if queue.closed {
			queue.conn.mu.Unlock()
			return wire.Reply{}, io.EOF
		}
		changed := queue.changed
		queue.conn.mu.Unlock()

		select {
		case <-changed:
		case <-queue.conn.done:
		}
	}
}

func (q *streamQueue) appendLocked(reply wire.Reply, bytes int) {
	q.records = append(q.records, queuedReply{reply: reply, bytes: bytes})
	q.bytes += bytes
	q.conn.bufferedStreamBytes += bytes
	q.conn.updateQueuePauseLocked(q)
	q.conn.updateAggregatePauseLocked()
	q.signalLocked()
}

func (q *streamQueue) closeLocked() {
	if q.closed {
		return
	}
	q.closed = true
	q.conn.setQueuePausedLocked(q, false)
	q.signalLocked()
}

func (q *streamQueue) failLocked(err error) {
	if q.closed {
		return
	}
	q.closed = true
	q.err = err
	q.conn.bufferedStreamBytes -= q.bytes
	q.records = nil
	q.bytes = 0
	q.conn.setQueuePausedLocked(q, false)
	q.conn.updateAggregatePauseLocked()
	q.signalLocked()
}

func (q *streamQueue) signalLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}

func (q *streamQueue) isFullLocked() bool {
	return len(q.records) >= q.maxRecords || q.bytes >= q.maxBytes
}

func (q *streamQueue) isDrainedLocked() bool {
	return len(q.records) <= q.maxRecords/2 && q.bytes <= q.maxBytes/2
}

func streamReplyBytes(reply wire.Reply) int {
	return wire.ReplyAttributeBytes(reply)
}
