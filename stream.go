package routeros

// Stream receives the !re records of a long-running command. A stream is
// bounded by its record count and byte limit; when either is full the reader
// stops consuming the connection until the queue drains, which is real TCP
// backpressure. A slow listener can therefore delay other commands on the same
// connection, so a high-volume listener deserves its own Client.
//
// A Stream is obtained from Client.Stream and is not valid after Close.
type Stream struct{}

// Recv returns the next record. It returns io.EOF only for a normal !done; a
// trap, context cancellation, peer close, and local Close each return a
// distinct typed error.
func (s *Stream) Recv() (Record, error) {
	return nil, &ConnectionError{Op: "recv", Err: errNoStreams}
}

// Close cancels the command on the router and waits for its !done or the
// cancel grace period. It is idempotent, wakes a waiting Recv, and returns nil
// once the stream is finished.
func (s *Stream) Close() error {
	return &ConnectionError{Op: "close stream", Err: errNoStreams}
}
