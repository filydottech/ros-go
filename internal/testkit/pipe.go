// Package testkit provides in-memory transport helpers for the library's
// tests. Nothing here opens a socket, so the tests run in environments that
// forbid a loopback listener.
package testkit

import "net"

// Pipe returns the two ends of an in-memory connection. A write blocks until
// the other end reads, which makes it a deterministic stand-in for a TCP
// connection in transport tests.
func Pipe() (client, server net.Conn) {
	return net.Pipe()
}
