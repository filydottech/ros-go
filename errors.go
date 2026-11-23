package routeros

import (
	"fmt"
	"time"
)

// ConnectionError reports a dial, login, read, write, or peer-close failure.
// Op names the failed operation; Err wraps the underlying cause, which may be
// a network error or a validation error.
type ConnectionError struct {
	Op  string
	Err error
}

func (e *ConnectionError) Error() string {
	if e.Err == nil {
		return "routeros: " + e.Op
	}
	return "routeros: " + e.Op + ": " + e.Err.Error()
}

// Unwrap returns the underlying cause.
func (e *ConnectionError) Unwrap() error { return e.Err }

// Is reports whether target is any *ConnectionError, so that errors.Is can
// match the category without comparing details.
func (*ConnectionError) Is(target error) bool { _, ok := target.(*ConnectionError); return ok }

// ClosedError reports an operation on a Client the caller closed.
type ClosedError struct{}

func (*ClosedError) Error() string { return "routeros: client is closed" }

// Is reports whether target is any *ClosedError.
func (*ClosedError) Is(target error) bool { _, ok := target.(*ClosedError); return ok }

// ProtocolError reports framing, reply-parsing, and desynchronization
// failures, and a /add reply without a ret attribute.
type ProtocolError struct {
	Detail string
}

func (e *ProtocolError) Error() string { return "routeros: protocol error: " + e.Detail }

// Is reports whether target is any *ProtocolError.
func (*ProtocolError) Is(target error) bool { _, ok := target.(*ProtocolError); return ok }

// LoginError reports a !trap reply during the login exchange.
type LoginError struct {
	Message string
}

func (e *LoginError) Error() string { return "routeros: login failed: " + e.Message }

// Is reports whether target is any *LoginError.
func (*LoginError) Is(target error) bool { _, ok := target.(*LoginError); return ok }

// TrapError reports a command the router rejected. Done holds the attributes
// of the terminating !done reply, so a caller can read the router's category
// and message together with the final status words.
type TrapError struct {
	Category string
	Message  string
	Done     Record
}

func (e *TrapError) Error() string {
	if e.Category == "" {
		return "routeros: trap: " + e.Message
	}
	return "routeros: trap (" + e.Category + "): " + e.Message
}

// Is reports whether target is any *TrapError.
func (*TrapError) Is(target error) bool { _, ok := target.(*TrapError); return ok }

// FatalError reports a !fatal reply. It is sticky for the connection
// generation: every pending operation fails with it.
type FatalError struct {
	Reason string
}

func (e *FatalError) Error() string { return "routeros: fatal: " + e.Reason }

// Is reports whether target is any *FatalError.
func (*FatalError) Is(target error) bool { _, ok := target.(*FatalError); return ok }

// TimeoutError reports a dial or command timeout. Phase names the operation
// that did not finish within Duration.
type TimeoutError struct {
	Phase    string
	Duration time.Duration
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("routeros: %s timed out after %s", e.Phase, e.Duration)
}

// Is reports whether target is any *TimeoutError.
func (*TimeoutError) Is(target error) bool { _, ok := target.(*TimeoutError); return ok }

// CancelledError reports a context or stream close that ended an operation.
// Cause wraps the context error or the stream's close cause.
type CancelledError struct {
	Cause error
}

func (e *CancelledError) Error() string {
	if e.Cause == nil {
		return "routeros: cancelled"
	}
	return "routeros: cancelled: " + e.Cause.Error()
}

// Unwrap returns the cause, so errors.Is can match context.Canceled and
// context.DeadlineExceeded.
func (e *CancelledError) Unwrap() error { return e.Cause }

// Is reports whether target is any *CancelledError.
func (*CancelledError) Is(target error) bool { _, ok := target.(*CancelledError); return ok }

// ResourceLimitError reports a decoder or queue limit that was exceeded.
// Scope names the limit, for example "word", "sentence", or "stream records".
type ResourceLimitError struct {
	Scope    string
	Limit    int64
	Observed int64
}

func (e *ResourceLimitError) Error() string {
	return fmt.Sprintf("routeros: %s exceeds limit %d (observed %d)", e.Scope, e.Limit, e.Observed)
}

// Is reports whether target is any *ResourceLimitError.
func (*ResourceLimitError) Is(target error) bool { _, ok := target.(*ResourceLimitError); return ok }

// ReconnectError reports work displaced by Reconnect, or work submitted while
// a reconnect is in progress. It is never returned for a command the router
// may have applied: the caller must decide whether to repeat a mutation.
type ReconnectError struct{}

func (*ReconnectError) Error() string { return "routeros: operation interrupted by reconnect" }

// Is reports whether target is any *ReconnectError.
func (*ReconnectError) Is(target error) bool { _, ok := target.(*ReconnectError); return ok }

var (
	_ error = (*ConnectionError)(nil)
	_ error = (*ClosedError)(nil)
	_ error = (*ProtocolError)(nil)
	_ error = (*LoginError)(nil)
	_ error = (*TrapError)(nil)
	_ error = (*FatalError)(nil)
	_ error = (*TimeoutError)(nil)
	_ error = (*CancelledError)(nil)
	_ error = (*ResourceLimitError)(nil)
	_ error = (*ReconnectError)(nil)
)
