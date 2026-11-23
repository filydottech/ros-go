package routeros

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestErrorCategoriesMatchWithErrorsIs(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		target error
	}{
		{"connection", &ConnectionError{Op: "dial"}, &ConnectionError{}},
		{"closed", &ClosedError{}, &ClosedError{}},
		{"protocol", &ProtocolError{Detail: "bad frame"}, &ProtocolError{}},
		{"login", &LoginError{Message: "denied"}, &LoginError{}},
		{"trap", &TrapError{Message: "denied"}, &TrapError{}},
		{"fatal", &FatalError{Reason: "rebooting"}, &FatalError{}},
		{"timeout", &TimeoutError{Phase: "dial", Duration: time.Second}, &TimeoutError{}},
		{"cancelled", &CancelledError{}, &CancelledError{}},
		{"resource limit", &ResourceLimitError{Scope: "word"}, &ResourceLimitError{}},
		{"reconnect", &ReconnectError{}, &ReconnectError{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !errors.Is(tt.err, tt.target) {
				t.Errorf("errors.Is(%v, %T) = false, want true", tt.err, tt.target)
			}
		})
	}
}

func TestErrorCategoriesDoNotMatchEachOther(t *testing.T) {
	if errors.Is(&ConnectionError{Op: "read"}, &ProtocolError{}) {
		t.Error("errors.Is(ConnectionError, *ProtocolError) = true, want false")
	}
	if errors.Is(&TrapError{Message: "denied"}, &LoginError{}) {
		t.Error("errors.Is(TrapError, *LoginError) = true, want false")
	}
}

func TestConnectionErrorUnwrapsItsCause(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", &ConnectionError{Op: "read", Err: os.ErrDeadlineExceeded})

	var connection *ConnectionError
	if !errors.As(err, &connection) {
		t.Fatalf("errors.As(%v, *ConnectionError) = false, want true", err)
	}
	if connection.Op != "read" {
		t.Errorf("Op = %q, want %q", connection.Op, "read")
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Error("errors.Is(err, os.ErrDeadlineExceeded) = false, want true")
	}
}

func TestCancelledErrorMatchesItsContextCause(t *testing.T) {
	err := &CancelledError{Cause: context.Canceled}
	if !errors.Is(err, context.Canceled) {
		t.Error("errors.Is(CancelledError, context.Canceled) = false, want true")
	}
	if !errors.Is(err, &CancelledError{}) {
		t.Error("errors.Is(CancelledError, *CancelledError) = false, want true")
	}
}
