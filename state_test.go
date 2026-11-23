package routeros

import (
	"errors"
	"testing"
)

func TestClientCloseIsFinalAndIdempotent(t *testing.T) {
	client := newClient(Config{Host: "router.invalid"})

	if got := client.State(); got != Connecting {
		t.Errorf("State() = %s, want %s", got, Connecting)
	}
	if err := client.Err(); err != nil {
		t.Errorf("Err() = %v, want nil", err)
	}
	select {
	case <-client.Done():
		t.Fatal("Done() closed before Close()")
	default:
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := client.State(); got != Closed {
		t.Errorf("State() = %s, want %s", got, Closed)
	}
	var closed *ClosedError
	if !errors.As(client.Err(), &closed) {
		t.Errorf("Err() = %v, want *ClosedError", client.Err())
	}
	select {
	case <-client.Done():
	default:
		t.Error("Done() is still open after Close()")
	}

	if err := client.Close(); err != nil {
		t.Errorf("second Close() error = %v, want nil", err)
	}
}

func TestStateString(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{Connecting, "connecting"},
		{LoggingIn, "logging in"},
		{Ready, "ready"},
		{Reconnecting, "reconnecting"},
		{Closing, "closing"},
		{Closed, "closed"},
		{State(99), "State(99)"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("State(%d).String() = %q, want %q", int(tt.state), got, tt.want)
		}
	}
}
