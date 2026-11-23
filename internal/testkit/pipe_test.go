package testkit_test

import (
	"io"
	"testing"

	"github.com/filydottech/ros-go/internal/testkit"
)

func TestPipeCarriesBytes(t *testing.T) {
	client, server := testkit.Pipe()
	defer client.Close()
	defer server.Close()

	const payload = "!done\n"

	go func() {
		_, _ = server.Write([]byte(payload))
	}()

	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(client, buf); err != nil {
		t.Fatalf("ReadFull() error = %v", err)
	}
	if got := string(buf); got != payload {
		t.Errorf("read %q, want %q", got, payload)
	}
}
