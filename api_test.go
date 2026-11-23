package routeros_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/filydottech/ros-go"
)

// The compile-time pin of the v1 public surface. A signature change breaks the
// build here before it can reach a consumer.
var (
	_ func(context.Context, routeros.Config) (*routeros.Client, error)                            = routeros.Dial
	_ func(*routeros.Client, context.Context, string, routeros.Request) (routeros.Result, error)  = (*routeros.Client).Do
	_ func(*routeros.Client, context.Context, string, routeros.Request) (*routeros.Stream, error) = (*routeros.Client).Stream
	_ func(*routeros.Client, context.Context, routeros.Config) error                              = (*routeros.Client).Reconnect
	_ func(*routeros.Client) error                                                                = (*routeros.Client).Close
	_ func(*routeros.Client) routeros.State                                                       = (*routeros.Client).State
	_ func(*routeros.Client) <-chan struct{}                                                      = (*routeros.Client).Done
	_ func(*routeros.Client) error                                                                = (*routeros.Client).Err
	_ func(*routeros.Stream) (routeros.Record, error)                                             = (*routeros.Stream).Recv
	_ func(*routeros.Stream) error                                                                = (*routeros.Stream).Close
	_ func(string, string) routeros.Expr                                                          = routeros.Equal
	_ func(string, string) routeros.Expr                                                          = routeros.LessThan
	_ func(string, string) routeros.Expr                                                          = routeros.GreaterThan
	_ func(string) routeros.Expr                                                                  = routeros.Exists
	_ func(string) routeros.Expr                                                                  = routeros.Absent
	_ func(routeros.Expr) routeros.Expr                                                           = routeros.Not
	_ func(routeros.Expr, ...routeros.Expr) routeros.Expr                                         = routeros.And
	_ func(routeros.Expr, ...routeros.Expr) routeros.Expr                                         = routeros.Or
	_ func(routeros.Expr) []string                                                                = routeros.Expr.Words
)

func TestDialRejectsInvalidConfigBeforeConnecting(t *testing.T) {
	const password = "not-a-real-password"

	_, err := routeros.Dial(context.Background(), routeros.Config{Password: []byte(password)})
	if err == nil {
		t.Fatal("Dial() error = nil, want a validation error")
	}

	var connection *routeros.ConnectionError
	if !errors.As(err, &connection) {
		t.Fatalf("Dial() error = %v, want *ConnectionError", err)
	}
	if !strings.Contains(err.Error(), "host is required") {
		t.Errorf("Dial() error = %q, want the config error rather than a transport error", err)
	}
	if strings.Contains(err.Error(), password) {
		t.Errorf("Dial() error = %q, must not contain the configured password", err)
	}
}

func TestDialReportsAnEndedContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := routeros.Dial(ctx, routeros.Config{Host: "router.invalid"})

	var cancelled *routeros.CancelledError
	if !errors.As(err, &cancelled) {
		t.Fatalf("Dial() error = %v, want *CancelledError", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("errors.Is(Dial() error, context.Canceled) = false, want true")
	}
}
