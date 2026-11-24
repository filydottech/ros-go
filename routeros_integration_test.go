//go:build integration

package routeros

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestRouterOSCancellationLeavesConnectionUsable(t *testing.T) {
	port, err := strconv.Atoi(os.Getenv("ROS_PORT"))
	if err != nil {
		t.Fatal("ROS_PORT must be set to the test router port")
	}
	client, err := Dial(context.Background(), Config{
		Host:           os.Getenv("ROS_HOST"),
		Port:           port,
		Username:       os.Getenv("ROS_USER"),
		Password:       []byte(os.Getenv("ROS_PASSWORD")),
		Plaintext:      true,
		CommandTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer client.Close()

	_, err = client.Do(context.Background(), "/ping", Request{Attributes: []Attribute{
		{Name: "address", Value: "127.0.0.1"},
		{Name: "count", Value: "1000"},
		{Name: "interval", Value: "1s"},
	}})
	var timeout *TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("Do(ping) error = %v, want *TimeoutError", err)
	}
	if _, err := client.Do(context.Background(), "/system/identity/print", Request{}); err != nil {
		t.Fatalf("Do(identity) after cancellation error = %v", err)
	}
}

func TestRouterOSStreamCancellationLeavesConnectionUsable(t *testing.T) {
	port, err := strconv.Atoi(os.Getenv("ROS_PORT"))
	if err != nil {
		t.Fatal("ROS_PORT must be set to the test router port")
	}
	client, err := Dial(context.Background(), Config{
		Host:      os.Getenv("ROS_HOST"),
		Port:      port,
		Username:  os.Getenv("ROS_USER"),
		Password:  []byte(os.Getenv("ROS_PASSWORD")),
		Plaintext: true,
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Stream(ctx, "/interface/listen", Request{})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	cancel()

	_, err = stream.Recv()
	var cancelled *CancelledError
	if !errors.As(err, &cancelled) {
		t.Fatalf("Recv() error = %v, want *CancelledError", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() after context cancellation error = %v", err)
	}
	if _, err := client.Do(context.Background(), "/system/identity/print", Request{}); err != nil {
		t.Fatalf("Do(identity) after stream cancellation error = %v", err)
	}
}

func TestRouterOSReconnectReplacesConnection(t *testing.T) {
	port, err := strconv.Atoi(os.Getenv("ROS_PORT"))
	if err != nil {
		t.Fatal("ROS_PORT must be set to the test router port")
	}
	cfg := Config{
		Host:      os.Getenv("ROS_HOST"),
		Port:      port,
		Username:  os.Getenv("ROS_USER"),
		Password:  []byte(os.Getenv("ROS_PASSWORD")),
		Plaintext: true,
	}
	client, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer client.Close()

	before := client.Done()
	if err := client.Reconnect(context.Background(), cfg); err != nil {
		t.Fatalf("Reconnect() error = %v", err)
	}
	select {
	case <-before:
	case <-time.After(5 * time.Second):
		t.Fatal("old client generation did not close")
	}
	if got := client.State(); got != Ready {
		t.Errorf("State() after Reconnect() = %s, want %s", got, Ready)
	}
	if _, err := client.Do(context.Background(), "/system/identity/print", Request{}); err != nil {
		t.Fatalf("Do(identity) after Reconnect() error = %v", err)
	}
}
