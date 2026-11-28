package routeros

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/filydottech/ros-go/internal/testkit"
	"github.com/filydottech/ros-go/internal/transport"
)

func dialWithTransportScripts(t *testing.T, scripts ...func(*sentenceReader)) *Client {
	return dialWithConfigAndTransportScripts(t, Config{
		Host:      "router.test",
		Plaintext: true,
		Username:  "alice",
		Password:  []byte(dialTestPassword),
	}, scripts...)
}

func dialWithConfigAndTransportScripts(t *testing.T, cfg Config, scripts ...func(*sentenceReader)) *Client {
	t.Helper()

	oldOpen := openTransport
	var (
		mu          sync.Mutex
		next        int
		clientConns []net.Conn
		serverDone  []<-chan struct{}
	)
	openTransport = func(ctx context.Context, opts transport.Options) (*transport.Conn, error) {
		mu.Lock()
		if next == len(scripts) {
			mu.Unlock()
			t.Fatalf("openTransport called more than %d times", len(scripts))
		}
		script := scripts[next]
		next++
		clientConn, serverConn := testkit.Pipe()
		clientConns = append(clientConns, clientConn)
		done := make(chan struct{})
		serverDone = append(serverDone, done)
		mu.Unlock()

		go func() {
			defer close(done)
			defer serverConn.Close()
			reader := newSentenceReader(t, serverConn)
			reader.login()
			script(reader)
		}()
		opts.Dial = func(context.Context, string) (net.Conn, error) {
			return clientConn, nil
		}
		return transport.Open(ctx, opts)
	}
	t.Cleanup(func() {
		openTransport = oldOpen
		mu.Lock()
		conns := append([]net.Conn(nil), clientConns...)
		dones := append([]<-chan struct{}(nil), serverDone...)
		mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
		for _, done := range dones {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("fake reconnect server did not finish")
			}
		}
	})

	client, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestPasswordConfigCanBeReusedForDialAndReconnect(t *testing.T) {
	password := []byte(dialTestPassword)
	caller := append([]byte(nil), password...)
	cfg := Config{
		Host:      "router.test",
		Plaintext: true,
		Username:  "alice",
		Password:  password,
	}
	client := dialWithConfigAndTransportScripts(t, cfg,
		func(r *sentenceReader) { r.drain() },
		func(r *sentenceReader) { r.drain() },
	)

	if err := client.Reconnect(context.Background(), cfg); err != nil {
		t.Fatalf("Reconnect() error = %v", err)
	}
	if !bytes.Equal(cfg.Password, caller) {
		t.Error("Dial() or Reconnect() modified the caller password")
	}
}

func TestReconnectReplacesGenerationAndResetsTags(t *testing.T) {
	client := dialWithTransportScripts(t,
		func(r *sentenceReader) {
			command := r.next()
			if tag := r.tag(command); tag != "1" {
				t.Errorf("old command tag = %q, want 1", tag)
			}
			r.send("!done", ".tag="+r.tag(command))
			r.drain()
		},
		func(r *sentenceReader) {
			command := r.next()
			if tag := r.tag(command); tag != "1" {
				t.Errorf("new command tag = %q, want 1 after reconnect", tag)
			}
			r.send("!done", ".tag="+r.tag(command))
			r.drain()
		},
	)

	if _, err := client.Do(context.Background(), "/system/identity/print", Request{}); err != nil {
		t.Fatalf("Do() before Reconnect() error = %v", err)
	}
	before := client.Done()

	if err := client.Reconnect(context.Background(), Config{
		Host:      "router.test",
		Plaintext: true,
		Username:  "alice",
		Password:  []byte(dialTestPassword),
	}); err != nil {
		t.Fatalf("Reconnect() error = %v", err)
	}
	if got := client.State(); got != Ready {
		t.Errorf("State() after Reconnect() = %s, want %s", got, Ready)
	}
	after := client.Done()
	if before == after {
		t.Error("Done() reused the old generation channel")
	}
	select {
	case <-before:
	default:
		t.Error("old Done() channel is still open after reconnect")
	}

	if _, err := client.Do(context.Background(), "/system/identity/print", Request{}); err != nil {
		t.Fatalf("Do() after Reconnect() error = %v", err)
	}
}

func TestReconnectRejectsNewWorkWhileDialing(t *testing.T) {
	client := dialWithTransportScripts(t,
		func(r *sentenceReader) { r.drain() },
		func(r *sentenceReader) { r.drain() },
	)

	underlying := openTransport
	entered := make(chan struct{})
	release := make(chan struct{})
	openTransport = func(ctx context.Context, opts transport.Options) (*transport.Conn, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return underlying(ctx, opts)
	}
	t.Cleanup(func() { openTransport = underlying })

	result := make(chan error, 1)
	go func() {
		result <- client.Reconnect(context.Background(), Config{
			Host:      "router.test",
			Plaintext: true,
			Username:  "alice",
			Password:  []byte(dialTestPassword),
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Reconnect() did not begin dialing")
	}

	if got := client.State(); got != Reconnecting {
		t.Errorf("State() while reconnecting = %s, want %s", got, Reconnecting)
	}
	if _, err := client.Do(context.Background(), "/system/identity/print", Request{}); !errors.Is(err, &ReconnectError{}) {
		t.Errorf("Do() while reconnecting error = %v, want *ReconnectError", err)
	}
	if _, err := client.Stream(context.Background(), "/interface/listen", Request{}); !errors.Is(err, &ReconnectError{}) {
		t.Errorf("Stream() while reconnecting error = %v, want *ReconnectError", err)
	}
	if err := client.Reconnect(context.Background(), Config{Host: "router.test"}); !errors.Is(err, &ReconnectError{}) {
		t.Errorf("second Reconnect() error = %v, want *ReconnectError", err)
	}

	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Reconnect() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Reconnect() did not finish")
	}
}

func TestReconnectInterruptsPendingCommand(t *testing.T) {
	commandSeen := make(chan struct{})
	client := dialWithTransportScripts(t,
		func(r *sentenceReader) {
			_ = r.next()
			close(commandSeen)
			r.drain()
		},
		func(r *sentenceReader) { r.drain() },
	)

	command := make(chan error, 1)
	go func() {
		_, err := client.Do(context.Background(), "/system/identity/print", Request{})
		command <- err
	}()
	select {
	case <-commandSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive the pending command")
	}

	if err := client.Reconnect(context.Background(), Config{
		Host:      "router.test",
		Plaintext: true,
		Username:  "alice",
		Password:  []byte(dialTestPassword),
	}); err != nil {
		t.Fatalf("Reconnect() error = %v", err)
	}
	select {
	case err := <-command:
		if !errors.Is(err, &ReconnectError{}) {
			t.Errorf("pending Do() error = %v, want *ReconnectError", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pending Do() did not finish after reconnect")
	}
}

func TestReconnectInterruptsPendingStream(t *testing.T) {
	client := dialWithTransportScripts(t,
		func(r *sentenceReader) {
			command := r.next()
			r.send("!re", "=name=ether1", ".tag="+r.tag(command))
			r.drain()
		},
		func(r *sentenceReader) { r.drain() },
	)

	stream, err := client.Stream(context.Background(), "/interface/listen", Request{})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	if record, err := stream.Recv(); err != nil || record["name"] != "ether1" {
		t.Fatalf("first Recv() = %+v, %v, want ether1 and nil", record, err)
	}

	received := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		received <- err
	}()
	if err := client.Reconnect(context.Background(), Config{
		Host:      "router.test",
		Plaintext: true,
		Username:  "alice",
		Password:  []byte(dialTestPassword),
	}); err != nil {
		t.Fatalf("Reconnect() error = %v", err)
	}

	select {
	case err := <-received:
		if !errors.Is(err, &ReconnectError{}) {
			t.Errorf("pending Recv() error = %v, want *ReconnectError", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pending Recv() did not finish after reconnect")
	}
}

func TestFailedReconnectIsRetryable(t *testing.T) {
	client := dialWithTransportScripts(t,
		func(r *sentenceReader) { r.drain() },
		func(r *sentenceReader) {
			command := r.next()
			r.send("!done", ".tag="+r.tag(command))
			r.drain()
		},
	)

	underlying := openTransport
	attempts := 0
	openTransport = func(ctx context.Context, opts transport.Options) (*transport.Conn, error) {
		attempts++
		if attempts == 1 {
			return nil, errors.New("test dial failure")
		}
		return underlying(ctx, opts)
	}
	t.Cleanup(func() { openTransport = underlying })

	cfg := Config{
		Host:      "router.test",
		Plaintext: true,
		Username:  "alice",
		Password:  []byte(dialTestPassword),
	}
	if err := client.Reconnect(context.Background(), cfg); err == nil {
		t.Fatal("first Reconnect() error = nil, want the dial failure")
	}
	if got := client.State(); got != Closed {
		t.Errorf("State() after failed reconnect = %s, want %s", got, Closed)
	}
	if err := client.Reconnect(context.Background(), cfg); err != nil {
		t.Fatalf("retry Reconnect() error = %v", err)
	}
	if got := client.State(); got != Ready {
		t.Errorf("State() after retry = %s, want %s", got, Ready)
	}
	if _, err := client.Do(context.Background(), "/system/identity/print", Request{}); err != nil {
		t.Fatalf("Do() after reconnect retry error = %v", err)
	}
}

func TestInvalidReconnectConfigLeavesTheCurrentConnectionReady(t *testing.T) {
	client := dialWithTransportScripts(t, func(r *sentenceReader) {
		command := r.next()
		r.send("!done", ".tag="+r.tag(command))
		r.drain()
	})

	err := client.Reconnect(context.Background(), Config{})
	var connection *ConnectionError
	if !errors.As(err, &connection) {
		t.Fatalf("Reconnect() error = %v, want *ConnectionError", err)
	}
	if got := client.State(); got != Ready {
		t.Errorf("State() after invalid Reconnect() = %s, want %s", got, Ready)
	}
	if _, err := client.Do(context.Background(), "/system/identity/print", Request{}); err != nil {
		t.Fatalf("Do() after invalid Reconnect() error = %v", err)
	}
}

func TestReconnectUsesTheNewConfigAndDiscardsItsPassword(t *testing.T) {
	client := dialWithTransportScripts(t,
		func(r *sentenceReader) { r.drain() },
		func(r *sentenceReader) { r.drain() },
	)

	underlying := openTransport
	var got transport.Options
	openTransport = func(ctx context.Context, opts transport.Options) (*transport.Conn, error) {
		got = opts
		got.Password = append([]byte(nil), opts.Password...)
		return underlying(ctx, opts)
	}
	t.Cleanup(func() {
		clear(got.Password)
		openTransport = underlying
	})

	password := []byte(dialTestPassword)
	caller := append([]byte(nil), password...)
	cfg := Config{
		Host:      "replacement.test",
		Port:      12345,
		Plaintext: true,
		Username:  "bob",
		Password:  password,
	}
	if err := client.Reconnect(context.Background(), cfg); err != nil {
		t.Fatalf("Reconnect() error = %v", err)
	}
	if got.Address != "replacement.test:12345" {
		t.Errorf("reconnect address = %q, want replacement.test:12345", got.Address)
	}
	if got.Username != "bob" {
		t.Errorf("reconnect username = %q, want bob", got.Username)
	}
	if string(got.Password) != dialTestPassword {
		t.Error("reconnect transport did not receive the new password")
	}
	if client.cfg.Password != nil {
		t.Error("client retained the reconnect password after login")
	}
	if !bytes.Equal(cfg.Password, caller) {
		t.Error("Reconnect() modified the caller password")
	}
}

func TestReconnectAfterCloseIsRejected(t *testing.T) {
	client := dialWithTransportScripts(t, func(r *sentenceReader) { r.drain() })
	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	err := client.Reconnect(context.Background(), Config{
		Host:      "router.test",
		Plaintext: true,
		Username:  "alice",
		Password:  []byte(dialTestPassword),
	})
	if !errors.Is(err, &ClosedError{}) {
		t.Errorf("Reconnect() after Close() error = %v, want *ClosedError", err)
	}
}

func TestCloseDuringReconnectIsFinal(t *testing.T) {
	client := dialWithTransportScripts(t, func(r *sentenceReader) { r.drain() })

	underlying := openTransport
	entered := make(chan struct{})
	openTransport = func(ctx context.Context, opts transport.Options) (*transport.Conn, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	t.Cleanup(func() { openTransport = underlying })

	reconnected := make(chan error, 1)
	go func() {
		reconnected <- client.Reconnect(context.Background(), Config{
			Host:      "router.test",
			Plaintext: true,
			Username:  "alice",
			Password:  []byte(dialTestPassword),
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Reconnect() did not begin dialing")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case err := <-reconnected:
		if !errors.Is(err, &ClosedError{}) {
			t.Errorf("Reconnect() after Close() error = %v, want *ClosedError", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Reconnect() did not stop after Close()")
	}
	if got := client.State(); got != Closed {
		t.Errorf("State() after Close() = %s, want %s", got, Closed)
	}
	if _, err := client.Do(context.Background(), "/system/identity/print", Request{}); !errors.Is(err, &ClosedError{}) {
		t.Errorf("Do() after Close() error = %v, want *ClosedError", err)
	}
}
