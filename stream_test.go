package routeros

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestStreamYieldsRecordsThenEOF(t *testing.T) {
	client := dialPlain(t, Config{}, func(r *sentenceReader) {
		command := r.next()
		tag := r.tag(command)
		r.send("!re", "=name=ether1", ".tag="+tag)
		r.send("!re", "=name=ether2", ".tag="+tag)
		r.send("!done", ".tag="+tag)
		r.drain()
	})

	stream, err := client.Stream(context.Background(), "/interface/listen", Request{})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("first Recv() error = %v", err)
	}
	if first["name"] != "ether1" {
		t.Errorf("first Recv() = %+v, want ether1", first)
	}

	second, err := stream.Recv()
	if err != nil {
		t.Fatalf("second Recv() error = %v", err)
	}
	if second["name"] != "ether2" {
		t.Errorf("second Recv() = %+v, want ether2", second)
	}

	if _, err := stream.Recv(); err != io.EOF {
		t.Errorf("third Recv() error = %v, want io.EOF", err)
	}
}

func TestStreamCloseWakesRecvAndSendsOneCancel(t *testing.T) {
	cancelSeen := make(chan struct{})
	client := dialPlain(t, Config{}, func(r *sentenceReader) {
		command := r.next()
		tag := r.tag(command)
		cancel := r.next()
		if len(cancel) != 2 || cancel[0] != "/cancel" || cancel[1] != "=tag="+tag {
			t.Errorf("cancel sentence = %q, want /cancel =tag=%s", cancel, tag)
			return
		}
		close(cancelSeen)
		r.send("!done")
		r.send("!done", ".tag="+tag)
		next := r.next()
		if next[0] != "/system/identity/print" {
			t.Errorf("next sentence = %q, want /system/identity/print", next)
			return
		}
		r.send("!done", ".tag="+r.tag(next))
		r.drain()
	})

	stream, err := client.Stream(context.Background(), "/interface/listen", Request{})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	received := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		received <- err
	}()

	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	select {
	case <-cancelSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive /cancel")
	}
	select {
	case err := <-received:
		var cancelled *CancelledError
		if !errors.As(err, &cancelled) {
			t.Fatalf("Recv() error = %v, want *CancelledError", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Recv() did not wake after Close")
	}

	if _, err := client.Do(context.Background(), "/system/identity/print", Request{}); err != nil {
		t.Fatalf("Do() after Stream.Close() error = %v", err)
	}
}

func TestStreamRejectsAnOversizedRecordAndKeepsTheConnectionUsable(t *testing.T) {
	client := dialPlain(t, Config{StreamMaxBytes: 2}, func(r *sentenceReader) {
		command := r.next()
		tag := r.tag(command)
		r.send("!re", "=v=1234", ".tag="+tag)
		cancel := r.next()
		if len(cancel) != 2 || cancel[0] != "/cancel" || cancel[1] != "=tag="+tag {
			t.Errorf("cancel sentence = %q, want /cancel =tag=%s", cancel, tag)
			return
		}
		r.send("!done")
		r.send("!done", ".tag="+tag)
		next := r.next()
		r.send("!done", ".tag="+r.tag(next))
		r.drain()
	})

	stream, err := client.Stream(context.Background(), "/interface/listen", Request{})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	_, err = stream.Recv()
	var limit *ResourceLimitError
	if !errors.As(err, &limit) {
		t.Fatalf("Recv() error = %v, want *ResourceLimitError", err)
	}
	if limit.Scope != "stream bytes" || limit.Limit != 2 || limit.Observed != 5 {
		t.Errorf("limit = %+v, want stream bytes limit 2 observed 5", limit)
	}

	if _, err := client.Do(context.Background(), "/system/identity/print", Request{}); err != nil {
		t.Fatalf("Do() after oversized stream record error = %v", err)
	}
}

func TestStreamContextCancellationWakesRecv(t *testing.T) {
	serverFinished := make(chan struct{})
	client := dialPlain(t, Config{}, func(r *sentenceReader) {
		command := r.next()
		tag := r.tag(command)
		cancel := r.next()
		if len(cancel) != 2 || cancel[0] != "/cancel" || cancel[1] != "=tag="+tag {
			t.Errorf("cancel sentence = %q, want /cancel =tag=%s", cancel, tag)
			return
		}
		r.send("!done")
		r.send("!done", ".tag="+tag)
		close(serverFinished)
		r.drain()
	})

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	stream, err := client.Stream(ctx, "/interface/listen", Request{})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	received := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		received <- err
	}()
	stop()

	select {
	case err := <-received:
		var cancelled *CancelledError
		if !errors.As(err, &cancelled) {
			t.Fatalf("Recv() error = %v, want *CancelledError", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Recv() error = %v, want it to wrap context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Recv() did not wake after the stream context ended")
	}
	select {
	case <-serverFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not finish stream cancellation")
	}
}

func TestStreamReportsTrapWithDoneAttributes(t *testing.T) {
	client := dialPlain(t, Config{}, func(r *sentenceReader) {
		command := r.next()
		tag := r.tag(command)
		r.send("!trap", "=category=2", "=message=listener failed", ".tag="+tag)
		r.send("!done", "=ret=*7", ".tag="+tag)
		r.drain()
	})

	stream, err := client.Stream(context.Background(), "/interface/listen", Request{})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	_, err = stream.Recv()
	var trap *TrapError
	if !errors.As(err, &trap) {
		t.Fatalf("Recv() error = %v, want *TrapError", err)
	}
	if trap.Category != "2" || trap.Message != "listener failed" || trap.Done["ret"] != "*7" {
		t.Errorf("trap = %+v, want category 2, listener failed, and ret=*7", trap)
	}
}

func TestStreamEnforcesTheOpenStreamLimit(t *testing.T) {
	client := dialPlain(t, Config{MaxOpenStreams: 1}, func(r *sentenceReader) {
		command := r.next()
		tag := r.tag(command)
		cancel := r.next()
		if len(cancel) != 2 || cancel[0] != "/cancel" || cancel[1] != "=tag="+tag {
			t.Errorf("cancel sentence = %q, want /cancel =tag=%s", cancel, tag)
			return
		}
		r.send("!done")
		r.send("!done", ".tag="+tag)
		r.drain()
	})

	first, err := client.Stream(context.Background(), "/interface/listen", Request{})
	if err != nil {
		t.Fatalf("first Stream() error = %v", err)
	}

	_, err = client.Stream(context.Background(), "/log/listen", Request{})
	var limit *ResourceLimitError
	if !errors.As(err, &limit) {
		t.Fatalf("second Stream() error = %v, want *ResourceLimitError", err)
	}
	if limit.Scope != "open streams" || limit.Limit != 1 || limit.Observed != 2 {
		t.Errorf("limit = %+v, want open streams limit 1 observed 2", limit)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
}

func TestStreamClientCloseWakesRecvWithClosedError(t *testing.T) {
	client := dialPlain(t, Config{}, func(r *sentenceReader) {
		_ = r.next()
		r.drain()
	})
	stream, err := client.Stream(context.Background(), "/interface/listen", Request{})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	received := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		received <- err
	}()
	if err := client.Close(); err != nil {
		t.Fatalf("Client.Close() error = %v", err)
	}

	select {
	case err := <-received:
		var closed *ClosedError
		if !errors.As(err, &closed) {
			t.Fatalf("Recv() error = %v, want *ClosedError", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Recv() did not wake after Client.Close")
	}
}
