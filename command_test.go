package routeros

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/filydottech/ros-go/internal/testkit"
	"github.com/filydottech/ros-go/internal/wire"
)

// sentenceReader decodes what the client sends on the server end of the pipe.
// It keeps decoded sentences that arrive in one read so a caller can consume
// them one at a time.
type sentenceReader struct {
	t       *testing.T
	conn    net.Conn
	decoder *wire.Decoder
	queue   [][]string
}

func newSentenceReader(t *testing.T, conn net.Conn) *sentenceReader {
	t.Helper()
	return &sentenceReader{t: t, conn: conn, decoder: wire.NewDecoder(wire.DefaultLimits())}
}

func (r *sentenceReader) next() []string {
	r.t.Helper()
	for len(r.queue) == 0 {
		if err := r.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			r.t.Fatalf("server read deadline: %v", err)
		}
		buf := make([]byte, 4096)
		n, err := r.conn.Read(buf)
		if err != nil {
			r.t.Fatalf("server read: %v", err)
		}
		sentences, err := r.decoder.Feed(buf[:n])
		if err != nil {
			r.t.Fatalf("server decode: %v", err)
		}
		r.queue = append(r.queue, sentences...)
	}
	words := r.queue[0]
	r.queue = r.queue[1:]
	return words
}

func (r *sentenceReader) send(words ...string) {
	r.t.Helper()
	if err := r.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		r.t.Fatalf("server write deadline: %v", err)
	}
	if _, err := r.conn.Write(testkit.EncodeSentence(words)); err != nil {
		r.t.Fatalf("server write: %v", err)
	}
}

func (r *sentenceReader) login() {
	r.t.Helper()
	words := r.next()
	if len(words) == 0 || words[0] != "/login" {
		r.t.Fatalf("first sentence = %q, want /login", words)
	}
	r.send("!done")
}

func (r *sentenceReader) tag(words []string) string {
	r.t.Helper()
	for _, word := range words {
		if strings.HasPrefix(word, ".tag=") {
			return strings.TrimPrefix(word, ".tag=")
		}
	}
	r.t.Fatalf("sentence %q has no tag", words)
	return ""
}

// drain reads until the client closes, with no deadline left over from next.
func (r *sentenceReader) drain() {
	_ = r.conn.SetReadDeadline(time.Time{})
	buf := make([]byte, 512)
	for {
		if _, err := r.conn.Read(buf); err != nil {
			return
		}
	}
}

func dialPlain(t *testing.T, cfg Config, script func(*sentenceReader)) *Client {
	t.Helper()
	pipeTransport(t, nil, func(conn net.Conn) {
		reader := newSentenceReader(t, conn)
		reader.login()
		script(reader)
	})

	cfg.Host = "router.test"
	cfg.Plaintext = true
	cfg.Username = "alice"
	cfg.Password = []byte(dialTestPassword)
	client, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestDoReturnsLosslessResult(t *testing.T) {
	sent := make(chan []string, 1)
	client := dialPlain(t, Config{}, func(r *sentenceReader) {
		words := r.next()
		sent <- words
		tag := r.tag(words)
		r.send("!re", "=name=ether1", "=type=ether", ".tag="+tag)
		r.send("!empty", ".tag="+tag)
		r.send("!re", "=name=ether2", ".tag="+tag)
		r.send("!done", "=ret=*1", ".tag="+tag)
		r.drain()
	})

	result, err := client.Do(context.Background(), "/interface/print", Request{
		Attributes: []Attribute{{Name: "detail", Value: ""}},
		Proplist:   []string{"name", "type"},
		Query:      []string{"?name=ether1"},
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}

	words := <-sent
	want := []string{"/interface/print", "=detail=", ".proplist=name,type", "?name=ether1"}
	if len(words) != len(want)+1 {
		t.Fatalf("sentence = %q, want %d words plus a tag", words, len(want))
	}
	for i, word := range want {
		if words[i] != word {
			t.Errorf("word %d = %q, want %q", i, words[i], word)
		}
	}
	if !strings.HasPrefix(words[len(words)-1], ".tag=") {
		t.Errorf("last word = %q, want a tag", words[len(words)-1])
	}

	if len(result.Records) != 2 {
		t.Fatalf("Records = %+v, want two records", result.Records)
	}
	if result.Records[0]["name"] != "ether1" || result.Records[1]["name"] != "ether2" {
		t.Errorf("Records = %+v, want wire order ether1 then ether2", result.Records)
	}
	if result.Done["ret"] != "*1" {
		t.Errorf("Done = %+v, want ret=*1", result.Done)
	}
}

func TestDoReportsTrapWithDoneAttributes(t *testing.T) {
	client := dialPlain(t, Config{}, func(r *sentenceReader) {
		tag := r.tag(r.next())
		r.send("!trap", "=category=0", "=message=no such item", ".tag="+tag)
		r.send("!done", "=ret=*2", ".tag="+tag)
		r.drain()
	})

	_, err := client.Do(context.Background(), "/interface/remove", Request{
		Attributes: []Attribute{{Name: "numbers", Value: "99"}},
	})

	var trap *TrapError
	if !errors.As(err, &trap) {
		t.Fatalf("Do() error = %v, want *TrapError", err)
	}
	if trap.Category != "0" {
		t.Errorf("Category = %q, want 0", trap.Category)
	}
	if trap.Message != "no such item" {
		t.Errorf("Message = %q, want %q", trap.Message, "no such item")
	}
	if trap.Done["ret"] != "*2" {
		t.Errorf("Done = %+v, want ret=*2 from the terminating !done", trap.Done)
	}
}

func TestDoAddWithoutRetIsNonFatal(t *testing.T) {
	client := dialPlain(t, Config{}, func(r *sentenceReader) {
		tag := r.tag(r.next())
		r.send("!done", ".tag="+tag)
		next := r.next()
		r.send("!done", ".tag="+r.tag(next))
		r.drain()
	})

	_, err := client.Do(context.Background(), "/ip/address/add", Request{
		Attributes: []Attribute{{Name: "address", Value: "192.0.2.1/32"}},
	})
	var protocol *ProtocolError
	if !errors.As(err, &protocol) {
		t.Fatalf("Do() error = %v, want *ProtocolError", err)
	}
	if !strings.Contains(protocol.Detail, "ret") {
		t.Errorf("Detail = %q, want it to name the missing ret attribute", protocol.Detail)
	}

	if _, err := client.Do(context.Background(), "/system/identity/print", Request{}); err != nil {
		t.Fatalf("Do() after a missing ret error = %v, want a usable connection", err)
	}
}

func TestDoAddWithRetSucceeds(t *testing.T) {
	client := dialPlain(t, Config{}, func(r *sentenceReader) {
		tag := r.tag(r.next())
		r.send("!done", "=ret=*3", ".tag="+tag)
		r.drain()
	})

	result, err := client.Do(context.Background(), "/ip/address/add", Request{
		Attributes: []Attribute{{Name: "address", Value: "192.0.2.1/32"}},
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if result.Done["ret"] != "*3" {
		t.Errorf("Done = %+v, want ret=*3", result.Done)
	}
}

func TestDoDoesNotResolveOnEmpty(t *testing.T) {
	emptySent := make(chan struct{})
	release := make(chan struct{})
	client := dialPlain(t, Config{}, func(r *sentenceReader) {
		tag := r.tag(r.next())
		r.send("!empty", ".tag="+tag)
		close(emptySent)
		<-release
		r.send("!done", ".tag="+tag)
		r.drain()
	})

	type outcome struct {
		result Result
		err    error
	}
	results := make(chan outcome, 1)
	go func() {
		result, err := client.Do(context.Background(), "/ping", Request{})
		results <- outcome{result: result, err: err}
	}()

	<-emptySent
	select {
	case got := <-results:
		t.Fatalf("Do() = %+v, %v, want it to wait for !done", got.result, got.err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case got := <-results:
		if got.err != nil {
			t.Fatalf("Do() error = %v", got.err)
		}
		if len(got.result.Records) != 0 {
			t.Errorf("Records = %+v, want none", got.result.Records)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Do() did not return after !done")
	}
}

func TestDoCancellationSendsCancelAndLeavesConnectionUsable(t *testing.T) {
	commandSeen := make(chan struct{})
	cancelSeen := make(chan struct{})
	client := dialPlain(t, Config{}, func(r *sentenceReader) {
		tag := r.tag(r.next())
		close(commandSeen)
		cancel := r.next()
		if len(cancel) != 2 || cancel[0] != "/cancel" || cancel[1] != "=tag="+tag {
			t.Errorf("cancel sentence = %q, want /cancel =tag=%s", cancel, tag)
			return
		}
		close(cancelSeen)
		r.send("!done")
		r.send("!done", ".tag="+tag)
		next := r.next()
		r.send("!done", ".tag="+r.tag(next))
		r.drain()
	})

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	type outcome struct{ err error }
	results := make(chan outcome, 1)
	go func() {
		_, err := client.Do(ctx, "/tool/ping", Request{})
		results <- outcome{err: err}
	}()

	select {
	case <-commandSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive the command")
	}
	stop()
	select {
	case got := <-results:
		var cancelled *CancelledError
		if !errors.As(got.err, &cancelled) {
			t.Fatalf("Do() error = %v, want *CancelledError", got.err)
		}
		if !errors.Is(got.err, context.Canceled) {
			t.Errorf("Do() error = %v, want it to wrap context.Canceled", got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Do() did not return after cancellation")
	}

	select {
	case <-cancelSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive /cancel")
	}

	if _, err := client.Do(context.Background(), "/system/resource/print", Request{}); err != nil {
		t.Fatalf("Do() after cancellation = %v, want a usable connection", err)
	}
}

func TestDoTimeoutSendsCancelOnceAndDoesNotRetry(t *testing.T) {
	commands := make(chan string, 4)
	client := dialPlain(t, Config{CommandTimeout: 100 * time.Millisecond}, func(r *sentenceReader) {
		words := r.next()
		commands <- words[0]
		tag := r.tag(words)
		cancel := r.next()
		commands <- cancel[0]
		r.send("!done")
		r.send("!empty", ".tag="+tag)
		r.send("!done", ".tag="+tag)
		next := r.next()
		commands <- next[0]
		r.send("!done", ".tag="+r.tag(next))
		r.drain()
	})

	_, err := client.Do(context.Background(), "/ip/address/add", Request{
		Attributes: []Attribute{{Name: "address", Value: "192.0.2.1/32"}},
	})
	var timeout *TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("Do() error = %v, want *TimeoutError", err)
	}
	if timeout.Phase != "command" {
		t.Errorf("Phase = %q, want %q", timeout.Phase, "command")
	}
	if timeout.Duration != 100*time.Millisecond {
		t.Errorf("Duration = %v, want 100ms", timeout.Duration)
	}

	if _, err := client.Do(context.Background(), "/system/resource/print", Request{}); err != nil {
		t.Fatalf("Do() after timeout = %v, want a usable connection", err)
	}

	got := []string{<-commands, <-commands, <-commands}
	want := []string{"/ip/address/add", "/cancel", "/system/resource/print"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("command %d = %q, want %q (a mutating command must not be retried)", i, got[i], want[i])
		}
	}
}

func TestDoRejectsInvalidRequestBeforeSending(t *testing.T) {
	sent := make(chan [][]string, 1)
	client := dialPlain(t, Config{}, func(r *sentenceReader) {
		var all [][]string
		for {
			_ = r.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			buf := make([]byte, 4096)
			n, err := r.conn.Read(buf)
			if err != nil {
				break
			}
			sentences, err := r.decoder.Feed(buf[:n])
			if err != nil {
				t.Errorf("server decode: %v", err)
				break
			}
			all = append(all, sentences...)
		}
		sent <- all
	})

	const secret = "sentinel-secret-value"
	invalid := []struct {
		name string
		path string
		req  Request
	}{
		{name: "empty path", path: "", req: Request{}},
		{name: "empty attribute name", path: "/print", req: Request{
			Attributes: []Attribute{{Name: "", Value: "x"}},
		}},
		{name: "period attribute name", path: "/print", req: Request{
			Attributes: []Attribute{{Name: ".proplist", Value: "name"}},
		}},
		{name: "equals in attribute name", path: "/print", req: Request{
			Attributes: []Attribute{{Name: "a=b", Value: "x"}},
		}},
		{name: "repeated attribute name", path: "/print", req: Request{
			Attributes: []Attribute{{Name: "a", Value: "1"}, {Name: "a", Value: "2"}},
		}},
		{name: "query without question mark", path: "/print", req: Request{
			Query: []string{"name=ether1"},
		}},
		{name: "secret in attribute name", path: "/print", req: Request{
			Attributes: []Attribute{{Name: "name=" + secret}},
		}},
		{name: "secret in query", path: "/print", req: Request{
			Query: []string{secret},
		}},
	}

	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.Do(context.Background(), test.path, test.req)
			var connection *ConnectionError
			if !errors.As(err, &connection) {
				t.Fatalf("Do() error = %v, want *ConnectionError", err)
			}
			if connection.Op != "do" {
				t.Errorf("Op = %q, want do", connection.Op)
			}
			if connection.Err == nil {
				t.Error("Err = nil, want the validation failure")
			}
			if strings.Contains(err.Error(), secret) {
				t.Errorf("Do() error = %q, must not echo the rejected input", err)
			}
		})
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case all := <-sent:
		if len(all) != 0 {
			t.Fatalf("server received %q, want an invalid request to send nothing", all)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not observe the closed connection")
	}

	var closed *ClosedError
	if _, err := client.Do(context.Background(), "/print", Request{}); !errors.As(err, &closed) {
		t.Fatalf("Do() after Close() error = %v, want *ClosedError", err)
	}
}
