package wire

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseReplyKinds(t *testing.T) {
	cases := []struct {
		words []string
		kind  Kind
	}{
		{[]string{"!re", "=name=ether1"}, KindReply},
		{[]string{"!done", "=ret=*1"}, KindDone},
		{[]string{"!trap", "=message=no such item"}, KindTrap},
		{[]string{"!empty", ".tag=4"}, KindEmpty},
		{[]string{"!fatal", "session closed by remote host"}, KindFatal},
	}

	for _, tc := range cases {
		reply, err := ParseReply(tc.words)
		if err != nil {
			t.Fatalf("ParseReply(%q) error = %v", tc.words, err)
		}
		if reply.Kind != tc.kind {
			t.Errorf("ParseReply(%q) kind = %v, want %v", tc.words, reply.Kind, tc.kind)
		}
	}
}

func TestParseReplyAttributes(t *testing.T) {
	reply, err := ParseReply([]string{
		"!re",
		"=name=ether1",
		"=value=a=b=c",
		"=duplicate=first",
		"=duplicate=last",
		"=flag=",
		".tag=17",
		".proplist=name,type",
	})
	if err != nil {
		t.Fatalf("ParseReply() error = %v", err)
	}

	wantAttributes := map[string]string{
		"name":      "ether1",
		"value":     "a=b=c",
		"duplicate": "last",
		"flag":      "",
	}
	if !reflect.DeepEqual(reply.Attributes, wantAttributes) {
		t.Errorf("Attributes = %v, want %v", reply.Attributes, wantAttributes)
	}
	if !reply.HasTag || reply.Tag != "17" {
		t.Errorf("tag = %q, HasTag = %t; want \"17\" and true", reply.Tag, reply.HasTag)
	}
	wantAPI := map[string]string{"proplist": "name,type"}
	if !reflect.DeepEqual(reply.APIAttributes, wantAPI) {
		t.Errorf("APIAttributes = %v, want %v", reply.APIAttributes, wantAPI)
	}
}

func TestReplyAttributeBytes(t *testing.T) {
	reply := Reply{
		Attributes: map[string]string{
			"name":    "ether1",
			"comment": "test",
		},
	}

	if got, want := ReplyAttributeBytes(reply), len("name")+len("ether1")+len("comment")+len("test"); got != want {
		t.Errorf("ReplyAttributeBytes() = %d, want %d", got, want)
	}
}

func TestParseReplyTreatsEmptyTagAsUntagged(t *testing.T) {
	reply, err := ParseReply([]string{"!done", ".tag="})
	if err != nil {
		t.Fatalf("ParseReply() error = %v", err)
	}
	if reply.HasTag || reply.Tag != "" {
		t.Errorf("tag = %q, HasTag = %t; want untagged", reply.Tag, reply.HasTag)
	}
}

func TestParseReplyFatalReason(t *testing.T) {
	reply, err := ParseReply([]string{"!fatal", "session", "closed"})
	if err != nil {
		t.Fatalf("ParseReply() error = %v", err)
	}
	if reply.FatalReason != "session closed" {
		t.Errorf("FatalReason = %q, want %q", reply.FatalReason, "session closed")
	}
	if !reflect.DeepEqual(reply.Plain, []string{"session", "closed"}) {
		t.Errorf("Plain = %q, want the two reason words", reply.Plain)
	}
}

func TestParseReplyRejectsMalformedSentences(t *testing.T) {
	cases := [][]string{
		nil,
		{},
		{"!bogus"},
		{"done"},
		{"!re", "="},
		{"!re", "==value"},
		{"!re", "."},
		{"!re", ".=value"},
	}

	for _, words := range cases {
		_, err := ParseReply(words)
		var protocolErr *ProtocolError
		if !errors.As(err, &protocolErr) {
			t.Errorf("ParseReply(%q) error = %v, want *ProtocolError", words, err)
		}
	}
}
