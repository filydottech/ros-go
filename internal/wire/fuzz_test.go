package wire

import (
	"reflect"
	"strings"
	"testing"
)

func FuzzDecoder(f *testing.F) {
	f.Add([]byte{0x06, '!', 'd', 'o', 'n', 'e', 0x00})
	f.Add([]byte{0x07, '!', 'r', 'e', 0x00, '=', 'x', '=', '1', 0x00, 0x00})
	f.Add([]byte{0xf0, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0x80, 0x80})
	f.Add([]byte{0xf8})

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := NewDecoder(Limits{MaxWordBytes: 1 << 10, MaxSentenceBytes: 1 << 12, MaxWordsPerSentence: 8})
		for index := 0; index < len(data); index++ {
			if _, err := decoder.Feed(data[index : index+1]); err != nil {
				if _, again := decoder.Feed(data[index+1:]); again != err {
					t.Fatalf("decoder error is not sticky: %v then %v", err, again)
				}
				return
			}
		}
	})
}

func FuzzParseReply(f *testing.F) {
	f.Add("!done\x00=ret=*1")
	f.Add("!re\x00=name=ether1\x00=value=a=b")
	f.Add("!trap\x00=category=0\x00=message=no such item")
	f.Add("!fatal\x00session closed")
	f.Add("!unknown")

	f.Fuzz(func(t *testing.T, sentence string) {
		reply, err := ParseReply(strings.Split(sentence, "\x00"))
		if err != nil {
			return
		}
		if reply.Kind < KindReply || reply.Kind > KindFatal {
			t.Fatalf("ParseReply() kind = %v, want one of the five reply kinds", reply.Kind)
		}
		for name, value := range reply.Attributes {
			if name == "" {
				t.Fatal("ParseReply() kept an attribute with an empty name")
			}
			if strings.Contains(value, "\x00") {
				t.Fatalf("ParseReply() value for %q contains a NUL byte", name)
			}
		}
	})
}

func FuzzSentenceRoundTrip(f *testing.F) {
	f.Add("!done", "=ret=*1")
	f.Add("!re", "=comment=café")
	f.Add("!re", "")

	limits := Limits{MaxWordBytes: 1 << 12, MaxSentenceBytes: 1 << 16, MaxWordsPerSentence: 2}
	f.Fuzz(func(t *testing.T, first, second string) {
		if first == "" || second == "" {
			return
		}
		encoded, err := NewEncoder(limits).Sentence([]string{first, second})
		if err != nil {
			return
		}
		decoded, err := NewDecoder(limits).Feed(encoded)
		if err != nil {
			t.Fatalf("Feed(encoded) error = %v", err)
		}
		want := [][]string{{first, second}}
		if !reflect.DeepEqual(decoded, want) {
			t.Fatalf("round trip = %q, want %q", decoded, want)
		}
	})
}
