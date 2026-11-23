package wire

import (
	"errors"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func mustEncode(t *testing.T, words ...string) []byte {
	t.Helper()
	encoded, err := NewEncoder(DefaultLimits()).Sentence(words)
	if err != nil {
		t.Fatalf("Sentence(%q) error = %v", words, err)
	}
	return encoded
}

func feedAll(t *testing.T, decoder *Decoder, chunks ...[]byte) [][]string {
	t.Helper()
	var sentences [][]string
	for _, chunk := range chunks {
		got, err := decoder.Feed(chunk)
		if err != nil {
			t.Fatalf("Feed() error = %v", err)
		}
		sentences = append(sentences, got...)
	}
	return sentences
}

func TestDecoderAcceptsSplitAtEveryBoundary(t *testing.T) {
	stream := append(mustEncode(t, "!re", "=name=café", "=value=a=b"), mustEncode(t, "!done", "=ret=*1")...)
	want := [][]string{
		{"!re", "=name=café", "=value=a=b"},
		{"!done", "=ret=*1"},
	}

	for split := 0; split <= len(stream); split++ {
		decoder := NewDecoder(DefaultLimits())
		got := feedAll(t, decoder, stream[:split], stream[split:])
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("split at %d decoded %q, want %q", split, got, want)
		}
	}
}

func TestDecoderAcceptsOneByteAtATime(t *testing.T) {
	stream := append(append(mustEncode(t, "!re", "=name=café"), mustEncode(t, "!empty")...), mustEncode(t, "!done")...)
	want := [][]string{{"!re", "=name=café"}, {"!empty"}, {"!done"}}

	decoder := NewDecoder(DefaultLimits())
	var got [][]string
	for index := range stream {
		got = append(got, feedAll(t, decoder, stream[index:index+1])...)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("byte-by-byte decoded %q, want %q", got, want)
	}
}

func TestDecoderAcceptsArbitraryChunking(t *testing.T) {
	stream := append(mustEncode(t, "!re", "=name=café"), mustEncode(t, "!done")...)
	want := [][]string{{"!re", "=name=café"}, {"!done"}}

	random := rand.New(rand.NewSource(1))
	for run := 0; run < 50; run++ {
		decoder := NewDecoder(DefaultLimits())
		var got [][]string
		for offset := 0; offset < len(stream); {
			size := random.Intn(7) + 1
			if offset+size > len(stream) {
				size = len(stream) - offset
			}
			got = append(got, feedAll(t, decoder, stream[offset:offset+size])...)
			offset += size
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("run %d decoded %q, want %q", run, got, want)
		}
	}
}

func TestDecoderIgnoresEmptySentences(t *testing.T) {
	stream := []byte{0x00}
	stream = append(stream, mustEncode(t, "!done")...)
	stream = append(stream, 0x00, 0x00)

	got := feedAll(t, NewDecoder(DefaultLimits()), stream)
	if len(got) != 1 || !reflect.DeepEqual(got[0], []string{"!done"}) {
		t.Fatalf("decoded %q, want only the !done sentence", got)
	}
}

func TestDecoderPreservesPayloadBytes(t *testing.T) {
	payload := []byte{'a', 0x00, 'b', 0xff, 'c'}
	frame := AppendLength(nil, uint32(len(payload)))
	frame = append(frame, payload...)
	frame = append(frame, 0x00)

	got := feedAll(t, NewDecoder(DefaultLimits()), frame)
	want := [][]string{{string(payload)}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded %q, want %q", got, want)
	}
}

func TestDecoderEnforcesLimits(t *testing.T) {
	cases := []struct {
		name   string
		limits Limits
		words  []string
		scope  string
		limit  int64
	}{
		{
			name:   "word",
			limits: Limits{MaxWordBytes: 4, MaxSentenceBytes: 64, MaxWordsPerSentence: 8},
			words:  []string{"abcde"},
			scope:  "word",
			limit:  4,
		},
		{
			name:   "sentence",
			limits: Limits{MaxWordBytes: 8, MaxSentenceBytes: 5, MaxWordsPerSentence: 8},
			words:  []string{"abc", "def"},
			scope:  "sentence",
			limit:  5,
		},
		{
			name:   "words per sentence",
			limits: Limits{MaxWordBytes: 8, MaxSentenceBytes: 64, MaxWordsPerSentence: 2},
			words:  []string{"a", "b", "c"},
			scope:  "words per sentence",
			limit:  2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := mustEncode(t, tc.words...)
			decoder := NewDecoder(tc.limits)
			_, err := decoder.Feed(encoded)
			var limitErr *LimitError
			if !errors.As(err, &limitErr) {
				t.Fatalf("Feed() error = %v, want *LimitError", err)
			}
			if limitErr.Scope != tc.scope || limitErr.Limit != tc.limit {
				t.Errorf("LimitError = %+v, want scope %q and limit %d", limitErr, tc.scope, tc.limit)
			}

			if _, again := decoder.Feed(nil); again != err {
				t.Errorf("second Feed() error = %v, want the first error %v", again, err)
			}
		})
	}
}

func TestDecoderRejectsOversizedPrefixBeforeReadingPayload(t *testing.T) {
	decoder := NewDecoder(Limits{MaxWordBytes: 16, MaxSentenceBytes: 64, MaxWordsPerSentence: 8})
	_, err := decoder.Feed(AppendLength(nil, MaxWireLength))

	var limitErr *LimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("Feed() error = %v, want *LimitError", err)
	}
	if limitErr.Observed != int64(MaxWireLength) {
		t.Errorf("Observed = %d, want %d", limitErr.Observed, int64(MaxWireLength))
	}
}

func TestDecoderRejectsReservedControlByte(t *testing.T) {
	decoder := NewDecoder(DefaultLimits())
	_, err := decoder.Feed([]byte{0xf8, 0x00})
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("Feed() error = %v, want *ProtocolError", err)
	}
	if !strings.Contains(protocolErr.Detail, "0xf8") {
		t.Errorf("Detail = %q, want it to name 0xf8", protocolErr.Detail)
	}
}
