package testkit_test

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/filydottech/ros-go/internal/testkit"
	"github.com/filydottech/ros-go/internal/wire"
)

func TestIndependentCodecAgreesWithWireAtEveryTier(t *testing.T) {
	sizes := []int{1, 0x7f, 0x80, 0x3fff, 0x4000, 0x1fffff, 0x200000}
	for _, size := range sizes {
		word := strings.Repeat("a", size)
		words := []string{"!re", "=value=" + word}

		independent := testkit.EncodeSentence(words)
		production, err := wire.NewEncoder(wire.DefaultLimits()).Sentence(words)
		if err != nil {
			t.Fatalf("Sentence(%d-byte word) error = %v", size, err)
		}
		if !bytes.Equal(independent, production) {
			t.Fatalf("%d-byte word: independent and wire encodings differ", size)
		}

		decoded, rest, err := testkit.DecodeSentences(production)
		if err != nil {
			t.Fatalf("%d-byte word: independent decode error = %v", size, err)
		}
		if len(rest) != 0 {
			t.Fatalf("%d-byte word: independent decode left %d bytes", size, len(rest))
		}
		if !reflect.DeepEqual(decoded, [][]string{words}) {
			t.Fatalf("%d-byte word: independent decode = %q, want %q", size, decoded, [][]string{words})
		}

		fromWire, err := wire.NewDecoder(wire.DefaultLimits()).Feed(independent)
		if err != nil {
			t.Fatalf("%d-byte word: wire decode error = %v", size, err)
		}
		if !reflect.DeepEqual(fromWire, [][]string{words}) {
			t.Fatalf("%d-byte word: wire decode = %q, want %q", size, fromWire, [][]string{words})
		}
	}
}

func TestIndependentDecoderKeepsPartialInput(t *testing.T) {
	encoded := testkit.EncodeSentence([]string{"!done", "=ret=*1"})
	for cut := 0; cut < len(encoded); cut++ {
		// The last byte is the terminator, so a shorter prefix never completes
		// the sentence and must be handed back whole.
		sentences, rest, err := testkit.DecodeSentences(encoded[:cut])
		if err != nil {
			t.Fatalf("DecodeSentences(%d-byte prefix) error = %v", cut, err)
		}
		if len(sentences) != 0 {
			t.Fatalf("DecodeSentences(%d-byte prefix) = %q, want no complete sentence", cut, sentences)
		}
		if !bytes.Equal(rest, encoded[:cut]) {
			t.Fatalf("DecodeSentences(%d-byte prefix) rest lost bytes", cut)
		}
	}
}

func TestIndependentDecoderRejectsReservedControlByte(t *testing.T) {
	if _, _, err := testkit.DecodeSentences([]byte{0xf8, 0x00}); err == nil {
		t.Fatal("DecodeSentences(0xf8) error = nil, want a rejection")
	}
}
