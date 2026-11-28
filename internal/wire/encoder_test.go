package wire

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestSentenceEncodesWordsAndTerminator(t *testing.T) {
	got, err := NewEncoder(DefaultLimits()).Sentence([]string{"!done", "=ret=*1"})
	if err != nil {
		t.Fatalf("Sentence() error = %v", err)
	}
	want := []byte{
		0x05, '!', 'd', 'o', 'n', 'e',
		0x07, '=', 'r', 'e', 't', '=', '*', '1',
		0x00,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Sentence() = % x, want % x", got, want)
	}
}

func TestSentenceMeasuresUTF8BytesNotRunes(t *testing.T) {
	got, err := NewEncoder(DefaultLimits()).Sentence([]string{"=comment=café"})
	if err != nil {
		t.Fatalf("Sentence() error = %v", err)
	}
	prefix, ok, err := DecodeLength(got)
	if err != nil || !ok {
		t.Fatalf("DecodeLength() = ok %t, err %v", ok, err)
	}
	if want := len("=comment=café"); prefix.Length != uint32(want) {
		t.Errorf("word length = %d, want %d", prefix.Length, want)
	}
}

func TestSentenceBytesPreservesUTF8Words(t *testing.T) {
	password := []byte("not-a-real-password=café")
	passwordWord := append([]byte("=password="), password...)
	words := [][]byte{[]byte("/login"), passwordWord}

	got, err := NewEncoder(DefaultLimits()).SentenceBytes(words)
	if err != nil {
		t.Fatalf("SentenceBytes() error = %v", err)
	}
	want := AppendLength(nil, uint32(len(words[0])))
	want = append(want, words[0]...)
	want = AppendLength(want, uint32(len(words[1])))
	want = append(want, words[1]...)
	want = append(want, 0)
	if !bytes.Equal(got, want) {
		t.Errorf("SentenceBytes() = % x, want % x", got, want)
	}
}

func TestSentenceRejectsEmptyInput(t *testing.T) {
	encoder := NewEncoder(DefaultLimits())
	if _, err := encoder.Sentence(nil); err == nil {
		t.Error("Sentence(nil) error = nil, want a *ProtocolError")
	}
	if _, err := encoder.Sentence([]string{"!done", ""}); err == nil {
		t.Error("Sentence with an empty word error = nil, want a *ProtocolError")
	}
}

func TestSentenceEnforcesWordLimit(t *testing.T) {
	encoder := NewEncoder(Limits{MaxWordBytes: 4, MaxSentenceBytes: 64, MaxWordsPerSentence: 8})

	if _, err := encoder.Sentence([]string{"abcd"}); err != nil {
		t.Fatalf("Sentence(word at the limit) error = %v, want nil", err)
	}

	_, err := encoder.Sentence([]string{"abcde"})
	var limitErr *LimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("Sentence(oversized word) error = %v, want *LimitError", err)
	}
	if limitErr.Scope != "word" || limitErr.Limit != 4 || limitErr.Observed != 5 {
		t.Errorf("LimitError = %+v, want scope word, limit 4, observed 5", limitErr)
	}
}

func TestAppendSentenceLeavesDstUnchangedOnError(t *testing.T) {
	dst := []byte("prefix")
	got, err := NewEncoder(DefaultLimits()).AppendSentence(dst, []string{"!done", ""})
	if err == nil {
		t.Fatal("AppendSentence() error = nil, want a rejection")
	}
	if string(got) != "prefix" {
		t.Errorf("AppendSentence() = %q, want %q", got, "prefix")
	}
}

func TestAppendSentenceBytesLeavesDstUnchangedOnError(t *testing.T) {
	dst := []byte("prefix")
	got, err := NewEncoder(DefaultLimits()).AppendSentenceBytes(dst, [][]byte{[]byte("!done"), nil})
	if err == nil {
		t.Fatal("AppendSentenceBytes() error = nil, want a rejection")
	}
	if string(got) != "prefix" {
		t.Errorf("AppendSentenceBytes() = %q, want %q", got, "prefix")
	}
}

func TestSentenceLargeWordSpansOneFrame(t *testing.T) {
	word := "=comment=" + strings.Repeat("a", 0x7f)
	encoded, err := NewEncoder(DefaultLimits()).Sentence([]string{word})
	if err != nil {
		t.Fatalf("Sentence() error = %v", err)
	}
	prefix, ok, err := DecodeLength(encoded)
	if err != nil || !ok {
		t.Fatalf("DecodeLength() = ok %t, err %v", ok, err)
	}
	if prefix.Size != 2 {
		t.Errorf("prefix size = %d, want 2 for a %d-byte word", prefix.Size, len(word))
	}
}

func TestSentenceEnforcesSentenceByteLimit(t *testing.T) {
	encoder := NewEncoder(Limits{MaxWordBytes: 16, MaxSentenceBytes: 4, MaxWordsPerSentence: 8})

	if _, err := encoder.Sentence([]string{"ab", "cd"}); err != nil {
		t.Fatalf("Sentence(payload at the limit) error = %v, want nil", err)
	}

	dst, err := encoder.AppendSentence([]byte("prefix"), []string{"ab", "cde"})
	var limitErr *LimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("Sentence(oversized sentence) error = %v, want *LimitError", err)
	}
	if limitErr.Scope != "sentence" || limitErr.Limit != 4 || limitErr.Observed != 5 {
		t.Errorf("LimitError = %+v, want scope sentence, limit 4, observed 5", limitErr)
	}
	if string(dst) != "prefix" {
		t.Errorf("AppendSentence() = %q, want dst unchanged", dst)
	}
}

func TestSentenceEnforcesWordCountLimit(t *testing.T) {
	encoder := NewEncoder(Limits{MaxSentenceBytes: 64, MaxWordsPerSentence: 2})

	if _, err := encoder.Sentence([]string{"a", "b"}); err != nil {
		t.Fatalf("Sentence(words at the limit) error = %v, want nil", err)
	}

	_, err := encoder.Sentence([]string{"a", "b", "c"})
	var limitErr *LimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("Sentence(too many words) error = %v, want *LimitError", err)
	}
	if limitErr.Scope != "words per sentence" || limitErr.Limit != 2 || limitErr.Observed != 3 {
		t.Errorf("LimitError = %+v, want scope words per sentence, limit 2, observed 3", limitErr)
	}
}
