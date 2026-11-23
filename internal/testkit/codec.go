// Package testkit holds in-memory fixtures for tests. Its codec is written
// independently of internal/wire so that a wire test compares two
// implementations instead of one implementation with itself.
package testkit

import (
	"encoding/binary"
	"fmt"
)

// EncodeSentence writes words as one complete RouterOS sentence.
func EncodeSentence(words []string) []byte {
	out := make([]byte, 0, len(words)*8)
	for _, word := range words {
		out = EncodeWord(out, word)
	}
	return append(out, 0)
}

// EncodeWord appends one length-prefixed UTF-8 word to dst. It derives the
// prefix by placing the length big-endian in a five-byte buffer and then
// emitting only the bytes its tier uses.
func EncodeWord(dst []byte, word string) []byte {
	n := int64(len(word))
	var container [5]byte
	binary.BigEndian.PutUint32(container[1:], uint32(n))

	switch {
	case n < 0x80:
		dst = append(dst, container[4])
	case n < 0x4000:
		dst = append(dst, container[3]|0x80, container[4])
	case n < 0x200000:
		dst = append(dst, container[2]|0xc0, container[3], container[4])
	case n < 0x10000000:
		dst = append(dst, container[1]|0xe0, container[2], container[3], container[4])
	default:
		dst = append(dst, 0xf0, container[1], container[2], container[3], container[4])
	}
	return append(dst, word...)
}

// DecodeSentences returns every complete sentence in b and the remainder, which
// starts at the first unfinished sentence so that a caller can prepend it to the
// next chunk. A partial prefix or payload is left in rest rather than failing.
func DecodeSentences(b []byte) (sentences [][]string, rest []byte, err error) {
	var words []string
	sentenceStart := 0
	offset := 0
	for offset < len(b) {
		length, size, ok, err := decodeLength(b[offset:])
		if err != nil {
			return nil, nil, err
		}
		if !ok || offset+size+length > len(b) {
			break
		}
		offset += size
		if length == 0 {
			if len(words) > 0 {
				sentences = append(sentences, words)
				words = nil
			}
			sentenceStart = offset
			continue
		}
		words = append(words, string(b[offset:offset+length]))
		offset += length
	}
	return sentences, b[sentenceStart:], nil
}

func decodeLength(b []byte) (length, size int, ok bool, err error) {
	first := b[0]
	if first == 0xf0 {
		if len(b) < 5 {
			return 0, 0, false, nil
		}
		return int(binary.BigEndian.Uint32(b[1:5])), 5, true, nil
	}

	switch {
	case first < 0x80:
		size = 1
	case first < 0xc0:
		size = 2
	case first < 0xe0:
		size = 3
	case first < 0xf0:
		size = 4
	default:
		return 0, 0, false, fmt.Errorf("reserved control byte 0x%02x", first)
	}
	if len(b) < size {
		return 0, 0, false, nil
	}

	length = int(first & (0x7f >> (size - 1)))
	for _, next := range b[1:size] {
		length = length<<8 | int(next)
	}
	return length, size, true, nil
}
