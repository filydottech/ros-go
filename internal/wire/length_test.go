package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestAppendLengthWritesEveryTier(t *testing.T) {
	cases := []struct {
		value uint32
		want  []byte
	}{
		{0, []byte{0x00}},
		{0x01, []byte{0x01}},
		{0x7f, []byte{0x7f}},
		{0x80, []byte{0x80, 0x80}},
		{0x3fff, []byte{0xbf, 0xff}},
		{0x4000, []byte{0xc0, 0x40, 0x00}},
		{0x1fffff, []byte{0xdf, 0xff, 0xff}},
		{0x200000, []byte{0xe0, 0x20, 0x00, 0x00}},
		{0x0fffffff, []byte{0xef, 0xff, 0xff, 0xff}},
		{0x10000000, []byte{0xf0, 0x10, 0x00, 0x00, 0x00}},
		{MaxWireLength, []byte{0xf0, 0xff, 0xff, 0xff, 0xff}},
	}

	for _, tc := range cases {
		got := AppendLength(nil, tc.value)
		if !bytes.Equal(got, tc.want) {
			t.Errorf("AppendLength(%#x) = % x, want % x", tc.value, got, tc.want)
			continue
		}
		prefix, ok, err := DecodeLength(got)
		if err != nil || !ok {
			t.Errorf("DecodeLength(% x) = ok %t, err %v; want ok true, err nil", got, ok, err)
			continue
		}
		if prefix.Length != tc.value || prefix.Size != len(tc.want) {
			t.Errorf("DecodeLength(% x) = %+v, want length %#x and size %d", got, prefix, tc.value, len(tc.want))
		}
	}
}

func TestDecodeLengthReportsIncompletePrefixes(t *testing.T) {
	encoded := [][]byte{
		AppendLength(nil, 0x80),
		AppendLength(nil, 0x4000),
		AppendLength(nil, 0x200000),
		AppendLength(nil, 0x10000000),
	}
	for _, prefix := range encoded {
		for cut := 0; cut < len(prefix); cut++ {
			got, ok, err := DecodeLength(prefix[:cut])
			if err != nil {
				t.Fatalf("DecodeLength(% x) error = %v, want nil", prefix[:cut], err)
			}
			if ok {
				t.Fatalf("DecodeLength(% x) ok = true, want false for a partial prefix", prefix[:cut])
			}
			if got != (Prefix{}) {
				t.Fatalf("DecodeLength(% x) = %+v, want the zero Prefix", prefix[:cut], got)
			}
		}
	}
}

func TestDecodeLengthRejectsReservedControlBytes(t *testing.T) {
	for first := 0xf1; first <= 0xff; first++ {
		_, ok, err := DecodeLength([]byte{byte(first), 0x00, 0x00, 0x00, 0x00})
		if ok {
			t.Fatalf("DecodeLength(0x%02x...) ok = true, want false", first)
		}
		var protocolErr *ProtocolError
		if !errors.As(err, &protocolErr) {
			t.Fatalf("DecodeLength(0x%02x...) error = %v, want *ProtocolError", first, err)
		}
	}
}
