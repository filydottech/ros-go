package wire

import (
	"encoding/binary"
	"fmt"
)

// MaxWireLength is the largest length a prefix can carry: the four-byte
// big-endian form behind a 0xf0 control byte.
const MaxWireLength = 0xffffffff

// Prefix is one decoded length word: the value it carries and how many bytes
// it occupied.
type Prefix struct {
	Length uint32
	Size   int
}

// AppendLength appends the length prefix for n to dst. The five tiers are the
// ones RouterOS defines: one, two, three, four, and 0xf0 plus a big-endian
// uint32.
func AppendLength(dst []byte, n uint32) []byte {
	switch {
	case n < 0x80:
		return append(dst, byte(n))
	case n < 0x4000:
		return append(dst, byte(n>>8)|0x80, byte(n))
	case n < 0x200000:
		return append(dst, byte(n>>16)|0xc0, byte(n>>8), byte(n))
	case n < 0x10000000:
		return append(dst, byte(n>>24)|0xe0, byte(n>>16), byte(n>>8), byte(n))
	default:
		return append(dst, 0xf0, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
}

// DecodeLength reads the prefix at the start of b. It reports ok=false when b
// holds only part of a prefix, which is not an error: the caller feeds more
// bytes. A byte in the reserved 0xf1..0xff range is a *ProtocolError.
func DecodeLength(b []byte) (Prefix, bool, error) {
	if len(b) == 0 {
		return Prefix{}, false, nil
	}
	first := b[0]
	switch {
	case first < 0x80:
		return Prefix{Length: uint32(first), Size: 1}, true, nil
	case first&0xc0 == 0x80:
		if len(b) < 2 {
			return Prefix{}, false, nil
		}
		return Prefix{Length: uint32(first&0x3f)<<8 | uint32(b[1]), Size: 2}, true, nil
	case first&0xe0 == 0xc0:
		if len(b) < 3 {
			return Prefix{}, false, nil
		}
		length := uint32(first&0x1f)<<16 | uint32(b[1])<<8 | uint32(b[2])
		return Prefix{Length: length, Size: 3}, true, nil
	case first&0xf0 == 0xe0:
		if len(b) < 4 {
			return Prefix{}, false, nil
		}
		length := uint32(first&0x0f)<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
		return Prefix{Length: length, Size: 4}, true, nil
	case first == 0xf0:
		if len(b) < 5 {
			return Prefix{}, false, nil
		}
		return Prefix{Length: binary.BigEndian.Uint32(b[1:5]), Size: 5}, true, nil
	default:
		return Prefix{}, false, &ProtocolError{Detail: fmt.Sprintf("reserved control byte 0x%02x", first)}
	}
}
