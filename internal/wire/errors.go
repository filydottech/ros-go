package wire

import "fmt"

// ProtocolError reports a framing or reply failure the codec cannot continue
// past. The transport converts it to the public *routeros.ProtocolError.
type ProtocolError struct {
	Detail string
}

func (e *ProtocolError) Error() string { return "wire: " + e.Detail }

// LimitError reports a word, sentence, or word-count limit that was exceeded.
// Scope names the limit in words, for example "word" or "words per sentence",
// so the transport can carry it into a *routeros.ResourceLimitError.
type LimitError struct {
	Scope    string
	Limit    int64
	Observed int64
}

func (e *LimitError) Error() string {
	return fmt.Sprintf("wire: %s exceeds limit %d (observed %d)", e.Scope, e.Limit, e.Observed)
}

var (
	_ error = (*ProtocolError)(nil)
	_ error = (*LimitError)(nil)
)
