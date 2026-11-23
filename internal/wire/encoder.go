package wire

// Encoder writes RouterOS sentences. Words are UTF-8 payloads: the API expects
// UTF-8 for the values a caller sends, and every word carries its byte length,
// not its rune count.
type Encoder struct {
	limits Limits
}

// NewEncoder returns an encoder that enforces limits. A zero limit takes its
// default.
func NewEncoder(limits Limits) *Encoder {
	return &Encoder{limits: limits.withDefaults()}
}

// Sentence encodes one complete sentence: every word as a length-prefixed
// payload, then the zero-length terminator.
//
// An empty word list, an empty word, a word larger than MaxWordBytes, more
// words than MaxWordsPerSentence, and a payload sum above MaxSentenceBytes are
// rejected; the encoder never writes a partial sentence.
func (e *Encoder) Sentence(words []string) ([]byte, error) {
	return e.AppendSentence(nil, words)
}

// AppendSentence appends one complete sentence to dst. On error dst is
// returned unchanged.
func (e *Encoder) AppendSentence(dst []byte, words []string) ([]byte, error) {
	if len(words) == 0 {
		return dst, &ProtocolError{Detail: "sentence has no words"}
	}
	if len(words) > e.limits.MaxWordsPerSentence {
		return dst, &LimitError{
			Scope:    "words per sentence",
			Limit:    int64(e.limits.MaxWordsPerSentence),
			Observed: int64(len(words)),
		}
	}
	start := len(dst)
	total := int64(0)
	for _, word := range words {
		if word == "" {
			return dst[:start], &ProtocolError{Detail: "sentence contains an empty word"}
		}
		if int64(len(word)) > int64(e.limits.MaxWordBytes) {
			return dst[:start], &LimitError{
				Scope:    "word",
				Limit:    int64(e.limits.MaxWordBytes),
				Observed: int64(len(word)),
			}
		}
		total += int64(len(word))
		if total > int64(e.limits.MaxSentenceBytes) {
			return dst[:start], &LimitError{
				Scope:    "sentence",
				Limit:    int64(e.limits.MaxSentenceBytes),
				Observed: total,
			}
		}
		dst = AppendLength(dst, uint32(len(word)))
		dst = append(dst, word...)
	}
	return append(dst, 0), nil
}
