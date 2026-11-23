package wire

// Decoder turns an incremental byte stream into sentences. It accepts every
// possible TCP split: a prefix, a payload, and a sentence terminator may each
// arrive in pieces, and a UTF-8 sequence may be split between chunks.
//
// A decoding failure is sticky. Once Feed returns an error, every later call
// returns that same error, because the stream position is no longer known.
type Decoder struct {
	limits Limits

	buf          []byte
	word         []byte
	wordLength   int
	words        []string
	sentenceSize int64
	err          error
}

// NewDecoder returns a decoder that enforces limits. A zero limit takes its
// default.
func NewDecoder(limits Limits) *Decoder {
	return &Decoder{limits: limits.withDefaults()}
}

// Feed adds chunk and returns every sentence completed by it, in wire order.
// A sentence is a list of word payloads; the terminator is not returned.
func (d *Decoder) Feed(chunk []byte) ([][]string, error) {
	if d.err != nil {
		return nil, d.err
	}
	d.buf = append(d.buf, chunk...)

	var sentences [][]string
	offset := 0
	for {
		if d.wordLength == 0 {
			prefix, ok, err := DecodeLength(d.buf[offset:])
			if err != nil {
				d.buf = nil
				return nil, d.fail(err)
			}
			if !ok {
				break
			}
			offset += prefix.Size
			if err := d.beginWord(prefix.Length); err != nil {
				d.buf = nil
				return nil, d.fail(err)
			}
			if d.wordLength == 0 {
				if len(d.words) > 0 {
					sentences = append(sentences, d.words)
					d.words = nil
					d.sentenceSize = 0
				}
				continue
			}
		}

		available := len(d.buf) - offset
		if available == 0 {
			break
		}
		if available > d.wordLength {
			available = d.wordLength
		}
		d.word = append(d.word, d.buf[offset:offset+available]...)
		offset += available
		d.wordLength -= available
		if d.wordLength == 0 {
			d.words = append(d.words, string(d.word))
			d.word = d.word[:0]
		}
	}

	d.buf = append(d.buf[:0], d.buf[offset:]...)
	if len(d.buf) == 0 {
		d.buf = nil
	}
	return sentences, nil
}

// beginWord validates one non-empty word against the limits and its payload
// length before any payload buffer is allocated.
func (d *Decoder) beginWord(length uint32) error {
	if length == 0 {
		d.wordLength = 0
		return nil
	}
	if int64(length) > int64(d.limits.MaxWordBytes) {
		return &LimitError{Scope: "word", Limit: int64(d.limits.MaxWordBytes), Observed: int64(length)}
	}
	d.sentenceSize += int64(length)
	if d.sentenceSize > int64(d.limits.MaxSentenceBytes) {
		return &LimitError{Scope: "sentence", Limit: int64(d.limits.MaxSentenceBytes), Observed: d.sentenceSize}
	}
	if len(d.words)+1 > d.limits.MaxWordsPerSentence {
		return &LimitError{
			Scope:    "words per sentence",
			Limit:    int64(d.limits.MaxWordsPerSentence),
			Observed: int64(len(d.words) + 1),
		}
	}
	d.wordLength = int(length)
	return nil
}

func (d *Decoder) fail(err error) error {
	d.err = err
	d.words = nil
	d.word = nil
	return err
}
