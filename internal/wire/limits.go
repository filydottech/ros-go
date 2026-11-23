package wire

// Limits bounds one word, one sentence, and the number of words a sentence may
// hold. A zero field takes its default, which matches the public Config
// defaults; repo/ros-go/config_test.go pins that they stay equal.
type Limits struct {
	// MaxWordBytes bounds one word payload, checked before allocation.
	MaxWordBytes int
	// MaxSentenceBytes bounds the sum of one sentence's word payloads.
	MaxSentenceBytes int
	// MaxWordsPerSentence bounds a sentence made of many small words.
	MaxWordsPerSentence int
}

// DefaultLimits returns the codec limits behind the public Config defaults.
func DefaultLimits() Limits {
	return Limits{
		MaxWordBytes:        32 << 20,
		MaxSentenceBytes:    64 << 20,
		MaxWordsPerSentence: 4096,
	}
}

func (l Limits) withDefaults() Limits {
	defaults := DefaultLimits()
	if l.MaxWordBytes == 0 {
		l.MaxWordBytes = defaults.MaxWordBytes
	}
	if l.MaxSentenceBytes == 0 {
		l.MaxSentenceBytes = defaults.MaxSentenceBytes
	}
	if l.MaxWordsPerSentence == 0 {
		l.MaxWordsPerSentence = defaults.MaxWordsPerSentence
	}
	return l
}
