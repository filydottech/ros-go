package routeros

import (
	"testing"

	"github.com/filydottech/ros-go/internal/wire"
)

func TestCodecDefaultsMatchConfigDefaults(t *testing.T) {
	cfg, err := Config{Host: "router.example.com"}.normalize()
	if err != nil {
		t.Fatalf("normalize() error = %v", err)
	}
	limits := wire.DefaultLimits()

	if cfg.MaxWordBytes != limits.MaxWordBytes {
		t.Errorf("config MaxWordBytes = %d, codec default = %d", cfg.MaxWordBytes, limits.MaxWordBytes)
	}
	if cfg.MaxSentenceBytes != limits.MaxSentenceBytes {
		t.Errorf("config MaxSentenceBytes = %d, codec default = %d", cfg.MaxSentenceBytes, limits.MaxSentenceBytes)
	}
	if cfg.MaxWordsPerSentence != limits.MaxWordsPerSentence {
		t.Errorf("config MaxWordsPerSentence = %d, codec default = %d", cfg.MaxWordsPerSentence, limits.MaxWordsPerSentence)
	}
}
