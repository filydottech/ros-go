package routeros

import (
	"crypto/tls"
	"errors"
	"fmt"
	"time"
)

const (
	defaultTLSPort       = 8729
	defaultPlaintextPort = 8728

	defaultConnectTimeout = 10 * time.Second
	defaultCommandTimeout = 10 * time.Second
	defaultCancelGrace    = 2 * time.Second

	defaultMaxWordBytes           = 32 << 20
	defaultMaxSentenceBytes       = 64 << 20
	defaultMaxWordsPerSentence    = 4096
	defaultStreamMaxRecords       = 1024
	defaultStreamMaxBytes         = 8 << 20
	defaultMaxBufferedStreamBytes = 64 << 20
	defaultMaxOpenStreams         = 16
)

// Config holds every setting Dial and Reconnect use.
//
// The zero value is usable once Host is set: TLS is enabled, the port is 8729,
// and every timeout and limit takes its documented default. Plaintext is never
// the default and must be opted into; a plaintext config defaults to port 8728.
//
// Dial validates Config before opening a socket, so an invalid config never
// reaches the network.
type Config struct {
	// Host is the router hostname or IP address, without a port. Required.
	Host string
	// Port is the API port. Zero selects 8729 with TLS and 8728 in plaintext.
	Port int

	// Username and Password authenticate the API user. Password is bytes so
	// that the client does not keep a second copy as a string.
	Username string
	Password []byte

	// TLSConfig is the TLS client configuration. It is cloned before use and
	// ServerName is filled from Host when it is empty. Ignored in plaintext.
	TLSConfig *tls.Config
	// Plaintext sends credentials without TLS and is never the default.
	Plaintext bool

	// ConnectTimeout bounds dialling and login. Zero selects 10 seconds.
	ConnectTimeout time.Duration
	// CommandTimeout bounds one Do call end to end. Zero selects 10 seconds.
	CommandTimeout time.Duration
	// CancelGrace bounds remote cancellation cleanup after a context ends.
	// Zero selects 2 seconds.
	CancelGrace time.Duration

	// MaxWordBytes is the largest accepted or sent word. Zero selects 32 MiB.
	// It is checked before a payload is allocated.
	MaxWordBytes int
	// MaxSentenceBytes bounds the total size of one sentence. Zero selects
	// 64 MiB.
	MaxSentenceBytes int
	// MaxWordsPerSentence bounds a sentence made of many small words. Zero
	// selects 4096.
	MaxWordsPerSentence int

	// StreamMaxRecords and StreamMaxBytes bound one stream's queue. Zero
	// selects 1024 records and 8 MiB.
	StreamMaxRecords int
	StreamMaxBytes   int
	// MaxBufferedStreamBytes bounds every queued stream record on the
	// connection together. It must be at least StreamMaxBytes. Zero selects
	// 64 MiB.
	MaxBufferedStreamBytes int
	// MaxOpenStreams bounds concurrent streams on the connection. Zero
	// selects 16.
	MaxOpenStreams int
}

// normalize returns a copy of c with defaults applied and every field
// validated. The receiver is left untouched, including the caller's TLSConfig.
func (c Config) normalize() (Config, error) {
	if c.Host == "" {
		return Config{}, errors.New("host is required")
	}
	if c.Port < 0 || c.Port > 65535 {
		return Config{}, fmt.Errorf("port %d is outside 0..65535", c.Port)
	}
	if c.Port == 0 {
		c.Port = defaultTLSPort
		if c.Plaintext {
			c.Port = defaultPlaintextPort
		}
	}

	var err error
	if c.ConnectTimeout, err = defaultDuration("connect timeout", c.ConnectTimeout, defaultConnectTimeout); err != nil {
		return Config{}, err
	}
	if c.CommandTimeout, err = defaultDuration("command timeout", c.CommandTimeout, defaultCommandTimeout); err != nil {
		return Config{}, err
	}
	if c.CancelGrace, err = defaultDuration("cancel grace", c.CancelGrace, defaultCancelGrace); err != nil {
		return Config{}, err
	}

	limits := []struct {
		name     string
		value    *int
		fallback int
	}{
		{"max word bytes", &c.MaxWordBytes, defaultMaxWordBytes},
		{"max sentence bytes", &c.MaxSentenceBytes, defaultMaxSentenceBytes},
		{"max words per sentence", &c.MaxWordsPerSentence, defaultMaxWordsPerSentence},
		{"stream max records", &c.StreamMaxRecords, defaultStreamMaxRecords},
		{"stream max bytes", &c.StreamMaxBytes, defaultStreamMaxBytes},
		{"max buffered stream bytes", &c.MaxBufferedStreamBytes, defaultMaxBufferedStreamBytes},
		{"max open streams", &c.MaxOpenStreams, defaultMaxOpenStreams},
	}
	for _, limit := range limits {
		if *limit.value, err = defaultLimit(limit.name, *limit.value, limit.fallback); err != nil {
			return Config{}, err
		}
	}
	if c.StreamMaxBytes > c.MaxBufferedStreamBytes {
		return Config{}, fmt.Errorf("stream max bytes %d exceeds max buffered stream bytes %d", c.StreamMaxBytes, c.MaxBufferedStreamBytes)
	}

	if !c.Plaintext {
		var tlsConfig *tls.Config
		if c.TLSConfig == nil {
			tlsConfig = &tls.Config{}
		} else {
			tlsConfig = c.TLSConfig.Clone()
		}
		if tlsConfig.ServerName == "" {
			tlsConfig.ServerName = c.Host
		}
		c.TLSConfig = tlsConfig
	}

	return c, nil
}

func defaultDuration(name string, value, fallback time.Duration) (time.Duration, error) {
	if value < 0 {
		return 0, fmt.Errorf("%s %s must not be negative", name, value)
	}
	if value == 0 {
		return fallback, nil
	}
	return value, nil
}

func defaultLimit(name string, value, fallback int) (int, error) {
	if value < 0 {
		return 0, fmt.Errorf("%s %d must not be negative", name, value)
	}
	if value == 0 {
		return fallback, nil
	}
	return value, nil
}
