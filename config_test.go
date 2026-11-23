package routeros

import (
	"crypto/tls"
	"strings"
	"testing"
	"time"
)

func TestNormalizeAppliesDefaults(t *testing.T) {
	cfg, err := Config{Host: "router.example.com"}.normalize()
	if err != nil {
		t.Fatalf("normalize() error = %v", err)
	}

	if cfg.Port != defaultTLSPort {
		t.Errorf("Port = %d, want %d", cfg.Port, defaultTLSPort)
	}
	if cfg.Plaintext {
		t.Error("Plaintext = true, want false")
	}
	if cfg.TLSConfig == nil {
		t.Fatal("TLSConfig = nil, want a TLS configuration")
	}
	if cfg.TLSConfig.ServerName != "router.example.com" {
		t.Errorf("TLSConfig.ServerName = %q, want %q", cfg.TLSConfig.ServerName, "router.example.com")
	}
	if cfg.ConnectTimeout != defaultConnectTimeout {
		t.Errorf("ConnectTimeout = %s, want %s", cfg.ConnectTimeout, defaultConnectTimeout)
	}
	if cfg.CommandTimeout != defaultCommandTimeout {
		t.Errorf("CommandTimeout = %s, want %s", cfg.CommandTimeout, defaultCommandTimeout)
	}
	if cfg.CancelGrace != defaultCancelGrace {
		t.Errorf("CancelGrace = %s, want %s", cfg.CancelGrace, defaultCancelGrace)
	}
	if cfg.MaxWordBytes != defaultMaxWordBytes {
		t.Errorf("MaxWordBytes = %d, want %d", cfg.MaxWordBytes, defaultMaxWordBytes)
	}
	if cfg.MaxSentenceBytes != defaultMaxSentenceBytes {
		t.Errorf("MaxSentenceBytes = %d, want %d", cfg.MaxSentenceBytes, defaultMaxSentenceBytes)
	}
	if cfg.StreamMaxRecords != defaultStreamMaxRecords {
		t.Errorf("StreamMaxRecords = %d, want %d", cfg.StreamMaxRecords, defaultStreamMaxRecords)
	}
	if cfg.StreamMaxBytes != defaultStreamMaxBytes {
		t.Errorf("StreamMaxBytes = %d, want %d", cfg.StreamMaxBytes, defaultStreamMaxBytes)
	}
	if cfg.MaxBufferedStreamBytes != defaultMaxBufferedStreamBytes {
		t.Errorf("MaxBufferedStreamBytes = %d, want %d", cfg.MaxBufferedStreamBytes, defaultMaxBufferedStreamBytes)
	}
	if cfg.MaxOpenStreams != defaultMaxOpenStreams {
		t.Errorf("MaxOpenStreams = %d, want %d", cfg.MaxOpenStreams, defaultMaxOpenStreams)
	}
}

func TestNormalizePlaintextDefaultsToPlaintextPort(t *testing.T) {
	cfg, err := Config{Host: "router.example.com", Plaintext: true}.normalize()
	if err != nil {
		t.Fatalf("normalize() error = %v", err)
	}
	if cfg.Port != defaultPlaintextPort {
		t.Errorf("Port = %d, want %d", cfg.Port, defaultPlaintextPort)
	}
	if cfg.TLSConfig != nil {
		t.Errorf("TLSConfig = %v, want nil for a plaintext config", cfg.TLSConfig)
	}
}

func TestNormalizeKeepsExplicitValues(t *testing.T) {
	cfg, err := Config{
		Host:           "router.example.com",
		Port:           1234,
		ConnectTimeout: 3 * time.Second,
		CommandTimeout: 7 * time.Second,
		MaxWordBytes:   1024,
	}.normalize()
	if err != nil {
		t.Fatalf("normalize() error = %v", err)
	}
	if cfg.Port != 1234 {
		t.Errorf("Port = %d, want 1234", cfg.Port)
	}
	if cfg.ConnectTimeout != 3*time.Second {
		t.Errorf("ConnectTimeout = %s, want 3s", cfg.ConnectTimeout)
	}
	if cfg.CommandTimeout != 7*time.Second {
		t.Errorf("CommandTimeout = %s, want 7s", cfg.CommandTimeout)
	}
	if cfg.MaxWordBytes != 1024 {
		t.Errorf("MaxWordBytes = %d, want 1024", cfg.MaxWordBytes)
	}
}

func TestNormalizeClonesTLSConfig(t *testing.T) {
	caller := &tls.Config{MinVersion: tls.VersionTLS12}
	cfg, err := Config{Host: "router.example.com", TLSConfig: caller}.normalize()
	if err != nil {
		t.Fatalf("normalize() error = %v", err)
	}

	if cfg.TLSConfig == caller {
		t.Fatal("normalize() reused the caller's TLSConfig, want a clone")
	}
	if caller.ServerName != "" {
		t.Errorf("caller TLSConfig.ServerName = %q, want it untouched", caller.ServerName)
	}
	if cfg.TLSConfig.ServerName != "router.example.com" {
		t.Errorf("TLSConfig.ServerName = %q, want %q", cfg.TLSConfig.ServerName, "router.example.com")
	}
	if cfg.TLSConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("TLSConfig.MinVersion = %d, want %d", cfg.TLSConfig.MinVersion, tls.VersionTLS12)
	}
}

func TestNormalizeRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"empty host", Config{}, "host is required"},
		{"port below range", Config{Host: "router.example.com", Port: -1}, "outside 0..65535"},
		{"port above range", Config{Host: "router.example.com", Port: 65536}, "outside 0..65535"},
		{"negative connect timeout", Config{Host: "router.example.com", ConnectTimeout: -time.Second}, "must not be negative"},
		{"negative word limit", Config{Host: "router.example.com", MaxWordBytes: -1}, "must not be negative"},
		{"stream bytes above connection budget", Config{
			Host:                   "router.example.com",
			StreamMaxBytes:         defaultMaxBufferedStreamBytes + 1,
			MaxBufferedStreamBytes: defaultMaxBufferedStreamBytes,
		}, "exceeds max buffered stream bytes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.cfg.normalize()
			if err == nil {
				t.Fatal("normalize() error = nil, want a validation error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("normalize() error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}
