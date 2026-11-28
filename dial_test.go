package routeros

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/filydottech/ros-go/internal/testkit"
	"github.com/filydottech/ros-go/internal/transport"
	"github.com/filydottech/ros-go/internal/wire"
)

const dialTestPassword = "not-a-real-password"

func testCertificates(t *testing.T) (*x509.CertPool, tls.Certificate) {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ros-go test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(caCert)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "router.test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"router.test"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server certificate: %v", err)
	}
	return roots, tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// pipeTransport replaces the dialler with an in-memory connection and runs
// script on the server end. The script must return on its own.
func pipeTransport(t *testing.T, serverConfig *tls.Config, script func(net.Conn)) *string {
	t.Helper()

	clientConn, serverConn := testkit.Pipe()
	addressed := new(string)

	oldOpen := openTransport
	openTransport = func(ctx context.Context, opts transport.Options) (*transport.Conn, error) {
		*addressed = opts.Address
		opts.Dial = func(context.Context, string) (net.Conn, error) {
			return clientConn, nil
		}
		return transport.Open(ctx, opts)
	}
	t.Cleanup(func() { openTransport = oldOpen })

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer serverConn.Close()
		conn := net.Conn(serverConn)
		if serverConfig != nil {
			secure := tls.Server(serverConn, serverConfig)
			if err := secure.Handshake(); err != nil {
				return
			}
			conn = secure
		}
		script(conn)
	}()
	t.Cleanup(func() {
		_ = clientConn.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("fake server did not finish")
		}
	})
	return addressed
}

func serveLogin(t *testing.T, conn net.Conn) []string {
	t.Helper()

	decoder := wire.NewDecoder(wire.DefaultLimits())
	buf := make([]byte, 4096)
	for {
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("server read deadline: %v", err)
		}
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("server read: %v", err)
		}
		sentences, err := decoder.Feed(buf[:n])
		if err != nil {
			t.Fatalf("server decode: %v", err)
		}
		if len(sentences) == 0 {
			continue
		}
		if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("server write deadline: %v", err)
		}
		if _, err := conn.Write(testkit.EncodeSentence([]string{"!done"})); err != nil {
			t.Fatalf("server write: %v", err)
		}
		return sentences[0]
	}
}

// drain reads until the peer closes. It lets a TLS Close consume close_notify
// promptly instead of waiting for its five second send timeout.
func drain(conn net.Conn) {
	buf := make([]byte, 512)
	for {
		if _, err := conn.Read(buf); err != nil {
			return
		}
	}
}

func TestDialTLSWithVerifiedCA(t *testing.T) {
	roots, certificate := testCertificates(t)
	loginWords := make(chan []string, 1)

	addressed := pipeTransport(t, &tls.Config{Certificates: []tls.Certificate{certificate}}, func(conn net.Conn) {
		loginWords <- serveLogin(t, conn)
		drain(conn)
	})

	client, err := Dial(context.Background(), Config{
		Host:     "router.test",
		Username: "alice",
		Password: []byte(dialTestPassword),
		TLSConfig: &tls.Config{
			RootCAs: roots,
		},
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if *addressed != "router.test:8729" {
		t.Errorf("dial address = %q, want router.test:8729", *addressed)
	}
	if got := client.State(); got != Ready {
		t.Errorf("State() = %s, want %s", got, Ready)
	}
	if err := client.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
	select {
	case <-client.Done():
		t.Fatalf("Done() closed on a healthy connection: %v", client.Err())
	default:
	}

	words := <-loginWords
	if len(words) != 3 || words[0] != "/login" || words[1] != "=name=alice" {
		t.Errorf("login sentence = %q, want /login with =name=alice", words)
	}
	if client.cfg.Password != nil {
		t.Error("client retained the password after login")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := client.State(); got != Closed {
		t.Errorf("State() after Close() = %s, want %s", got, Closed)
	}
	var closed *ClosedError
	if !errors.As(client.Err(), &closed) {
		t.Errorf("Err() = %v, want *ClosedError", client.Err())
	}
}

func TestDialTLSRejectsWrongHostname(t *testing.T) {
	roots, certificate := testCertificates(t)
	pipeTransport(t, &tls.Config{Certificates: []tls.Certificate{certificate}}, func(net.Conn) {})

	_, err := Dial(context.Background(), Config{
		Host:           "other.test",
		Username:       "alice",
		Password:       []byte(dialTestPassword),
		ConnectTimeout: 500 * time.Millisecond,
		TLSConfig:      &tls.Config{RootCAs: roots},
	})
	if err == nil {
		t.Fatal("Dial() error = nil, want a hostname verification failure")
	}
	var connection *ConnectionError
	if !errors.As(err, &connection) {
		t.Fatalf("Dial() error = %v, want *ConnectionError", err)
	}
	if !strings.Contains(err.Error(), "certificate") {
		t.Errorf("Dial() error = %q, want the certificate failure", err)
	}
	if strings.Contains(err.Error(), dialTestPassword) {
		t.Errorf("Dial() error = %q, must not contain the password", err)
	}
}

func TestDialTLSInsecureSkipVerifyOptIn(t *testing.T) {
	roots, certificate := testCertificates(t)
	pipeTransport(t, &tls.Config{Certificates: []tls.Certificate{certificate}}, func(conn net.Conn) {
		serveLogin(t, conn)
		drain(conn)
	})

	client, err := Dial(context.Background(), Config{
		Host:     "other.test",
		Username: "alice",
		Password: []byte(dialTestPassword),
		TLSConfig: &tls.Config{
			RootCAs:            roots,
			InsecureSkipVerify: true,
		},
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	_ = client.Close()
}

func TestDialPlaintextUsesPlaintextPort(t *testing.T) {
	addressed := pipeTransport(t, nil, func(conn net.Conn) {
		serveLogin(t, conn)
		drain(conn)
	})

	client, err := Dial(context.Background(), Config{
		Host:      "router.test",
		Plaintext: true,
		Username:  "alice",
		Password:  []byte(dialTestPassword),
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	_ = client.Close()

	if *addressed != "router.test:8728" {
		t.Errorf("dial address = %q, want router.test:8728", *addressed)
	}
}

func TestDialClearsOwnedPasswordAfterTransportFailure(t *testing.T) {
	password := []byte(dialTestPassword)
	caller := append([]byte(nil), password...)
	var owned []byte

	oldOpen := openTransport
	openTransport = func(ctx context.Context, opts transport.Options) (*transport.Conn, error) {
		owned = opts.Password
		return nil, errors.New("test transport failure")
	}
	t.Cleanup(func() { openTransport = oldOpen })

	_, err := Dial(context.Background(), Config{
		Host:      "router.test",
		Plaintext: true,
		Username:  "alice",
		Password:  password,
	})
	if err == nil {
		t.Fatal("Dial() error = nil, want a transport failure")
	}
	if !bytes.Equal(password, caller) {
		t.Fatal("Dial() modified the caller password")
	}
	for _, value := range owned {
		if value != 0 {
			t.Fatal("Dial() did not clear its password copy after a transport failure")
		}
	}
}

func TestDialLoginTrap(t *testing.T) {
	pipeTransport(t, nil, func(conn net.Conn) {
		decoder := wire.NewDecoder(wire.DefaultLimits())
		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("server read: %v", err)
		}
		if _, err := decoder.Feed(buf[:n]); err != nil {
			t.Fatalf("server decode: %v", err)
		}
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write(testkit.EncodeSentence([]string{"!trap", "=message=cannot log in"})); err != nil {
			t.Fatalf("server write: %v", err)
		}
	})

	password := []byte(dialTestPassword)
	caller := append([]byte(nil), password...)
	_, err := Dial(context.Background(), Config{
		Host:      "router.test",
		Plaintext: true,
		Username:  "alice",
		Password:  password,
	})
	var login *LoginError
	if !errors.As(err, &login) {
		t.Fatalf("Dial() error = %v, want *LoginError", err)
	}
	if login.Message != "cannot log in" {
		t.Errorf("Message = %q, want %q", login.Message, "cannot log in")
	}
	if strings.Contains(err.Error(), dialTestPassword) {
		t.Errorf("Dial() error = %q, must not contain the password", err)
	}
	if !bytes.Equal(password, caller) {
		t.Error("Dial() modified the caller password after a login failure")
	}
}

func TestDialPeerCloseDuringLogin(t *testing.T) {
	pipeTransport(t, nil, func(conn net.Conn) {
		buf := make([]byte, 4096)
		_, _ = conn.Read(buf)
	})

	_, err := Dial(context.Background(), Config{
		Host:      "router.test",
		Plaintext: true,
		Username:  "alice",
		Password:  []byte(dialTestPassword),
	})
	var connection *ConnectionError
	if !errors.As(err, &connection) {
		t.Fatalf("Dial() error = %v, want *ConnectionError", err)
	}
	if strings.Contains(err.Error(), dialTestPassword) {
		t.Errorf("Dial() error = %q, must not contain the password", err)
	}
}
