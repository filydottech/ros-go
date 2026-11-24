//go:build integration && integration_tls

package routeros

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"strconv"
	"testing"
)

func TestRouterOSTLSWithVerifiedCA(t *testing.T) {
	host := os.Getenv("ROS_TLS_HOST")
	if host == "" {
		t.Fatal("ROS_TLS_HOST must be set to the TLS router host")
	}
	port, err := strconv.Atoi(os.Getenv("ROS_TLS_PORT"))
	if err != nil {
		t.Fatal("ROS_TLS_PORT must be set to the TLS router port")
	}
	caFile := os.Getenv("ROS_TLS_CA_FILE")
	if caFile == "" {
		t.Fatal("ROS_TLS_CA_FILE must be set to a trusted CA PEM file")
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatalf("ReadFile(TLS CA) error = %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("TLS CA file contains no PEM certificate")
	}

	client, err := Dial(context.Background(), Config{
		Host:     host,
		Port:     port,
		Username: os.Getenv("ROS_USER"),
		Password: []byte(os.Getenv("ROS_PASSWORD")),
		TLSConfig: &tls.Config{
			RootCAs: roots,
		},
	})
	if err != nil {
		t.Fatalf("Dial(TLS) error = %v", err)
	}
	defer client.Close()

	if _, err := client.Do(context.Background(), "/system/identity/print", Request{}); err != nil {
		t.Fatalf("Do(identity) over verified TLS error = %v", err)
	}
}
