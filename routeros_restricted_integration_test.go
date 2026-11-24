//go:build integration && integration_restricted

package routeros

import (
	"context"
	"os"
	"strconv"
	"testing"
)

func TestRouterOSRestrictedAPIUserReadsIdentity(t *testing.T) {
	port, err := strconv.Atoi(os.Getenv("ROS_PORT"))
	if err != nil {
		t.Fatal("ROS_PORT must be set to the test router port")
	}
	username := os.Getenv("ROS_RESTRICTED_USER")
	if username == "" {
		t.Fatal("ROS_RESTRICTED_USER must be set to the restricted API user")
	}
	client, err := Dial(context.Background(), Config{
		Host:      os.Getenv("ROS_HOST"),
		Port:      port,
		Username:  username,
		Password:  []byte(os.Getenv("ROS_RESTRICTED_PASSWORD")),
		Plaintext: true,
	})
	if err != nil {
		t.Fatalf("Dial(restricted user) error = %v", err)
	}
	defer client.Close()

	result, err := client.Do(context.Background(), "/system/identity/print", Request{})
	if err != nil {
		t.Fatalf("Do(identity) as restricted user error = %v", err)
	}
	if len(result.Records) != 1 {
		t.Errorf("identity records = %d, want 1", len(result.Records))
	}
}
