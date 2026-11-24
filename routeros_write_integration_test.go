//go:build integration && integration_write

package routeros

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestRouterOSHotspotUserCRUD(t *testing.T) {
	port, err := strconv.Atoi(os.Getenv("ROS_PORT"))
	if err != nil {
		t.Fatal("ROS_PORT must be set to the test router port")
	}
	client, err := Dial(context.Background(), Config{
		Host:      os.Getenv("ROS_HOST"),
		Port:      port,
		Username:  os.Getenv("ROS_USER"),
		Password:  []byte(os.Getenv("ROS_PASSWORD")),
		Plaintext: true,
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	name := fmt.Sprintf("ros-go-test-hotspot-%d", time.Now().UnixNano())
	password := fmt.Sprintf("test-%d", time.Now().UnixNano())
	const createdComment = "ros-go integration \u00e9"
	const updatedComment = "ros-go updated \u00e9"
	var ret string
	t.Cleanup(func() {
		if ret == "" {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := client.Do(ctx, "/ip/hotspot/user/remove", Request{Attributes: []Attribute{{Name: "numbers", Value: ret}}}); err != nil {
			t.Errorf("cleanup hotspot user error = %v", err)
		}
	})

	added, err := client.Do(context.Background(), "/ip/hotspot/user/add", Request{Attributes: []Attribute{
		{Name: "name", Value: name},
		{Name: "password", Value: password},
		{Name: "profile", Value: "default"},
		{Name: "comment", Value: createdComment},
		{Name: "disabled", Value: "yes"},
	}})
	if err != nil {
		t.Fatalf("Do(hotspot user add) error = %v", err)
	}
	ret = added.Done["ret"]
	if ret == "" {
		t.Fatal("Do(hotspot user add) returned no ret")
	}

	found, err := client.Do(context.Background(), "/ip/hotspot/user/print", Request{
		Proplist: []string{"name", "profile", "comment", "disabled"},
		Query:    Equal("name", name).Words(),
	})
	if err != nil {
		t.Fatalf("Do(hotspot user print) error = %v", err)
	}
	if len(found.Records) != 1 {
		t.Fatalf("hotspot user records = %d, want 1", len(found.Records))
	}
	if got := found.Records[0]["comment"]; got != createdComment {
		t.Errorf("created hotspot user comment = %q, want %q", got, createdComment)
	}

	if _, err := client.Do(context.Background(), "/ip/hotspot/user/set", Request{Attributes: []Attribute{
		{Name: "numbers", Value: ret},
		{Name: "comment", Value: updatedComment},
	}}); err != nil {
		t.Fatalf("Do(hotspot user set) error = %v", err)
	}

	updated, err := client.Do(context.Background(), "/ip/hotspot/user/print", Request{
		Proplist: []string{"comment"},
		Query: And(
			Equal("name", name),
			Equal("comment", updatedComment),
		).Words(),
	})
	if err != nil {
		t.Fatalf("Do(hotspot user print after set) error = %v", err)
	}
	if len(updated.Records) != 1 {
		t.Fatalf("updated hotspot user records = %d, want 1", len(updated.Records))
	}

	if _, err := client.Do(context.Background(), "/ip/hotspot/user/remove", Request{Attributes: []Attribute{{Name: "numbers", Value: ret}}}); err != nil {
		t.Fatalf("Do(hotspot user remove) error = %v", err)
	}
	ret = ""

	removed, err := client.Do(context.Background(), "/ip/hotspot/user/print", Request{
		Proplist: []string{"name"},
		Query:    Equal("name", name).Words(),
	})
	if err != nil {
		t.Fatalf("Do(hotspot user print after remove) error = %v", err)
	}
	if len(removed.Records) != 0 {
		t.Errorf("removed hotspot user records = %d, want 0", len(removed.Records))
	}
}
