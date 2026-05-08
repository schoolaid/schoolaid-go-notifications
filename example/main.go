// Example: produce a note.created event from a Go service.
//
//	go run ./example
//
// Requires KAFKA_ENABLED=true plus KAFKA_BROKERS set to a reachable broker.
package main

import (
	"context"
	"log"
	"time"

	notifications "github.com/schoolaid/schoolaid-go-notifications"
)

func main() {
	cfg := notifications.ConfigFromEnv()
	cfg.Enabled = true

	client, err := notifications.NewClient(cfg)
	if err != nil {
		log.Fatalf("notifications client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.PublishNoteCreated(ctx, notifications.NoteCreated{
		Note: notifications.Note{
			NoteID:    1001,
			SchoolID:  42,
			Title:     "Example note",
			Subject:   "Example note",
			Content:   "<p>hello from go</p>",
			Important: false,
			Channels:  []string{"push", "email"},
			PushTitle: map[string]string{
				"es": "Nueva circular: Example note",
				"en": "New message: Example note",
			},
		},
		Recipient: notifications.Recipient{
			StudentID: 7,
			Users: []notifications.PushUser{
				{UserID: 1, Language: "es", Devices: []string{"abc"}},
			},
			EmailUsers: []notifications.EmailUser{
				{UserID: 1, Email: "parent@example.com"},
			},
		},
	})
	if err != nil {
		log.Fatalf("publish: %v", err)
	}

	log.Println("note.created published")
}
