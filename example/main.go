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
		},
		Recipient: notifications.Recipient{
			"student_id": int64(7),
			"push_users": []map[string]any{
				{"user_id": 1, "device_ids": []string{"abc"}},
			},
		},
	})
	if err != nil {
		log.Fatalf("publish: %v", err)
	}

	log.Println("note.created published")
}
