package notifications

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
)

// fakeProducer captures the last Produce call so tests can inspect topic,
// key, headers, and payload without contacting Kafka.
type fakeProducer struct {
	topic   string
	key     string
	payload []byte
	headers map[string]string
}

func (f *fakeProducer) Produce(_ context.Context, topic, key string, payload any, headers map[string]string) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	f.topic = topic
	f.key = key
	f.payload = body
	f.headers = headers
	return nil
}

func (f *fakeProducer) Close() error { return nil }

func newTestClient() (*Client, *fakeProducer) {
	fp := &fakeProducer{}
	return NewClientWithProducer(fp, Topics{}), fp
}

func TestPublishNoteCreated_RoutesPriorityTopic(t *testing.T) {
	c, fp := newTestClient()

	err := c.PublishNoteCreated(context.Background(), NoteCreated{
		Note: Note{
			NoteID:    42,
			SchoolID:  7,
			Important: true,
		},
		Recipient: Recipient{"student_id": int64(99)},
	})
	if err != nil {
		t.Fatalf("PublishNoteCreated: %v", err)
	}

	if fp.topic != DefaultTopicNoteCreatedPriority {
		t.Errorf("topic = %q, want %q", fp.topic, DefaultTopicNoteCreatedPriority)
	}
	if fp.key != "99" {
		t.Errorf("key = %q, want \"99\"", fp.key)
	}
	if fp.headers["priority"] != string(PriorityHigh) {
		t.Errorf("priority header = %q, want high", fp.headers["priority"])
	}
	if fp.headers["school-id"] != "7" {
		t.Errorf("school-id header = %q, want 7", fp.headers["school-id"])
	}
	if fp.headers["trace-id"] == "" {
		t.Error("expected trace-id header to be auto-populated")
	}
}

func TestPublishNoteCreated_DefaultsAndShape(t *testing.T) {
	c, fp := newTestClient()

	err := c.PublishNoteCreated(context.Background(), NoteCreated{
		Note: Note{NoteID: 1, SchoolID: 2},
	})
	if err != nil {
		t.Fatalf("PublishNoteCreated: %v", err)
	}

	if fp.topic != DefaultTopicNoteCreated {
		t.Errorf("topic = %q, want %q", fp.topic, DefaultTopicNoteCreated)
	}

	var decoded map[string]any
	if err := json.Unmarshal(fp.payload, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded["version"] != SchemaVersion {
		t.Errorf("version = %v, want %s", decoded["version"], SchemaVersion)
	}
	if decoded["event_type"] != EventNoteCreated {
		t.Errorf("event_type = %v, want %s", decoded["event_type"], EventNoteCreated)
	}
	for _, k := range []string{"event_id", "timestamp", "note", "recipient", "attachments", "metadata"} {
		if _, ok := decoded[k]; !ok {
			t.Errorf("payload missing key %q", k)
		}
	}
}

func TestPublishEmail_RoutingAndKey(t *testing.T) {
	c, fp := newTestClient()

	err := c.PublishEmail(context.Background(), EmailMessage{
		SchoolID: 3,
		UserID:   55,
		Email:    "x@y.com",
		Subject:  "hi",
		Content:  "<p>hi</p>",
	})
	if err != nil {
		t.Fatalf("PublishEmail: %v", err)
	}

	if fp.topic != DefaultTopicEmailBatch {
		t.Errorf("topic = %q, want %q", fp.topic, DefaultTopicEmailBatch)
	}
	if fp.key != strconv.FormatInt(55, 10) {
		t.Errorf("key = %q, want \"55\"", fp.key)
	}
	if fp.headers["priority"] != string(PriorityNormal) {
		t.Errorf("priority = %q, want normal (default)", fp.headers["priority"])
	}
}

func TestConfigFromEnv_AppliesTopicDefaults(t *testing.T) {
	cfg := ConfigFromEnv()
	if cfg.Topics.NoteCreated == "" || cfg.Topics.EmailBatch == "" {
		t.Errorf("expected default topics, got %#v", cfg.Topics)
	}
}
