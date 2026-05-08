package notifications

import (
	"context"
	"encoding/json"
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
		Recipient: Recipient{StudentID: 99},
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

func TestPublishNoteCreated_FallsBackToNoteIDForKey(t *testing.T) {
	c, fp := newTestClient()

	err := c.PublishNoteCreated(context.Background(), NoteCreated{
		Note: Note{NoteID: 17, SchoolID: 1},
	})
	if err != nil {
		t.Fatalf("PublishNoteCreated: %v", err)
	}
	if fp.key != "17" {
		t.Errorf("key = %q, want \"17\" (note id fallback)", fp.key)
	}
}

func TestPublishNoteCreated_Shape(t *testing.T) {
	c, fp := newTestClient()

	err := c.PublishNoteCreated(context.Background(), NoteCreated{
		Note: Note{NoteID: 1, SchoolID: 2, Channels: []string{"push"}},
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

func TestPublishPush_MatchesConsumerWireFormat(t *testing.T) {
	c, fp := newTestClient()

	err := c.PublishPush(context.Background(), PushMessage{
		NoteID:   123,
		SchoolID: 7,
		UserID:   55,
		Title:    "Nueva circular: hola",
		Devices:  []string{"tok-1", "tok-2"},
		Data:     map[string]string{"item_id": "123", "notification_type": "note"},
	})
	if err != nil {
		t.Fatalf("PublishPush: %v", err)
	}

	if fp.topic != DefaultTopicPushBatch {
		t.Errorf("topic = %q, want %q", fp.topic, DefaultTopicPushBatch)
	}
	if fp.key != "55" {
		t.Errorf("key = %q, want \"55\"", fp.key)
	}

	// Field names must line up with consumer model PushBatchMessage.
	var decoded map[string]any
	if err := json.Unmarshal(fp.payload, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, k := range []string{"event_id", "trace_id", "note_id", "school_id", "user_id", "title", "priority", "devices", "data"} {
		if _, ok := decoded[k]; !ok {
			t.Errorf("push payload missing key %q (consumer contract)", k)
		}
	}
	// Devices and data must be the consumer-expected shapes
	if _, ok := decoded["devices"].([]any); !ok {
		t.Errorf("devices not a JSON array, got %T", decoded["devices"])
	}
	if data, ok := decoded["data"].(map[string]any); !ok {
		t.Errorf("data not an object, got %T", decoded["data"])
	} else if _, ok := data["item_id"].(string); !ok {
		t.Errorf("data.item_id not a string (consumer expects map[string]string)")
	}
}

func TestPublishEmail_MatchesConsumerWireFormat(t *testing.T) {
	c, fp := newTestClient()

	err := c.PublishEmail(context.Background(), EmailMessage{
		NoteID:    9,
		SchoolID:  3,
		UserID:    55,
		Email:     "x@y.com",
		Subject:   "hi",
		Content:   "<p>hi</p>",
	})
	if err != nil {
		t.Fatalf("PublishEmail: %v", err)
	}

	if fp.topic != DefaultTopicEmailBatch {
		t.Errorf("topic = %q, want %q", fp.topic, DefaultTopicEmailBatch)
	}
	if fp.key != "55" {
		t.Errorf("key = %q, want \"55\"", fp.key)
	}
	if fp.headers["priority"] != string(PriorityNormal) {
		t.Errorf("priority = %q, want normal (default)", fp.headers["priority"])
	}

	var decoded map[string]any
	if err := json.Unmarshal(fp.payload, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// note_id must be a number (not null) — consumer field is non-pointer int
	if _, ok := decoded["note_id"].(float64); !ok {
		t.Errorf("note_id should be a number, got %T (%v)", decoded["note_id"], decoded["note_id"])
	}
}

func TestConfigFromEnv_AppliesTopicDefaults(t *testing.T) {
	cfg := ConfigFromEnv()
	if cfg.Topics.NoteCreated == "" || cfg.Topics.EmailBatch == "" {
		t.Errorf("expected default topics, got %#v", cfg.Topics)
	}
}
