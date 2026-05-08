package notifications

import (
	"context"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// Client is the high-level entry point. It owns a Producer plus the resolved
// topic configuration and exposes one Publish helper per logical event.
type Client struct {
	producer Producer
	topics   Topics
}

// NewClient wires up the default segmentio-kafka-go-backed producer.
// Use NewClientWithProducer to inject a stub or alternative implementation.
func NewClient(cfg Config) (*Client, error) {
	cfg.Topics = cfg.Topics.withDefaults()
	p, err := newKafkaProducer(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{producer: p, topics: cfg.Topics}, nil
}

// NewClientWithProducer skips Kafka setup. Useful for tests or for plugging
// in a different transport (e.g. an in-memory ring buffer).
func NewClientWithProducer(p Producer, topics Topics) *Client {
	return &Client{producer: p, topics: topics.withDefaults()}
}

// Close flushes and shuts down the underlying producer.
func (c *Client) Close() error {
	if c.producer == nil {
		return nil
	}
	return c.producer.Close()
}

// Topics returns the resolved topic set, useful for diagnostics.
func (c *Client) Topics() Topics { return c.topics }

// PublishNoteCreated routes to the priority topic when n.Note.Important is true
// and to the regular topic otherwise. The partition key is the recipient's
// student_id when present, falling back to the note id — matching the Laravel
// NoteMessage::kafkaKey().
func (c *Client) PublishNoteCreated(ctx context.Context, n NoteCreated) error {
	if n.Version == "" {
		n.Version = SchemaVersion
	}
	if n.EventType == "" {
		n.EventType = EventNoteCreated
	}
	if n.EventID == "" {
		n.EventID = uuid.NewString()
	}
	if n.Timestamp == "" {
		n.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}
	if n.Metadata.TraceID == "" {
		n.Metadata.TraceID = uuid.NewString()
	}
	if n.Metadata.CreatedAt == "" {
		n.Metadata.CreatedAt = n.Timestamp
	}

	topic := c.topics.NoteCreated
	if n.Note.Important {
		topic = c.topics.NoteCreatedPriority
	}

	key := strconv.Itoa(n.Note.NoteID)
	if n.Recipient.StudentID != 0 {
		key = strconv.Itoa(n.Recipient.StudentID)
	}

	priority := PriorityNormal
	if n.Note.Important {
		priority = PriorityHigh
	}

	return c.producer.Produce(ctx, topic, key, n, map[string]string{
		"trace-id":  n.Metadata.TraceID,
		"school-id": strconv.Itoa(n.Note.SchoolID),
		"priority":  string(priority),
	})
}

// PublishEmail puts one entry on the email batch topic.
// Key is the recipient user id, matching the Laravel side.
func (c *Client) PublishEmail(ctx context.Context, m EmailMessage) error {
	if m.EventID == "" {
		m.EventID = uuid.NewString()
	}
	if m.TraceID == "" {
		m.TraceID = uuid.NewString()
	}
	if m.Priority == "" {
		m.Priority = PriorityNormal
	}
	return c.producer.Produce(ctx, c.topics.EmailBatch, strconv.Itoa(m.UserID), m, map[string]string{
		"trace-id":  m.TraceID,
		"school-id": strconv.Itoa(m.SchoolID),
		"priority":  string(m.Priority),
	})
}

// PublishPush puts one entry on the push batch topic.
func (c *Client) PublishPush(ctx context.Context, m PushMessage) error {
	if m.EventID == "" {
		m.EventID = uuid.NewString()
	}
	if m.TraceID == "" {
		m.TraceID = uuid.NewString()
	}
	if m.Priority == "" {
		m.Priority = PriorityNormal
	}
	if m.Data == nil {
		m.Data = map[string]string{}
	}
	return c.producer.Produce(ctx, c.topics.PushBatch, strconv.Itoa(m.UserID), m, map[string]string{
		"trace-id":  m.TraceID,
		"school-id": strconv.Itoa(m.SchoolID),
		"priority":  string(m.Priority),
	})
}

// PublishSMS puts one entry on the SMS batch topic.
func (c *Client) PublishSMS(ctx context.Context, m SMSMessage) error {
	if m.EventID == "" {
		m.EventID = uuid.NewString()
	}
	if m.TraceID == "" {
		m.TraceID = uuid.NewString()
	}
	if m.Priority == "" {
		m.Priority = PriorityNormal
	}
	return c.producer.Produce(ctx, c.topics.SMSBatch, strconv.Itoa(m.UserID), m, map[string]string{
		"trace-id":  m.TraceID,
		"school-id": strconv.Itoa(m.SchoolID),
		"priority":  string(m.Priority),
	})
}

// PublishWhatsApp puts one entry on the WhatsApp batch topic.
func (c *Client) PublishWhatsApp(ctx context.Context, m WhatsAppMessage) error {
	if m.EventID == "" {
		m.EventID = uuid.NewString()
	}
	if m.TraceID == "" {
		m.TraceID = uuid.NewString()
	}
	if m.Priority == "" {
		m.Priority = PriorityNormal
	}
	return c.producer.Produce(ctx, c.topics.WhatsAppBatch, strconv.Itoa(m.UserID), m, map[string]string{
		"trace-id":  m.TraceID,
		"school-id": strconv.Itoa(m.SchoolID),
		"priority":  string(m.Priority),
	})
}

// PublishRaw is the escape hatch for anything that does not yet have a
// first-class helper. The caller owns topic, key, and headers.
func (c *Client) PublishRaw(ctx context.Context, topic, key string, payload any, headers map[string]string) error {
	return c.producer.Produce(ctx, topic, key, payload, headers)
}
