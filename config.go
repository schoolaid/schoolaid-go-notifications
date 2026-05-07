package notifications

import (
	"errors"
	"os"
	"strings"
	"time"
)

// Priority is the value carried by the "priority" Kafka header and by the
// priority field embedded in payloads.
type Priority string

const (
	PriorityNormal Priority = "normal"
	PriorityHigh   Priority = "high"
)

// Config controls how the producer connects to Kafka and which topics it
// resolves for each high-level Publish helper. All fields are optional except
// Brokers; topic fields fall back to the Laravel defaults so a Go service can
// publish to the same destinations a Laravel producer would.
type Config struct {
	// Brokers is a comma-separated list of host:port pairs. Required.
	Brokers string

	// ClientID identifies the producer to the broker. Optional; defaults to
	// "schoolaid-go".
	ClientID string

	// Topics override the default Laravel topic names. Empty values fall
	// back to defaults defined in topics.go.
	Topics Topics

	// Async controls whether writes are fire-and-forget. Mirrors the
	// Laravel side which dispatches a queued job. Defaults to true.
	Async bool

	// BatchTimeout maps to Kafka's linger.ms. Defaults to 20ms to match
	// the Laravel KAFKA_LINGER_MS default.
	BatchTimeout time.Duration

	// BatchSize maps to batch.num.messages. Defaults to 10000 to match
	// the Laravel KAFKA_BATCH_NUM_MESSAGES default.
	BatchSize int

	// RequireAll, when true, sets acks=all. Defaults to true.
	RequireAll bool

	// WriteTimeout caps how long a synchronous write may block.
	// Defaults to 10s.
	WriteTimeout time.Duration

	// Enabled lets a service ship the wrapper but keep production traffic
	// on the legacy path until a flag flip. When false, all Publish calls
	// return nil without contacting Kafka.
	Enabled bool
}

// ConfigFromEnv reads the same KAFKA_* environment variables Laravel uses, so
// the same deployment manifest can drive both sides of the cutover.
//
//	KAFKA_ENABLED                    -> Enabled (bool, default false)
//	KAFKA_BROKERS                    -> Brokers (default "localhost:9092")
//	KAFKA_CLIENT_ID                  -> ClientID (default "schoolaid-go")
//	KAFKA_TOPIC_NOTE_CREATED         -> Topics.NoteCreated
//	KAFKA_TOPIC_NOTE_CREATED_PRIORITY-> Topics.NoteCreatedPriority
//	KAFKA_TOPIC_EMAIL_BATCH          -> Topics.EmailBatch
//	KAFKA_TOPIC_PUSH_BATCH           -> Topics.PushBatch
//	KAFKA_TOPIC_SMS_BATCH            -> Topics.SMSBatch
//	KAFKA_TOPIC_WHATSAPP_BATCH       -> Topics.WhatsAppBatch
//	KAFKA_TOPIC_DLQ                  -> Topics.DLQ
//	KAFKA_TOPIC_RETRY                -> Topics.Retry
func ConfigFromEnv() Config {
	cfg := Config{
		Brokers:      envOr("KAFKA_BROKERS", "localhost:9092"),
		ClientID:     envOr("KAFKA_CLIENT_ID", "schoolaid-go"),
		Async:        true,
		RequireAll:   true,
		BatchTimeout: 20 * time.Millisecond,
		BatchSize:    10000,
		WriteTimeout: 10 * time.Second,
		Enabled:      envBool("KAFKA_ENABLED", false),
		Topics: Topics{
			NoteCreated:         os.Getenv("KAFKA_TOPIC_NOTE_CREATED"),
			NoteCreatedPriority: os.Getenv("KAFKA_TOPIC_NOTE_CREATED_PRIORITY"),
			EmailBatch:          os.Getenv("KAFKA_TOPIC_EMAIL_BATCH"),
			PushBatch:           os.Getenv("KAFKA_TOPIC_PUSH_BATCH"),
			SMSBatch:            os.Getenv("KAFKA_TOPIC_SMS_BATCH"),
			WhatsAppBatch:       os.Getenv("KAFKA_TOPIC_WHATSAPP_BATCH"),
			DLQ:                 os.Getenv("KAFKA_TOPIC_DLQ"),
			Retry:               os.Getenv("KAFKA_TOPIC_RETRY"),
		},
	}
	cfg.Topics = cfg.Topics.withDefaults()
	return cfg
}

func (c *Config) validate() error {
	if strings.TrimSpace(c.Brokers) == "" {
		return errors.New("notifications: Config.Brokers is required")
	}
	return nil
}

func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off", "":
		return false
	}
	return fallback
}
