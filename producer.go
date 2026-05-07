package notifications

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// Producer is the minimum surface needed to publish a raw Kafka message.
// Defining it as an interface lets callers stub the wrapper in tests
// without spinning up a broker.
type Producer interface {
	Produce(ctx context.Context, topic string, key string, payload any, headers map[string]string) error
	Close() error
}

// kafkaProducer is the segmentio/kafka-go-backed implementation.
type kafkaProducer struct {
	writer       *kafka.Writer
	writeTimeout time.Duration
	enabled      bool
}

func newKafkaProducer(cfg Config) (*kafkaProducer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	required := kafka.RequireOne
	if cfg.RequireAll {
		required = kafka.RequireAll
	}

	w := &kafka.Writer{
		Addr:                   kafka.TCP(splitBrokers(cfg.Brokers)...),
		Balancer:               &kafka.Hash{},
		RequiredAcks:           required,
		Async:                  cfg.Async,
		BatchTimeout:           cfg.BatchTimeout,
		BatchSize:              cfg.BatchSize,
		Compression:            kafka.Snappy,
		AllowAutoTopicCreation: false,
	}

	return &kafkaProducer{
		writer:       w,
		writeTimeout: cfg.WriteTimeout,
		enabled:      cfg.Enabled,
	}, nil
}

// Produce JSON-encodes payload and writes a single message to the topic.
// Headers are written as Kafka record headers; "trace-id" must be present
// in headers (the Client wrapper adds it automatically).
func (p *kafkaProducer) Produce(ctx context.Context, topic, key string, payload any, headers map[string]string) error {
	if !p.enabled {
		return nil
	}
	if topic == "" {
		return fmt.Errorf("notifications: empty topic")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("notifications: encode payload: %w", err)
	}

	hdrs := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		hdrs = append(hdrs, kafka.Header{Key: k, Value: []byte(v)})
	}

	msg := kafka.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   body,
		Headers: hdrs,
		Time:    time.Now(),
	}

	if p.writeTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.writeTimeout)
		defer cancel()
	}

	return p.writer.WriteMessages(ctx, msg)
}

// Close flushes the underlying writer.
func (p *kafkaProducer) Close() error {
	if p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

func splitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
