// Package notifications is a thin wrapper around a Kafka producer for the
// SchoolAid notifications pipeline. It mirrors the schemas, topics, and
// header conventions used by the schoolaid-admin Laravel KafkaChannel so
// Go services can publish identical messages without re-deriving the wire
// format.
//
// The package intentionally carries no secrets or service-specific defaults.
// Brokers, topics, and client identity are supplied by the caller (typically
// from environment variables matching the Laravel KAFKA_* names).
//
// Quick start:
//
//	c, err := notifications.NewClient(notifications.ConfigFromEnv())
//	if err != nil { return err }
//	defer c.Close()
//
//	err = c.PublishEmail(ctx, notifications.EmailMessage{
//	    EventID:   uuid.NewString(),
//	    TraceID:   traceID,
//	    SchoolID:  42,
//	    UserID:    99,
//	    Email:     "foo@example.com",
//	    Subject:   "Hello",
//	    Content:   "<p>hi</p>",
//	    Priority:  notifications.PriorityNormal,
//	})
package notifications
