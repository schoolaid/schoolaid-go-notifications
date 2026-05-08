# schoolaid-go-notifications

Thin Go wrapper around a Kafka producer for the SchoolAid notifications
pipeline. Mirrors the schemas, topic names, and header conventions used by
[`schoolaid-admin`](https://github.com/schoolaid)'s Laravel `KafkaChannel`
so a Go service can publish identical messages without re-deriving the
wire format.

The package is intentionally a wrapper. There are **no service-specific
defaults, secrets, or business rules** — brokers, topics, and client
identity come from the caller (typically the same `KAFKA_*` environment
variables Laravel reads).

## Install

```bash
go get github.com/schoolaid/schoolaid-go-notifications
```

## Usage

```go
import notifications "github.com/schoolaid/schoolaid-go-notifications"

cfg := notifications.ConfigFromEnv()
client, err := notifications.NewClient(cfg)
if err != nil { return err }
defer client.Close()

err = client.PublishPush(ctx, notifications.PushMessage{
    NoteID:   1001,
    SchoolID: 42,
    UserID:   99,
    Title:    "Nueva circular: Example",
    Devices:  []string{"fcm-token-a", "fcm-token-b"},
    Data:     map[string]string{"item_id": "1001", "notification_type": "note"},
})
```

Schemas mirror the consumer at
[`github.com/schoolaid/notifications`](https://github.com/schoolaid/notifications)
(`internal/models/event.go` + `batch.go`). Producers MUST match — mismatched
fields land in the DLQ.

## Environment

Reads the same variables `schoolaid-admin/config/kafka.php` reads:

| Var | Default |
|---|---|
| `KAFKA_ENABLED` | `false` |
| `KAFKA_BROKERS` | `localhost:9092` |
| `KAFKA_CLIENT_ID` | `schoolaid-go` |
| `KAFKA_TOPIC_NOTE_CREATED` | `notifications.note.created` |
| `KAFKA_TOPIC_NOTE_CREATED_PRIORITY` | `notifications.note.created.priority` |
| `KAFKA_TOPIC_EMAIL_BATCH` | `notifications.email.batch` |
| `KAFKA_TOPIC_PUSH_BATCH` | `notifications.push.batch` |
| `KAFKA_TOPIC_SMS_BATCH` | `notifications.sms.batch` |
| `KAFKA_TOPIC_WHATSAPP_BATCH` | `notifications.whatsapp.batch` |
| `KAFKA_TOPIC_DLQ` | `notifications.dlq` |
| `KAFKA_TOPIC_RETRY` | `notifications.retry` |

## Helpers

| Method | Topic | Partition key |
|---|---|---|
| `PublishNoteCreated` | `note.created` (or `.priority`) | `recipient.student_id` → `note_id` |
| `PublishEmail` | `email.batch` | `user_id` |
| `PublishPush` | `push.batch` | `user_id` |
| `PublishSMS` | `sms.batch` | `user_id` |
| `PublishWhatsApp` | `whatsapp.batch` | `user_id` |
| `PublishRaw` | caller-supplied | caller-supplied |

All helpers add three Kafka headers: `trace-id`, `school-id`, `priority`.

## Testing

`NewClientWithProducer` accepts any `Producer` implementation, so unit
tests can stub the broker entirely. See `client_test.go`.

## Schema source of truth

Field names and JSON tags mirror
`schoolaid-admin/app/Notifications/Messages/*`. When the Laravel side
changes, this package follows.
