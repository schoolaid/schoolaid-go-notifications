package notifications

// Wire-format definitions for the SchoolAid notifications topics.
//
// These types mirror the consumer models at
// github.com/schoolaid/notifications/internal/models (event.go, batch.go)
// field-for-field. The consumer is the source of truth — when the consumer
// changes, this file follows.
//
// Producers (Laravel via KafkaChannel, Go via this wrapper) MUST emit JSON
// that unmarshalls into the consumer types. Mismatches go to the DLQ.

const SchemaVersion = "1.0"

// EventType values used in NoteCreated.EventType.
const (
	EventNoteCreated = "note.created"
)

// Attachment is the note-level attachment shape carried inside NoteCreated.
// Consumer: notifications/internal/models/event.go
type Attachment struct {
	Filename string `json:"filename"`
	Path     string `json:"path"`
	URL      string `json:"url"`
}

// EmailAttachment is the trimmed shape attached to email batch payloads.
// Consumer: notifications/internal/models/batch.go
type EmailAttachment struct {
	Filename string `json:"filename"`
	URL      string `json:"url"`
}

// PushUser is one recipient on the NoteCreated.Recipient.Users list.
type PushUser struct {
	UserID   int      `json:"user_id"`
	Language string   `json:"language"`
	Devices  []string `json:"devices"`
}

// EmailUser is one recipient on the NoteCreated.Recipient.EmailUsers list.
type EmailUser struct {
	UserID int    `json:"user_id"`
	Email  string `json:"email"`
}

// Recipient bundles the per-student recipient set for a NoteCreated event.
type Recipient struct {
	StudentID  int         `json:"student_id"`
	Users      []PushUser  `json:"users"`
	EmailUsers []EmailUser `json:"email_users,omitempty"`
}

// Note holds the note-level body for a NoteCreated event.
// PushTitle keys are language codes ("es", "en"); the consumer's
// Note.PushTitleFor() falls back to "es" then to Title.
type Note struct {
	NoteID        int               `json:"note_id"`
	SchoolID      int               `json:"school_id"`
	Title         string            `json:"title"`
	Subject       string            `json:"subject"`
	FromName      string            `json:"from_name"`
	PushTitle     map[string]string `json:"push_title,omitempty"`
	Content       string            `json:"content"`
	Important     bool              `json:"important"`
	FeaturedImage *string           `json:"featured_image"`
	Signature     *string           `json:"signature"`
	Channels      []string          `json:"channels"`
}

// EventMetadata carries tracing info on every NoteCreated event.
type EventMetadata struct {
	TraceID     string  `json:"trace_id"`
	UserID      int     `json:"user_id"`
	CreatedAt   string  `json:"created_at"`
	ScheduledAt *string `json:"scheduled_at"`
}

// NoteCreated is the full notifications.note.created[.priority] payload.
// The fan-out consumer reads this and emits PushBatch + EmailBatch messages.
type NoteCreated struct {
	Version     string        `json:"version"`
	EventType   string        `json:"event_type"`
	EventID     string        `json:"event_id"`
	Timestamp   string        `json:"timestamp"`
	Note        Note          `json:"note"`
	Recipient   Recipient     `json:"recipient"`
	Attachments []Attachment  `json:"attachments"`
	Metadata    EventMetadata `json:"metadata"`
}

// PushMessage is one entry on notifications.push.batch.
// Consumer model: PushBatchMessage in batch.go.
type PushMessage struct {
	EventID   string            `json:"event_id"`
	TraceID   string            `json:"trace_id"`
	NoteID    int               `json:"note_id"`
	SchoolID  int               `json:"school_id"`
	StudentID int               `json:"student_id"`
	UserID    int               `json:"user_id"`
	Title     string            `json:"title"`
	ImageURL  *string           `json:"image_url,omitempty"`
	Priority  Priority          `json:"priority"`
	Devices   []string          `json:"devices"`
	Data      map[string]string `json:"data"`
}

// EmailMessage is one entry on notifications.email.batch.
// Consumer model: EmailBatchMessage in batch.go.
type EmailMessage struct {
	EventID       string            `json:"event_id"`
	TraceID       string            `json:"trace_id"`
	NoteID        int               `json:"note_id"`
	SchoolID      int               `json:"school_id"`
	StudentID     int               `json:"student_id"`
	UserID        int               `json:"user_id"`
	Email         string            `json:"email"`
	FromName      string            `json:"from_name,omitempty"`
	Subject       string            `json:"subject"`
	Content       string            `json:"content"`
	FeaturedImage *string           `json:"featured_image,omitempty"`
	Signature     string            `json:"signature,omitempty"`
	Attachments   []EmailAttachment `json:"attachments,omitempty"`
	Priority      Priority          `json:"priority"`
}

// SMSMessage is one entry on notifications.sms.batch.
// The consumer does not yet handle this topic; field shape mirrors the
// other batch types so it can be added later without a wire break.
type SMSMessage struct {
	EventID   string   `json:"event_id"`
	TraceID   string   `json:"trace_id"`
	NoteID    int      `json:"note_id,omitempty"`
	SchoolID  int      `json:"school_id"`
	StudentID int      `json:"student_id,omitempty"`
	UserID    int      `json:"user_id"`
	Phone     string   `json:"phone"`
	Body      string   `json:"body"`
	Priority  Priority `json:"priority"`
}

// WhatsAppMessage is one entry on notifications.whatsapp.batch.
// As with SMSMessage, the consumer does not yet handle this topic.
type WhatsAppMessage struct {
	EventID   string   `json:"event_id"`
	TraceID   string   `json:"trace_id"`
	NoteID    int      `json:"note_id,omitempty"`
	SchoolID  int      `json:"school_id"`
	StudentID int      `json:"student_id,omitempty"`
	UserID    int      `json:"user_id"`
	Phone     string   `json:"phone"`
	Text      string   `json:"text"`
	Priority  Priority `json:"priority"`
}
