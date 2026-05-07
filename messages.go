package notifications

// Wire-format definitions for the SchoolAid notifications topics.
//
// Field names and JSON tags mirror the payloads built by
// schoolaid-admin/app/Notifications/Messages/* so producers and consumers
// share a single contract. When in doubt, the Laravel side is authoritative
// and this file should be updated to match.

const SchemaVersion = "1.0"

// EventType values used in NoteCreated.EventType.
const (
	EventNoteCreated = "note.created"
)

// Attachment represents a single signed attachment URL.
// Mirrors NoteMessage::buildCachedAttachments().
type Attachment struct {
	Filename string  `json:"filename"`
	Path     string  `json:"path,omitempty"`
	URL      *string `json:"url"`
}

// EmailAttachment is the trimmed shape attached to email batch payloads.
type EmailAttachment struct {
	Filename string  `json:"filename"`
	URL      *string `json:"url"`
}

// PushTitle holds localized push notification titles.
type PushTitle struct {
	ES *string `json:"es"`
	EN *string `json:"en"`
}

// Note is the note-level body shared across recipients in a NoteCreated event.
// Matches NoteMessage::buildCachedNotePayload().
type Note struct {
	NoteID         int64     `json:"note_id"`
	SchoolID       int64     `json:"school_id"`
	Title          string    `json:"title"`
	Subject        string    `json:"subject"`
	FromName       *string   `json:"from_name"`
	PushTitle      PushTitle `json:"push_title"`
	Content        string    `json:"content"`
	Important      bool      `json:"important"`
	FeaturedImage  *string   `json:"featured_image"`
	Signature      *string   `json:"signature"`
	Channels       []string  `json:"channels"`
	RelativeTypeID *int64    `json:"relative_type_id"`
}

// Recipient is the per-student block produced by the Laravel kafka resolver.
// Carried verbatim through the wrapper; consumers know the shape.
type Recipient map[string]any

// Metadata mirrors NoteMessage::toKafka()['metadata'].
type Metadata struct {
	TraceID     string  `json:"trace_id"`
	UserID      *int64  `json:"user_id"`
	CreatedAt   string  `json:"created_at"`
	ScheduledAt *string `json:"scheduled_at"`
}

// NoteCreated is the full notifications.note.created[.priority] payload.
type NoteCreated struct {
	Version     string       `json:"version"`
	EventType   string       `json:"event_type"`
	EventID     string       `json:"event_id"`
	Timestamp   string       `json:"timestamp"`
	Note        Note         `json:"note"`
	Recipient   Recipient    `json:"recipient"`
	Attachments []Attachment `json:"attachments"`
	Metadata    Metadata     `json:"metadata"`
}

// EmailMessage is the wire format of one notifications.email.batch entry.
// Matches NoteMessage::additionalKafkaMessages() payload shape.
type EmailMessage struct {
	EventID       string            `json:"event_id"`
	TraceID       string            `json:"trace_id"`
	NoteID        *int64            `json:"note_id"`
	SchoolID      int64             `json:"school_id"`
	StudentID     *int64            `json:"student_id"`
	UserID        int64             `json:"user_id"`
	Email         string            `json:"email"`
	FromName      *string           `json:"from_name"`
	Subject       string            `json:"subject"`
	Content       string            `json:"content"`
	FeaturedImage *string           `json:"featured_image"`
	Signature     *string           `json:"signature"`
	Attachments   []EmailAttachment `json:"attachments"`
	Priority      Priority          `json:"priority"`
}

// PushMessage is one entry on notifications.push.batch.
type PushMessage struct {
	EventID   string         `json:"event_id"`
	TraceID   string         `json:"trace_id"`
	SchoolID  int64          `json:"school_id"`
	StudentID *int64         `json:"student_id"`
	UserID    int64          `json:"user_id"`
	DeviceIDs []string       `json:"device_ids"`
	TitleES   string         `json:"title_es"`
	TitleEN   string         `json:"title_en"`
	BodyES    string         `json:"body_es"`
	BodyEN    string         `json:"body_en"`
	ImageURL  *string        `json:"image_url"`
	Data      map[string]any `json:"data"`
	Priority  Priority       `json:"priority"`
}

// SMSMessage is one entry on notifications.sms.batch.
type SMSMessage struct {
	EventID   string   `json:"event_id"`
	TraceID   string   `json:"trace_id"`
	SchoolID  int64    `json:"school_id"`
	StudentID *int64   `json:"student_id"`
	UserID    int64    `json:"user_id"`
	Phone     string   `json:"phone"`
	Body      string   `json:"body"`
	Priority  Priority `json:"priority"`
}

// WhatsAppMessage is one entry on notifications.whatsapp.batch.
type WhatsAppMessage struct {
	EventID   string   `json:"event_id"`
	TraceID   string   `json:"trace_id"`
	SchoolID  int64    `json:"school_id"`
	StudentID *int64   `json:"student_id"`
	UserID    int64    `json:"user_id"`
	Phone     string   `json:"phone"`
	Text      string   `json:"text"`
	Priority  Priority `json:"priority"`
}
