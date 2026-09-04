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
// PushTitle and PushBody keys are language codes ("es", "en"); the
// consumer's Note.PushTitleFor / PushBodyFor fall back to "es" then to
// Title (for PushTitle) or empty string (for PushBody).
type Note struct {
	NoteID        int               `json:"note_id"`
	SchoolID      int               `json:"school_id"`
	Title         string            `json:"title"`
	Subject       string            `json:"subject"`
	FromName      string            `json:"from_name"`
	PushTitle     map[string]string `json:"push_title,omitempty"`
	PushBody      map[string]string `json:"push_body,omitempty"`
	Content       string            `json:"content"`
	Important     bool              `json:"important"`
	FeaturedImage *string           `json:"featured_image"`
	Signature     *string           `json:"signature"`
	// Channels lists the DELIVERY transports a fan-out consumer must execute
	// (push, email, whatsapp). It is not a transport field itself.
	//
	// ⚠️ EMPTY MEANS DELIVER NOTHING — the event is PERSIST-ONLY.
	// A producer that already fanned out its own delivery (positions-api
	// publishes straight to the push topic) emits note.created with an empty
	// Channels purely so history has the pre-fanout event and its full
	// recipient list. A fan-out consumer that ignores this WILL double-send:
	// once from the producer's own fanout, once from the note event.
	//
	// Use IsDeliveryless() rather than testing len() at each call site, so the
	// rule lives in one place. Non-empty means deliver via exactly those
	// channels — schoolaid-admin's NoteMessage::deliveryChannels() always
	// returns at least ["push"], so today no producer emits empty.
	Channels []string `json:"channels"`
}

// EventMetadata carries tracing info on every NoteCreated event.
// IsDeliveryless reports whether this note must NOT be delivered by a fan-out
// consumer — it exists only to be persisted as history.
//
// This is the guard against double fan-out. See the Channels field comment.
func (n Note) IsDeliveryless() bool { return len(n.Channels) == 0 }

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
	Body      string            `json:"body,omitempty"`
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

// Command actions carried by PushCommand.Action.
const (
	// ActionBusOff turns a bus device off. The first command in use.
	ActionBusOff = "off"
)

// PushCommand is a data-only DEVICE command: an instruction to a device, not a
// message to a person.
//
// ⚠️ IT TRAVELS ON ITS OWN TOPIC (Topics.PushCommand) AND THAT IS DELIBERATE.
// It deliberately carries NO user_id, student_id, title or body, because it is
// not history and must never be persisted as such. Riding the push batch topic
// with those fields nulled would make it indistinguishable — by absence alone —
// from a genuine notification whose attribution an upstream bug had dropped,
// and the persistence consumer would silently discard real history while
// believing it was skipping a command. A separate topic makes the distinction
// structural: the persistence consumer never subscribes here, so a command
// cannot reach history by any path, and a misrouted message breaks DELIVERY,
// which is loud, instead of HISTORY, which is silent.
//
// Consequently a message on the push batch topic with no attribution is a
// DEFECT, not a command. Log it and count it; never silently drop it.
type PushCommand struct {
	EventID  string            `json:"event_id"`
	TraceID  string            `json:"trace_id"`
	SchoolID int               `json:"school_id"`
	Action   string            `json:"action"`
	Devices  []string          `json:"devices"`
	Priority Priority          `json:"priority"`
	Data     map[string]string `json:"data,omitempty"`
}
