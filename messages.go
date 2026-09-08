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

	// Store decides whether this notification becomes a HISTORY ROW. It is
	// independent of Channels: Channels says who DELIVERS it, Store says
	// whether it is REMEMBERED.
	//
	// ⚠️ A POINTER, DELIBERATELY — nil means "unset", which means PERSIST.
	// This mirrors the legacy rule exactly: said-notifications stores unless
	// the producer sent literal "0" (`if (request()->store !== "0")`), so an
	// absent value has always meant "keep it".
	//
	// ⚠️ A PLAIN bool WOULD HAVE THE WRONG DEFAULT AND LOSE HISTORY SILENTLY.
	// Go's zero value is false, so any producer that forgot the field would
	// stop being persisted — no error, no log, just a user whose history
	// quietly goes empty. That is the exact failure this work exists to
	// prevent, arriving through the field meant to control it.
	//
	// Read it with ShouldPersist(), never directly.
	Store *bool `json:"store,omitempty"`
}

// EventMetadata carries tracing info on every NoteCreated event.
// ShouldPersist reports whether this notification becomes a history row.
//
// Unset (nil) means YES, mirroring legacy: said-notifications persists unless
// the producer explicitly sent "0". Defaulting the other way would make a
// forgotten field indistinguishable from a deliberate suppression, and the
// consequence — a silently empty history — is invisible until a parent asks
// where their notification went.
func (n Note) ShouldPersist() bool { return n.Store == nil || *n.Store }

// PersistFlag builds an explicit Store value.
//
//	Note{Store: notifications.PersistFlag(false)} // deliver, do not remember
func PersistFlag(v bool) *bool { return &v }

// IsDeliveryless reports whether this note must NOT be delivered by a fan-out
// consumer — it exists only to be persisted as history.
//
// This is the guard against double fan-out. See the Channels field comment.
//
// ⚠️ It tests LENGTH, not content. Channels{""} has length 1 and is therefore
// NOT delivery-less — a fan-out consumer will look for a channel named "" and
// deliver nothing anyway, but through the "unknown channel" path rather than
// the persist-only one. Producers must not emit blank channel values, and
// consumers should validate channel names rather than assume this predicate
// screens them.
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
	EventID   string `json:"event_id"`
	TraceID   string `json:"trace_id"`
	NoteID    int    `json:"note_id"`
	SchoolID  int    `json:"school_id"`
	StudentID int    `json:"student_id"`
	UserID    int    `json:"user_id"`
	Email     string `json:"email"`

	// RecipientType names the id-space UserID belongs to:
	// RecipientTypeUserEmail (`user_email` table) or RecipientTypeStaff
	// (`staff` table). It is NOT the `user` table on either path.
	//
	// ⚠️ REQUIRED FOR ANYTHING THAT KEYS ON THE RECIPIENT. Empty means the
	// producer has not been updated; consumers must treat that as "unknown"
	// and decline to record, rather than guessing a default — a wrong guess
	// attributes a recipient to the wrong person and cannot be detected later.
	RecipientType string            `json:"recipient_type,omitempty"`
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

// Recipient id-spaces for note emails.
//
// ⚠️ user_id ON AN EMAIL MESSAGE IS POLYMORPHIC, AND THAT IS THE PROBLEM THIS
// SOLVES. Two independent producers write this one field from two different
// tables, and both land on the same email topic:
//
//	family → NoteRecipientResolver::buildEmailUsers:329
//	         'user_id' => $userEmail->id          → user_email.id
//	staff  → NoteMessage.php:261
//	         'user_id' => $staff['staff_id']      → staff.id
//
// Their low ids densely overlap, so anything keying on (note_id, user_id)
// collides: staff #5 and family address #5 on one note are the same key.
// Mixed staff+family is the NORMAL case for opt-in staff copies, not an edge.
//
// RecipientType is the discriminator. It must be set explicitly by the
// producer — it cannot be inferred downstream, and inferring it from a null
// student_id would silently misfile a family recipient into the staff
// id-space, which is the exact data loss this exists to prevent.
const (
	// RecipientTypeUserEmail is the FAMILY path. UserID holds a `user_email`
	// PK — an EMAIL ADDRESS row, not a person. One human with two addresses
	// is two recipients here; that per-address unit is the correct one for
	// open-tracking and is the semantics hsingli signed off on.
	//
	// Deliberately NOT named "user": `user_email.id` and `user.id` are
	// different tables. Calling this "user" would be a false label, and a
	// consumer joining recipient_id → user.id would resolve the wrong person
	// or nobody at all — silently, since both ids are small positive ints.
	RecipientTypeUserEmail = "user_email"

	// RecipientTypeStaff is the STAFF path. UserID holds a `staff` PK.
	RecipientTypeStaff = "staff"
)

// The set above is CLOSED, and "user" (the `user` table) is absent on purpose.
//
// schoolaid-admin does have producers that put a real `user.id` in this field
// — StudentRecipientResolver:77,102,126,151 ('user_id' => $userEmail->user_id)
// and UserRecipientResolver:67,91,114,138 — but those run on the LEGACY mail
// channel, not on Kafka, so no such value reaches this contract today.
//
// Do not add a "user" constant pre-emptively. An accepted-but-unproduced label
// only creates a way to mislabel a user_email id as a user id, which is the
// defect this type exists to prevent. If a legacy path is ever moved onto
// Kafka, adding the constant then is a deliberate, reviewable change; until
// then an unknown value must fail closed at the consumer.

// Command actions carried by PushCommand.Action.
const (
	// ActionBusOff turns a bus device off. The first command in use.
	//
	// ⚠️ Action is an OPEN set, like notification_type: a consumer meeting an
	// unknown action must log it and count it, never silently drop the
	// message. A dropped command is indistinguishable from a delivered one
	// from the producer's side.
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
	EventID  string `json:"event_id"`
	TraceID  string `json:"trace_id"`
	SchoolID int    `json:"school_id"`
	Action   string `json:"action"`
	// Devices is the exact set of device tokens to command. It MUST be
	// non-empty, and PublishPushCommand rejects it if it is not.
	//
	// ⚠️ EMPTY HAS NO LEGITIMATE MEANING HERE, WHICH IS WHY IT IS AN ERROR
	// RATHER THAN A CONVENTION. Contrast Channels, where empty means
	// "persist-only" and is a deliberate signal. An empty device list reads
	// three different ways to a consumer — nothing, unknown, or "everything in
	// SchoolID" — and the last one is the dangerous reading: a producer bug
	// that builds an empty slice would turn off EVERY bus in the school. There
	// is no command whose correct audience is "no devices", so the ambiguity
	// is removed at the source instead of each consumer guessing.
	Devices  []string          `json:"devices"`
	Priority Priority          `json:"priority"`
	Data     map[string]string `json:"data,omitempty"`
}
