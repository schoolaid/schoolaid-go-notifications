package notifications

// Topics is the set of Kafka topics the wrapper knows how to publish to.
// Defaults match the values in schoolaid-admin's config/kafka.php so a
// Go service can drop in alongside Laravel without re-wiring topic names.
type Topics struct {
	NoteCreated         string
	NoteCreatedPriority string
	EmailBatch          string
	PushBatch           string
	SMSBatch            string
	WhatsAppBatch       string
	DLQ                 string
	Retry               string
}

// Default topic names, kept in sync with schoolaid-admin/config/kafka.php.
const (
	DefaultTopicNoteCreated         = "notifications.note.created"
	DefaultTopicNoteCreatedPriority = "notifications.note.created.priority"
	DefaultTopicEmailBatch          = "notifications.email.batch"
	DefaultTopicPushBatch           = "notifications.push.batch"
	DefaultTopicSMSBatch            = "notifications.sms.batch"
	DefaultTopicWhatsAppBatch       = "notifications.whatsapp.batch"
	DefaultTopicDLQ                 = "notifications.dlq"
	DefaultTopicRetry               = "notifications.retry"
)

func (t Topics) withDefaults() Topics {
	if t.NoteCreated == "" {
		t.NoteCreated = DefaultTopicNoteCreated
	}
	if t.NoteCreatedPriority == "" {
		t.NoteCreatedPriority = DefaultTopicNoteCreatedPriority
	}
	if t.EmailBatch == "" {
		t.EmailBatch = DefaultTopicEmailBatch
	}
	if t.PushBatch == "" {
		t.PushBatch = DefaultTopicPushBatch
	}
	if t.SMSBatch == "" {
		t.SMSBatch = DefaultTopicSMSBatch
	}
	if t.WhatsAppBatch == "" {
		t.WhatsAppBatch = DefaultTopicWhatsAppBatch
	}
	if t.DLQ == "" {
		t.DLQ = DefaultTopicDLQ
	}
	if t.Retry == "" {
		t.Retry = DefaultTopicRetry
	}
	return t
}
