package notifications

import "testing"

// The double-fan-out guard, as a test rather than a comment.
//
// ⚠️ A fan-out consumer that ignores Channels double-sends: once from a
// producer's own fanout, once from the note event. This pins the rule both
// sides build against.
func TestIsDeliverylessDistinguishesPersistOnlyFromDelivery(t *testing.T) {
	if !(Note{Channels: nil}).IsDeliveryless() {
		t.Error("nil Channels must be delivery-less — a persist-only event would be double-sent")
	}
	if !(Note{Channels: []string{}}).IsDeliveryless() {
		t.Error("empty Channels must be delivery-less")
	}
	// The case that must NOT be delivery-less: admin always sends at least this.
	if (Note{Channels: []string{"push"}}).IsDeliveryless() {
		t.Error(`["push"] must be delivered — schoolaid-admin's deliveryChannels() ` +
			"always returns at least this, so treating it as persist-only would " +
			"silently stop every admin notification")
	}
	if (Note{Channels: []string{"push", "email", "whatsapp"}}).IsDeliveryless() {
		t.Error("multi-channel notes must be delivered")
	}
}

// A command must never be routed to the topic the persistence consumer reads.
func TestPushCommandHasItsOwnTopic(t *testing.T) {
	tp := Topics{}.withDefaults()

	if tp.PushCommand != "notifications.push.command" {
		t.Errorf("unexpected command topic %q", tp.PushCommand)
	}
	if tp.PushCommand == tp.PushBatch {
		t.Fatal("command topic must differ from the push batch topic — sharing it " +
			"reintroduces the absence-based discrimination this split exists to remove")
	}
	for _, historyTopic := range []string{tp.NoteCreated, tp.NoteCreatedPriority} {
		if tp.PushCommand == historyTopic {
			t.Fatalf("command topic collides with a history topic (%q)", historyTopic)
		}
	}
}

// An explicit override must still win, or per-environment topic naming breaks.
func TestPushCommandTopicIsOverridable(t *testing.T) {
	tp := Topics{PushCommand: "custom.command"}.withDefaults()
	if tp.PushCommand != "custom.command" {
		t.Errorf("override ignored, got %q", tp.PushCommand)
	}
}
