package notifications

import (
	"context"
	"testing"
)

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

// ⚠️ An empty device list must be REFUSED, not published.
//
// The dangerous reading of `devices: []` is "everything in SchoolID", which
// would turn a producer bug into a school-wide bus shutdown. Unlike Channels,
// empty has no legitimate meaning here, so it is rejected at the source rather
// than left for each consumer to guess. Caught by qa-1 before v0.4.0.
func TestPublishPushCommandRefusesEmptyDevices(t *testing.T) {
	c := NewClientWithProducer(&recordingProducer{}, Topics{})

	for name, devices := range map[string][]string{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			err := c.PublishPushCommand(context.Background(), PushCommand{
				SchoolID: 3, Action: ActionBusOff, Devices: devices,
			})
			if err == nil {
				t.Fatal("publishing a command with no devices must fail — an empty list " +
					"risks being read downstream as a school-wide broadcast")
			}
		})
	}
}

// The control: a real device list must still publish, or the guard has simply
// broken the feature.
func TestPublishPushCommandAcceptsRealDevices(t *testing.T) {
	rec := &recordingProducer{}
	c := NewClientWithProducer(rec, Topics{})
	if err := c.PublishPushCommand(context.Background(), PushCommand{
		SchoolID: 3, Action: ActionBusOff, Devices: []string{"tok-1"},
	}); err != nil {
		t.Fatalf("a command with devices must publish, got %v", err)
	}
	if rec.topic != DefaultTopicPushCommand {
		t.Errorf("published to %q, want %q", rec.topic, DefaultTopicPushCommand)
	}
}

// Channels{""} has length 1, so it is NOT delivery-less — documented, and
// pinned so the predicate's len-based nature is not mistaken for validation.
func TestBlankChannelValueIsNotTreatedAsPersistOnly(t *testing.T) {
	if (Note{Channels: []string{""}}).IsDeliveryless() {
		t.Error(`Channels{""} has length 1 and must NOT read as persist-only; ` +
			"channel-name validation is the consumer's job, not this predicate's")
	}
}

// recordingProducer captures what would have been published.
type recordingProducer struct {
	topic string
	key   string
}

func (r *recordingProducer) Produce(ctx context.Context, topic, key string, payload any, headers map[string]string) error {
	r.topic, r.key = topic, key
	return nil
}
func (r *recordingProducer) Close() error { return nil }
