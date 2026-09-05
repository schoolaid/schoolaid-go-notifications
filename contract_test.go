package notifications

import (
	"context"
	"encoding/json"
	"strings"
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

// ⚠️ THE DEFAULT DIRECTION IS THE WHOLE POINT OF THE POINTER.
//
// Legacy persists unless the producer sent literal "0", so an absent value has
// always meant "keep it". A plain bool would invert that: Go's zero value is
// false, so every producer that forgot the field would silently stop being
// persisted — no error, no log, just a user whose history quietly empties.
// This pins the safe direction so a "simplification" to a plain bool fails
// here rather than in production months later.
func TestShouldPersistDefaultsToYesWhenUnset(t *testing.T) {
	if !(Note{}).ShouldPersist() {
		t.Fatal("an unset Store must mean PERSIST — mirroring legacy's `store !== \"0\"`. " +
			"Defaulting to false makes a forgotten field indistinguishable from a " +
			"deliberate suppression, and silently empties history")
	}
	if !(Note{Store: PersistFlag(true)}).ShouldPersist() {
		t.Error("explicit true must persist")
	}
	// The case that must be able to say no — otherwise the field is decoration.
	if (Note{Store: PersistFlag(false)}).ShouldPersist() {
		t.Error("explicit false must NOT persist, or suppression is impossible " +
			"and the flag cannot express what legacy's store=0 expressed")
	}
}

// Store and Channels are independent axes: deliver-without-remembering and
// remember-without-delivering must both be expressible.
func TestStoreAndChannelsAreIndependent(t *testing.T) {
	deliverNotRemember := Note{Channels: []string{"push"}, Store: PersistFlag(false)}
	if deliverNotRemember.IsDeliveryless() || deliverNotRemember.ShouldPersist() {
		t.Error("deliver-but-do-not-remember must be expressible")
	}

	// positions-api's case: its own fanout delivers, the event exists only for history.
	rememberNotDeliver := Note{Channels: []string{}}
	if !rememberNotDeliver.IsDeliveryless() || !rememberNotDeliver.ShouldPersist() {
		t.Error("remember-but-do-not-deliver must be expressible — this is exactly " +
			"what positions-api emits")
	}
}

// The field must survive the wire, and an unset Store must not serialise as
// `"store":false` — that would turn "unset" into an explicit suppression at
// the first hop.
func TestStoreOmittedWhenUnset(t *testing.T) {
	b, err := json.Marshal(Note{Title: "t"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(b), "\"store\"") {
		t.Errorf("unset Store must be OMITTED, not emitted as false — a consumer "+
			"reading explicit false would suppress history the producer never "+
			"asked to suppress. got %s", b)
	}

	b2, _ := json.Marshal(Note{Title: "t", Store: PersistFlag(false)})
	if !strings.Contains(string(b2), "\"store\":false") {
		t.Errorf("an explicit false must reach the wire, got %s", b2)
	}
}
