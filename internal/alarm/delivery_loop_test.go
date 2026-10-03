package alarm

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/notify"
)

type fakeChannel struct {
	name      string
	required  bool
	mu        sync.Mutex
	sends     int
	failUntil int
}

func (c *fakeChannel) Name() string { return c.name }
func (c *fakeChannel) Required() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.required
}

func (c *fakeChannel) Send(_ context.Context, _ model.Alarm) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sends++
	if c.sends <= c.failUntil {
		return errors.New("offline")
	}
	return nil
}

func (c *fakeChannel) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sends
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

type harness struct {
	store      *journal.Store
	queue      *notify.RetryQueue
	dispatcher *notify.Dispatcher
	service    *Service
	siren      *fakeChannel
	terminal   *fakeChannel
	clk        *clock
}

func newHarness(t *testing.T, dir string, terminalFailUntil int) *harness {
	t.Helper()
	clk := &clock{t: time.Unix(1000, 0)}
	store, err := journal.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	siren := &fakeChannel{name: "siren", required: true}
	terminal := &fakeChannel{name: "terminal", required: true, failUntil: terminalFailUntil}
	dispatcher, err := notify.NewDispatcher(clk.now, siren, terminal)
	if err != nil {
		t.Fatal(err)
	}
	queue := notify.NewRetryQueue(time.Minute, clk.now)
	delivery, err := NewDeliveryService(dispatcher, queue, store, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(delivery, store, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	queue.SetSink(service)
	service.SetRetryQueue(queue)
	return &harness{store: store, queue: queue, dispatcher: dispatcher, service: service,
		siren: siren, terminal: terminal, clk: clk}
}

func eventKinds(t *testing.T, store *journal.Store) []string {
	t.Helper()
	events, err := store.Events("alarms", 0)
	if err != nil {
		t.Fatal(err)
	}
	kinds := make([]string, 0, len(events))
	for _, event := range events {
		kinds = append(kinds, event.Kind)
	}
	return kinds
}

func TestSuccessfulRetryClosesLoopToStateAndJournal(t *testing.T) {
	h := newHarness(t, t.TempDir(), 1)

	alarm, err := h.service.Raise(context.Background(), "V-01", model.AlarmLeak, "leak")
	if err == nil {
		t.Fatal("expected first dispatch to fail")
	}
	if alarm.State != model.AlarmPartial {
		t.Fatalf("state=%s", alarm.State)
	}
	if pending := len(h.queue.Pending()); pending != 1 {
		t.Fatalf("pending=%d", pending)
	}

	// Second send succeeds once the retry is due.
	h.clk.t = h.clk.t.Add(2 * time.Minute)
	if errs := h.queue.RunDue(context.Background(), h.dispatcher); len(errs) != 0 {
		t.Fatalf("retry errors: %v", errs)
	}
	if pending := len(h.queue.Pending()); pending != 0 {
		t.Fatalf("queue not drained: %d", pending)
	}
	current, ok := h.service.Get(alarm.ID)
	if !ok {
		t.Fatal("alarm missing")
	}
	if current.State != model.AlarmDelivered {
		t.Fatalf("state after retry=%s, want delivered", current.State)
	}
	for _, result := range current.Channels {
		if !result.Delivered {
			t.Fatalf("channel %s not delivered", result.Channel)
		}
	}

	// The retry outcome is durable, not just in memory.
	kinds := eventKinds(t, h.store)
	last := kinds[len(kinds)-1]
	if last != model.EventAlarmDeliveryRetry {
		t.Fatalf("last event=%s, want %s", last, model.EventAlarmDeliveryRetry)
	}
	if h.siren.count() != 1 {
		t.Fatalf("delivered channel resent %d times", h.siren.count())
	}

	// Simulate a restart against the same journal.
	restarted := newHarness(t, h.store.Root(), 0)
	if err := NewRecovery(restarted.store, restarted.service).Restore(); err != nil {
		t.Fatal(err)
	}
	restarted.service.ReschedulePending(restarted.queue)
	got, ok := restarted.service.Get(alarm.ID)
	if !ok {
		t.Fatal("alarm not restored")
	}
	if got.State != model.AlarmDelivered {
		t.Fatalf("state after restart=%s, want delivered", got.State)
	}
	if pending := len(restarted.queue.Pending()); pending != 0 {
		t.Fatalf("delivered alarm rescheduled after restart: %d", pending)
	}
}

func TestAcknowledgeDuringRetryCancelsPendingSend(t *testing.T) {
	h := newHarness(t, t.TempDir(), 10)
	alarm, _ := h.service.Raise(context.Background(), "V-01", model.AlarmLeak, "leak")

	ack, err := h.service.Acknowledge(alarm.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ack.State != model.AlarmAcknowledged {
		t.Fatalf("state=%s", ack.State)
	}
	if pending := len(h.queue.Pending()); pending != 0 {
		t.Fatalf("queue not cancelled: %d", pending)
	}

	// No retry goes out even after the backoff would have elapsed.
	h.clk.t = h.clk.t.Add(10 * time.Minute)
	h.queue.RunDue(context.Background(), h.dispatcher)
	if got := h.terminal.count(); got != 1 {
		t.Fatalf("cancelled channel sent %d times, want 1", got)
	}
	current, _ := h.service.Get(alarm.ID)
	if current.State != model.AlarmAcknowledged {
		t.Fatalf("state=%s", current.State)
	}

	kinds := eventKinds(t, h.store)
	foundAck := false
	for _, kind := range kinds {
		if kind == model.EventAlarmAcknowledged {
			foundAck = true
		}
	}
	if !foundAck {
		t.Fatal("acknowledge not journaled")
	}

	// Restart keeps the terminal state and does not re-arm delivery.
	restarted := newHarness(t, h.store.Root(), 10)
	if err := NewRecovery(restarted.store, restarted.service).Restore(); err != nil {
		t.Fatal(err)
	}
	restarted.service.ReschedulePending(restarted.queue)
	got, _ := restarted.service.Get(alarm.ID)
	if got.State != model.AlarmAcknowledged {
		t.Fatalf("state after restart=%s", got.State)
	}
	if pending := len(restarted.queue.Pending()); pending != 0 {
		t.Fatalf("resolved alarm rescheduled after restart: %d", pending)
	}
}

func TestRetryCompletingAfterAcknowledgeKeepsTerminalState(t *testing.T) {
	h := newHarness(t, t.TempDir(), 1)
	alarm, _ := h.service.Raise(context.Background(), "V-01", model.AlarmLeak, "leak")

	if _, err := h.service.Acknowledge(alarm.ID); err != nil {
		t.Fatal(err)
	}
	// Cancel drops the queued item; emulate a send that was already in flight
	// by delivering its successful outcome directly.
	results := []model.ChannelResult{
		{Channel: "siren", Required: true, Delivered: true, Attempted: h.clk.now()},
		{Channel: "terminal", Required: true, Delivered: true, Attempted: h.clk.now()},
	}
	remove, err := h.service.ApplyRetry(alarm, results, nil)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !remove {
		t.Fatal("resolved alarm must not stay queued")
	}
	current, _ := h.service.Get(alarm.ID)
	if current.State != model.AlarmAcknowledged {
		t.Fatalf("state regressed to %s", current.State)
	}
	for _, result := range current.Channels {
		if !result.Delivered {
			t.Fatalf("in-flight delivery not recorded for %s", result.Channel)
		}
	}

	// A failed in-flight outcome on a resolved alarm also stops retrying.
	h2 := newHarness(t, t.TempDir(), 10)
	alarm2, _ := h2.service.Raise(context.Background(), "V-01", model.AlarmLeak, "leak")
	if _, err := h2.service.Acknowledge(alarm2.ID); err != nil {
		t.Fatal(err)
	}
	failed := []model.ChannelResult{
		{Channel: "siren", Required: true, Delivered: true, Attempted: h2.clk.now()},
		{Channel: "terminal", Required: true, Delivered: false, Error: "offline", Attempted: h2.clk.now()},
	}
	remove, err = h2.service.ApplyRetry(alarm2, failed, errors.New("offline"))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !remove {
		t.Fatal("failed send on resolved alarm must not requeue")
	}
	state, _ := h2.service.Get(alarm2.ID)
	if state.State != model.AlarmAcknowledged {
		t.Fatalf("state=%s", state.State)
	}
}
