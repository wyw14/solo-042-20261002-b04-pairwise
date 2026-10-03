package alarm_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/wyw14/cry-98/internal/alarm"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/notify"
)

type stubChannel struct {
	name     string
	required bool

	mu     sync.Mutex
	sends  int
	err    error
	onSend func()
}

func (c *stubChannel) Name() string   { return c.name }
func (c *stubChannel) Required() bool { return c.required }

func (c *stubChannel) Send(context.Context, model.Alarm) error {
	c.mu.Lock()
	c.sends++
	err := c.err
	onSend := c.onSend
	c.mu.Unlock()
	if onSend != nil {
		onSend()
	}
	return err
}

func (c *stubChannel) setErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = err
}

func (c *stubChannel) sendCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sends
}

func newTestStack(t *testing.T, channels ...*stubChannel) (*alarm.Service, *notify.RetryQueue, *journal.Store) {
	t.Helper()
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("journal store: %v", err)
	}
	wrapped := make([]notify.Channel, len(channels))
	for index, channel := range channels {
		wrapped[index] = channel
	}
	dispatcher, err := notify.NewDispatcher(nil, wrapped...)
	if err != nil {
		t.Fatalf("dispatcher: %v", err)
	}
	queue := notify.NewRetryQueue(0, nil)
	delivery, err := alarm.NewDeliveryService(dispatcher, queue, store, nil)
	if err != nil {
		t.Fatalf("delivery service: %v", err)
	}
	return alarm.NewService(delivery, nil), queue, store
}

func lastDeliveryState(t *testing.T, store *journal.Store) model.Alarm {
	t.Helper()
	events, err := store.Events("alarms", 0)
	if err != nil {
		t.Fatalf("journal events: %v", err)
	}
	var logged model.Alarm
	found := false
	for _, event := range events {
		if event.Kind != "alarm.delivery" {
			continue
		}
		if err := json.Unmarshal(event.Payload, &logged); err != nil {
			t.Fatalf("decode delivery event: %v", err)
		}
		found = true
	}
	if !found {
		t.Fatal("no alarm.delivery events recorded")
	}
	return logged
}

func TestRetryDeliveryClosesLoop(t *testing.T) {
	pager := &stubChannel{name: "pager", required: true, err: errors.New("pager offline")}
	email := &stubChannel{name: "email", required: false}
	service, queue, store := newTestStack(t, pager, email)

	raised, err := service.Raise(context.Background(), "V-03", model.AlarmLeak, "nitrogen leak")
	if err == nil {
		t.Fatal("expected partial delivery error")
	}
	if raised.State != model.AlarmPartial {
		t.Fatalf("state = %s, want %s", raised.State, model.AlarmPartial)
	}

	pager.setErr(nil)
	if errs := queue.RunDue(context.Background(), service.RetryDelivery); len(errs) != 0 {
		t.Fatalf("retry errors: %v", errs)
	}

	got, ok := service.Get(raised.ID)
	if !ok {
		t.Fatal("alarm missing from service")
	}
	if got.State != model.AlarmDelivered {
		t.Fatalf("queried state = %s, want %s", got.State, model.AlarmDelivered)
	}
	if pager.sendCount() != 2 {
		t.Fatalf("pager sends = %d, want 2", pager.sendCount())
	}
	if email.sendCount() != 1 {
		t.Fatalf("email sends = %d, want 1 (delivered channels must not be resent)", email.sendCount())
	}
	if pending := queue.Pending(); len(pending) != 0 {
		t.Fatalf("retry queue not drained: %d pending", len(pending))
	}

	logged := lastDeliveryState(t, store)
	if logged.State != model.AlarmDelivered {
		t.Fatalf("journaled state = %s, want %s", logged.State, model.AlarmDelivered)
	}

	restored := alarm.NewService(nil, nil)
	if err := alarm.NewRecovery(store, restored).Restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	recoverable, ok := restored.Get(raised.ID)
	if !ok {
		t.Fatal("alarm missing after recovery")
	}
	if recoverable.State != model.AlarmDelivered {
		t.Fatalf("recovered state = %s, want %s", recoverable.State, model.AlarmDelivered)
	}
}

func TestAcknowledgeCancelsPendingRetry(t *testing.T) {
	pager := &stubChannel{name: "pager", required: true, err: errors.New("pager offline")}
	service, queue, _ := newTestStack(t, pager)

	raised, _ := service.Raise(context.Background(), "V-03", model.AlarmLeak, "nitrogen leak")
	if len(queue.Pending()) != 1 {
		t.Fatal("expected a pending retry")
	}
	if _, err := service.Acknowledge(raised.ID); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}
	if pending := queue.Pending(); len(pending) != 0 {
		t.Fatalf("acknowledged alarm still has %d pending retries", len(pending))
	}

	pager.setErr(nil)
	if errs := queue.RunDue(context.Background(), service.RetryDelivery); len(errs) != 0 {
		t.Fatalf("retry errors: %v", errs)
	}
	if pager.sendCount() != 1 {
		t.Fatalf("pager sends = %d, want 1 (acknowledged alarm must not be re-notified)", pager.sendCount())
	}
	got, _ := service.Get(raised.ID)
	if got.State != model.AlarmAcknowledged {
		t.Fatalf("state = %s, want %s", got.State, model.AlarmAcknowledged)
	}
}

func TestClearCancelsPendingRetry(t *testing.T) {
	pager := &stubChannel{name: "pager", required: true, err: errors.New("pager offline")}
	service, queue, _ := newTestStack(t, pager)

	raised, _ := service.Raise(context.Background(), "V-03", model.AlarmLeak, "nitrogen leak")
	if _, err := service.Clear(raised.ID); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if pending := queue.Pending(); len(pending) != 0 {
		t.Fatalf("cleared alarm still has %d pending retries", len(pending))
	}

	pager.setErr(nil)
	if errs := queue.RunDue(context.Background(), service.RetryDelivery); len(errs) != 0 {
		t.Fatalf("retry errors: %v", errs)
	}
	if pager.sendCount() != 1 {
		t.Fatalf("pager sends = %d, want 1 (cleared alarm must not be re-notified)", pager.sendCount())
	}
	got, _ := service.Get(raised.ID)
	if got.State != model.AlarmCleared {
		t.Fatalf("state = %s, want %s", got.State, model.AlarmCleared)
	}
}

func TestRetryPreservesConcurrentAcknowledge(t *testing.T) {
	pager := &stubChannel{name: "pager", required: true, err: errors.New("pager offline")}
	service, queue, store := newTestStack(t, pager)

	raised, _ := service.Raise(context.Background(), "V-03", model.AlarmLeak, "nitrogen leak")

	// Acknowledge lands while the retry attempt is in flight.
	pager.setErr(nil)
	pager.onSend = func() {
		_, _ = service.Acknowledge(raised.ID)
	}
	if errs := queue.RunDue(context.Background(), service.RetryDelivery); len(errs) != 0 {
		t.Fatalf("retry errors: %v", errs)
	}

	got, _ := service.Get(raised.ID)
	if got.State != model.AlarmAcknowledged {
		t.Fatalf("state = %s, want %s (retry must not clobber acknowledgement)", got.State, model.AlarmAcknowledged)
	}
	if len(got.Channels) != 1 || !got.Channels[0].Delivered {
		t.Fatalf("fresh channel results lost: %+v", got.Channels)
	}
	logged := lastDeliveryState(t, store)
	if logged.State != model.AlarmAcknowledged {
		t.Fatalf("journaled state = %s, want %s", logged.State, model.AlarmAcknowledged)
	}
}

func TestFailedRetryKeepsPartialStateAndReschedules(t *testing.T) {
	pager := &stubChannel{name: "pager", required: true, err: errors.New("pager offline")}
	service, queue, store := newTestStack(t, pager)

	raised, _ := service.Raise(context.Background(), "V-03", model.AlarmLeak, "nitrogen leak")
	if errs := queue.RunDue(context.Background(), service.RetryDelivery); len(errs) != 1 {
		t.Fatalf("expected one retry error, got %v", errs)
	}

	got, _ := service.Get(raised.ID)
	if got.State != model.AlarmPartial {
		t.Fatalf("state = %s, want %s", got.State, model.AlarmPartial)
	}
	pending := queue.Pending()
	if len(pending) != 1 {
		t.Fatalf("pending = %d, want 1", len(pending))
	}
	if pending[0].Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", pending[0].Attempts)
	}
	logged := lastDeliveryState(t, store)
	if logged.State != model.AlarmPartial {
		t.Fatalf("journaled state = %s, want %s", logged.State, model.AlarmPartial)
	}
}
