package notify

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

type scriptedChannel struct {
	name      string
	required  bool
	mu        sync.Mutex
	sends     int
	failUntil int
}

func (c *scriptedChannel) Name() string { return c.name }
func (c *scriptedChannel) Required() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.required
}

func (c *scriptedChannel) Send(_ context.Context, _ model.Alarm) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sends++
	if c.sends <= c.failUntil {
		return errors.New("channel offline")
	}
	return nil
}

func (c *scriptedChannel) sendCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sends
}

func newTestAlarm() model.Alarm {
	alarm, err := model.NewAlarm("V-01", model.AlarmLeak, "leak detected", time.Unix(1000, 0))
	if err != nil {
		panic(err)
	}
	return alarm
}

func TestRetrySkipsAlreadyDeliveredChannels(t *testing.T) {
	now := time.Unix(2000, 0)
	siren := &scriptedChannel{name: "siren", required: true}
	terminal := &scriptedChannel{name: "terminal", required: true, failUntil: 1}
	dispatcher, err := NewDispatcher(func() time.Time { return now }, siren, terminal)
	if err != nil {
		t.Fatal(err)
	}
	alarm := newTestAlarm()

	results, dispatchErr := dispatcher.Dispatch(context.Background(), alarm)
	if dispatchErr == nil {
		t.Fatal("expected first dispatch to fail")
	}

	// Retry: the siren already delivered and must not be sent again.
	retried, retryErr := dispatcher.Retry(context.Background(), alarm, results)
	if retryErr != nil {
		t.Fatalf("retry failed: %v", retryErr)
	}
	for _, result := range retried {
		if !result.Delivered {
			t.Fatalf("channel %s still not delivered", result.Channel)
		}
	}
	if got := siren.sendCount(); got != 1 {
		t.Fatalf("delivered channel resent %d times", got)
	}
	if got := terminal.sendCount(); got != 2 {
		t.Fatalf("failing channel send count=%d, want 2", got)
	}
}

func TestRetryTreatsOptionalFailureAsTerminal(t *testing.T) {
	now := time.Unix(2000, 0)
	siren := &scriptedChannel{name: "siren", required: true}
	optional := &scriptedChannel{name: "digest", required: false, failUntil: 10}
	dispatcher, err := NewDispatcher(func() time.Time { return now }, siren, optional)
	if err != nil {
		t.Fatal(err)
	}
	alarm := newTestAlarm()

	results, dispatchErr := dispatcher.Dispatch(context.Background(), alarm)
	if dispatchErr != nil {
		t.Fatalf("optional failure should not fail dispatch: %v", dispatchErr)
	}
	if _, retryErr := dispatcher.Retry(context.Background(), alarm, results); retryErr != nil {
		t.Fatalf("optional failure should not keep retrying: %v", retryErr)
	}
}
