package notify_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/notify"
)

func TestRunDueReschedulesFailedAttempts(t *testing.T) {
	now := time.Now()
	current := now
	queue := notify.NewRetryQueue(time.Minute, func() time.Time { return current })
	alarmValue := model.Alarm{ID: uuid.New(), State: model.AlarmPartial}
	queue.Schedule(alarmValue)

	attempts := 0
	attempt := func(context.Context, model.Alarm) (model.Alarm, error) {
		attempts++
		return model.Alarm{}, errors.New("channel offline")
	}
	if errs := queue.RunDue(context.Background(), attempt); len(errs) != 0 || attempts != 0 {
		t.Fatalf("item ran before its next attempt was due: errs=%v attempts=%d", errs, attempts)
	}

	current = current.Add(2 * time.Minute)
	if errs := queue.RunDue(context.Background(), attempt); len(errs) != 1 || attempts != 1 {
		t.Fatalf("expected one failed attempt, got errs=%v attempts=%d", errs, attempts)
	}
	pending := queue.Pending()
	if len(pending) != 1 || pending[0].Attempts != 1 {
		t.Fatalf("pending = %+v, want one item with one attempt", pending)
	}
}

func TestRunDueKeepsMergedAlarmForNextAttempt(t *testing.T) {
	queue := notify.NewRetryQueue(0, nil)
	alarmValue := model.Alarm{ID: uuid.New(), State: model.AlarmPartial}
	queue.Schedule(alarmValue)

	merged := alarmValue
	merged.Channels = []model.ChannelResult{{Channel: "pager", Required: true, Delivered: true}}
	if errs := queue.RunDue(context.Background(), func(context.Context, model.Alarm) (model.Alarm, error) {
		return merged, errors.New("email still failing")
	}); len(errs) != 1 {
		t.Fatalf("expected one retry error, got %v", errs)
	}

	var seen model.Alarm
	if errs := queue.RunDue(context.Background(), func(_ context.Context, alarm model.Alarm) (model.Alarm, error) {
		seen = alarm
		return alarm, nil
	}); len(errs) != 0 {
		t.Fatalf("retry errors: %v", errs)
	}
	if len(seen.Channels) != 1 || !seen.Channels[0].Delivered {
		t.Fatalf("merged channels lost between attempts: %+v", seen.Channels)
	}
	if pending := queue.Pending(); len(pending) != 0 {
		t.Fatalf("queue not drained: %d pending", len(pending))
	}
}

func TestCancelDropsPendingRetry(t *testing.T) {
	queue := notify.NewRetryQueue(0, nil)
	alarmValue := model.Alarm{ID: uuid.New(), State: model.AlarmPartial}
	queue.Schedule(alarmValue)
	queue.Cancel(alarmValue.ID)
	if pending := queue.Pending(); len(pending) != 0 {
		t.Fatalf("pending = %d, want 0", len(pending))
	}

	attempts := 0
	if errs := queue.RunDue(context.Background(), func(_ context.Context, alarm model.Alarm) (model.Alarm, error) {
		attempts++
		return alarm, nil
	}); len(errs) != 0 || attempts != 0 {
		t.Fatalf("cancelled item ran: errs=%v attempts=%d", errs, attempts)
	}
}
