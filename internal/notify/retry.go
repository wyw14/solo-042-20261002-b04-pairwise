package notify

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

type RetryItem struct {
	Alarm       model.Alarm
	NextAttempt time.Time
	Attempts    int
}

// AttemptFunc performs a single redelivery attempt for a queued alarm and
// returns the updated alarm. A nil error removes the alarm from the queue;
// a non-nil error reschedules it with the updated alarm kept, so channels
// that already succeeded are not sent again on the next attempt.
type AttemptFunc func(context.Context, model.Alarm) (model.Alarm, error)

type RetryQueue struct {
	mu       sync.Mutex
	items    map[uuid.UUID]RetryItem
	interval time.Duration
	now      func() time.Time
}

func NewRetryQueue(interval time.Duration, now func() time.Time) *RetryQueue {
	if now == nil {
		now = time.Now
	}
	return &RetryQueue{items: make(map[uuid.UUID]RetryItem), interval: interval, now: now}
}

func (q *RetryQueue) Schedule(alarm model.Alarm) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items[alarm.ID] = RetryItem{Alarm: alarm, NextAttempt: q.now().Add(q.interval).UTC()}
}

// Cancel drops a pending retry, e.g. when the alarm was acknowledged or
// cleared while waiting for its next attempt.
func (q *RetryQueue) Cancel(id uuid.UUID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.items, id)
}

func (q *RetryQueue) RunDue(ctx context.Context, attempt AttemptFunc) []error {
	q.mu.Lock()
	due := make([]RetryItem, 0)
	for _, item := range q.items {
		if !q.now().Before(item.NextAttempt) {
			due = append(due, item)
		}
	}
	q.mu.Unlock()
	errorsFound := make([]error, 0)
	for _, item := range due {
		updated, err := attempt(ctx, item.Alarm)
		if updated.ID != uuid.Nil {
			item.Alarm = updated
		}
		q.mu.Lock()
		if err == nil {
			delete(q.items, item.Alarm.ID)
		} else {
			item.Attempts++
			item.NextAttempt = q.now().Add(q.interval).UTC()
			q.items[item.Alarm.ID] = item
			errorsFound = append(errorsFound, err)
		}
		q.mu.Unlock()
	}
	return errorsFound
}

func (q *RetryQueue) Pending() []RetryItem {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]RetryItem, 0, len(q.items))
	for _, item := range q.items {
		result = append(result, item)
	}
	return result
}
