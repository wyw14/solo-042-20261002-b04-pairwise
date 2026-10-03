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

// RetrySink is the authoritative owner of alarm state. The queue hands every
// retry outcome to it outside the queue lock so the owner can persist the
// result without risking a lock cycle. Returning true removes the item from
// the queue; returning false keeps it scheduled for another attempt.
type RetrySink interface {
	ApplyRetry(queued model.Alarm, results []model.ChannelResult, sendErr error) (bool, error)
}

type RetryQueue struct {
	mu       sync.Mutex
	items    map[uuid.UUID]RetryItem
	interval time.Duration
	now      func() time.Time
	sink     RetrySink
}

func NewRetryQueue(interval time.Duration, now func() time.Time) *RetryQueue {
	if now == nil {
		now = time.Now
	}
	return &RetryQueue{items: make(map[uuid.UUID]RetryItem), interval: interval, now: now}
}

// SetSink wires the owner that persists retry outcomes.
func (q *RetryQueue) SetSink(sink RetrySink) {
	q.mu.Lock()
	q.sink = sink
	q.mu.Unlock()
}

func (q *RetryQueue) Schedule(alarm model.Alarm) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Refresh an existing item's alarm snapshot but preserve its backoff.
	if existing, ok := q.items[alarm.ID]; ok {
		existing.Alarm = alarm
		q.items[alarm.ID] = existing
		return
	}
	q.items[alarm.ID] = RetryItem{Alarm: alarm, NextAttempt: q.now().Add(q.interval).UTC()}
}

// Cancel drops any pending retry for an alarm, e.g. after it was acknowledged
// or cleared. It reports whether an item was removed.
func (q *RetryQueue) Cancel(alarmID uuid.UUID) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.items[alarmID]; !ok {
		return false
	}
	delete(q.items, alarmID)
	return true
}

func (q *RetryQueue) RunDue(ctx context.Context, dispatcher *Dispatcher) []error {
	q.mu.Lock()
	due := make([]RetryItem, 0)
	for _, item := range q.items {
		if !q.now().Before(item.NextAttempt) {
			due = append(due, item)
		}
	}
	sink := q.sink
	q.mu.Unlock()

	errorsFound := make([]error, 0)
	for _, item := range due {
		// Retry only channels that have not delivered yet; the dispatcher
		// additionally skips every result already marked Delivered.
		results, err := dispatcher.Retry(ctx, item.Alarm, item.Alarm.Channels)
		attempted := q.now().UTC()
		item.Alarm = item.Alarm.WithChannels(results, attempted)

		// Invoke the sink without holding the queue lock: the sink may call
		// back into Cancel (acknowledge/clear), which must not deadlock here.
		remove := err == nil
		persisted := true
		if sink != nil {
			var applyErr error
			remove, applyErr = sink.ApplyRetry(item.Alarm, results, err)
			if applyErr != nil {
				// The outcome could not be persisted; stay queued so the
				// attempt is repeated. Already-delivered channels stay marked
				// in the results and will not be resent.
				remove = false
				persisted = false
				errorsFound = append(errorsFound, applyErr)
			}
		}

		q.mu.Lock()
		if remove {
			// Delete unconditionally: Cancel may already have removed the item
			// while this retry was in flight.
			delete(q.items, item.Alarm.ID)
		} else {
			item.Attempts++
			item.NextAttempt = attempted.Add(q.interval)
			q.items[item.Alarm.ID] = item
			if err != nil && persisted {
				errorsFound = append(errorsFound, err)
			}
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
