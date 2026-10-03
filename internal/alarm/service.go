package alarm

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
)

// CancelQueue drops pending notification retries for an alarm.
type CancelQueue interface {
	Cancel(alarmID uuid.UUID) bool
}

type Service struct {
	mu        sync.RWMutex
	alarms    map[uuid.UUID]model.Alarm
	revisions map[uuid.UUID]uint64
	delivery  *DeliveryService
	retries   CancelQueue
	journal   *journal.Store
	now       func() time.Time
}

func NewService(delivery *DeliveryService, store *journal.Store, now func() time.Time) (*Service, error) {
	if delivery == nil || store == nil {
		return nil, errors.New("alarm service dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &Service{
		alarms:    make(map[uuid.UUID]model.Alarm),
		revisions: make(map[uuid.UUID]uint64),
		delivery:  delivery,
		journal:   store,
		now:       now,
	}, nil
}

// SetRetryQueue wires the retry queue so acknowledge/clear can cancel pending
// deliveries and recovery can reschedule unfinished ones.
func (s *Service) SetRetryQueue(retries CancelQueue) {
	s.mu.Lock()
	s.retries = retries
	s.mu.Unlock()
}

func (s *Service) Raise(ctx context.Context, vesselID string, kind model.AlarmKind, message string) (model.Alarm, error) {
	alarm, err := model.NewAlarm(vesselID, kind, message, s.now())
	if err != nil {
		return model.Alarm{}, err
	}
	s.mu.Lock()
	s.alarms[alarm.ID] = alarm
	s.revisions[alarm.ID]++
	s.mu.Unlock()
	delivered, err := s.delivery.Deliver(ctx, alarm)
	s.mu.Lock()
	s.alarms[alarm.ID] = delivered
	s.revisions[alarm.ID]++
	s.mu.Unlock()
	return delivered, err
}

func (s *Service) Restore(alarm model.Alarm) {
	s.mu.Lock()
	s.alarms[alarm.ID] = alarm
	s.revisions[alarm.ID]++
	s.mu.Unlock()
}

// store applies an alarm state change and bumps its revision; callers hold s.mu.
func (s *Service) store(alarm model.Alarm) {
	s.alarms[alarm.ID] = alarm
	s.revisions[alarm.ID]++
}

func (s *Service) Get(id uuid.UUID) (model.Alarm, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	alarm, ok := s.alarms[id]
	return alarm, ok
}

func (s *Service) List() []model.Alarm {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]model.Alarm, 0, len(s.alarms))
	for _, alarm := range s.alarms {
		result = append(result, alarm)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

func (s *Service) Acknowledge(id uuid.UUID) (model.Alarm, error) {
	return s.resolve(id, model.EventAlarmAcknowledged, func(alarm model.Alarm, now time.Time) model.Alarm {
		return alarm.Acknowledge(now)
	})
}

func (s *Service) Clear(id uuid.UUID) (model.Alarm, error) {
	return s.resolve(id, model.EventAlarmCleared, func(alarm model.Alarm, now time.Time) model.Alarm {
		return alarm.Clear(now)
	})
}

func (s *Service) resolve(id uuid.UUID, eventKind string, transition func(model.Alarm, time.Time) model.Alarm) (model.Alarm, error) {
	s.mu.Lock()
	current, ok := s.alarms[id]
	if !ok {
		s.mu.Unlock()
		return model.Alarm{}, errors.New("alarm not found")
	}
	// Idempotent: an already-resolved alarm keeps its first terminal state.
	if current.Resolved() {
		s.mu.Unlock()
		return current, nil
	}
	updated := transition(current, s.now())
	s.store(updated)
	rev := s.revisions[id]
	s.mu.Unlock()

	if err := s.appendAlarmEvent(eventKind, updated); err != nil {
		// The terminal transition could not be journaled; roll the in-memory
		// state back if nothing else changed it meanwhile, and leave retries
		// intact so delivery can still complete.
		s.mu.Lock()
		if s.revisions[id] == rev {
			s.store(current)
		}
		s.mu.Unlock()
		return model.Alarm{}, err
	}

	s.mu.Lock()
	if s.retries != nil {
		s.retries.Cancel(id)
	}
	s.mu.Unlock()
	return updated, nil
}

// ApplyRetry is the retry-queue sink. It folds a retry outcome into the
// authoritative alarm state and appends it to the event log. The boolean
// reports whether the queue item is finished (no further retry needed).
func (s *Service) ApplyRetry(queued model.Alarm, results []model.ChannelResult, sendErr error) (bool, error) {
	s.mu.Lock()
	current, ok := s.alarms[queued.ID]
	if !ok {
		// The alarm is gone; nothing to persist or resend.
		s.mu.Unlock()
		return true, nil
	}
	resolved := current.Resolved()
	updated := current.WithChannels(results, s.now())
	if resolved {
		// The alarm was acknowledged/cleared while the retry was in flight.
		// Keep the newer per-channel delivery record for an accurate audit
		// trail but never move the user-visible state back to partial/delivered.
		updated.State = current.State
	}
	s.store(updated)
	rev := s.revisions[queued.ID]
	s.mu.Unlock()

	if err := s.appendAlarmEvent(model.EventAlarmDeliveryRetry, updated); err != nil {
		// The outcome could not be journaled. Unless the alarm is terminal,
		// stay queued: the next run resends nothing (delivered channels are
		// skipped) and retries the journal append instead.
		if resolved {
			return true, nil
		}
		s.mu.Lock()
		if s.revisions[queued.ID] == rev {
			// Best-effort revert so memory does not outrun the durable log.
			s.store(current)
		}
		s.mu.Unlock()
		return false, err
	}

	// A resolved alarm never retries again, even if this send failed.
	if resolved {
		return true, nil
	}
	return sendErr == nil, nil
}

// ReschedulePending re-arms retries for alarms restored from the journal whose
// delivery never completed. It must be called after Restore and SetRetryQueue.
func (s *Service) ReschedulePending(scheduler RetryScheduler) {
	if scheduler == nil {
		return
	}
	s.mu.RLock()
	for _, alarm := range s.alarms {
		if alarm.State == model.AlarmPartial || alarm.State == model.AlarmDispatching {
			scheduler.Schedule(alarm)
		}
	}
	s.mu.RUnlock()
}

func (s *Service) appendAlarmEvent(kind string, alarm model.Alarm) error {
	event, err := model.NewEvent("alarms", kind, alarm.ID.String(), alarm, s.now())
	if err != nil {
		return err
	}
	_, err = s.journal.Append(event)
	return err
}
