package alarm

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

type Service struct {
	mu       sync.RWMutex
	alarms   map[uuid.UUID]model.Alarm
	delivery *DeliveryService
	now      func() time.Time
}

func NewService(delivery *DeliveryService, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{alarms: make(map[uuid.UUID]model.Alarm), delivery: delivery, now: now}
}

func (s *Service) Raise(ctx context.Context, vesselID string, kind model.AlarmKind, message string) (model.Alarm, error) {
	alarm, err := model.NewAlarm(vesselID, kind, message, s.now())
	if err != nil {
		return model.Alarm{}, err
	}
	s.mu.Lock()
	s.alarms[alarm.ID] = alarm
	s.mu.Unlock()
	delivered, err := s.delivery.Deliver(ctx, alarm)
	s.mu.Lock()
	if delivered.ID != uuid.Nil {
		s.alarms[alarm.ID] = delivered
	}
	s.mu.Unlock()
	return delivered, err
}

func (s *Service) Restore(alarm model.Alarm) {
	s.mu.Lock()
	s.alarms[alarm.ID] = alarm
	s.mu.Unlock()
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

// RetryDelivery performs one scheduled redelivery attempt and closes the
// loop: the merged outcome is stored in memory and journaled. Alarms that
// were acknowledged or cleared while queued are left untouched and returned
// with a nil error so the caller drops them from the retry queue.
func (s *Service) RetryDelivery(ctx context.Context, queued model.Alarm) (model.Alarm, error) {
	s.mu.RLock()
	current, ok := s.alarms[queued.ID]
	s.mu.RUnlock()
	if ok && (current.State == model.AlarmAcknowledged || current.State == model.AlarmCleared) {
		return current, nil
	}
	redelivered, dispatchErr := s.delivery.Redeliver(ctx, queued)
	s.mu.Lock()
	if latest, exists := s.alarms[redelivered.ID]; exists &&
		(latest.State == model.AlarmAcknowledged || latest.State == model.AlarmCleared) {
		// Acknowledged or cleared while the attempt was in flight: keep the
		// operator-visible state, retain the fresh channel results.
		redelivered.State = latest.State
	}
	s.alarms[redelivered.ID] = redelivered
	s.mu.Unlock()
	if err := s.delivery.Record(redelivered); err != nil {
		return redelivered, err
	}
	return redelivered, dispatchErr
}

func (s *Service) Acknowledge(id uuid.UUID) (model.Alarm, error) {
	s.mu.Lock()
	alarm, ok := s.alarms[id]
	if !ok {
		s.mu.Unlock()
		return model.Alarm{}, errors.New("alarm not found")
	}
	alarm = alarm.Acknowledge(s.now())
	s.alarms[id] = alarm
	s.mu.Unlock()
	s.delivery.CancelRetries(id)
	return alarm, nil
}

func (s *Service) Clear(id uuid.UUID) (model.Alarm, error) {
	s.mu.Lock()
	alarm, ok := s.alarms[id]
	if !ok {
		s.mu.Unlock()
		return model.Alarm{}, errors.New("alarm not found")
	}
	alarm = alarm.Clear(s.now())
	s.alarms[id] = alarm
	s.mu.Unlock()
	s.delivery.CancelRetries(id)
	return alarm, nil
}
