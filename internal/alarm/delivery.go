package alarm

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
)

type Dispatcher interface {
	Dispatch(context.Context, model.Alarm) ([]model.ChannelResult, error)
	Retry(context.Context, model.Alarm, []model.ChannelResult) ([]model.ChannelResult, error)
}

type RetryScheduler interface {
	Schedule(model.Alarm)
	Cancel(uuid.UUID)
}

type DeliveryService struct {
	dispatcher Dispatcher
	retries    RetryScheduler
	journal    *journal.Store
	now        func() time.Time
}

func NewDeliveryService(dispatcher Dispatcher, retries RetryScheduler, store *journal.Store, now func() time.Time) (*DeliveryService, error) {
	if dispatcher == nil || retries == nil || store == nil {
		return nil, errors.New("alarm delivery dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &DeliveryService{dispatcher: dispatcher, retries: retries, journal: store, now: now}, nil
}

func (s *DeliveryService) Deliver(ctx context.Context, alarm model.Alarm) (model.Alarm, error) {
	alarm.State = model.AlarmDispatching
	results, dispatchErr := s.dispatcher.Dispatch(ctx, alarm)
	alarm = alarm.WithChannels(results, s.now())
	if err := s.Record(alarm); err != nil {
		return model.Alarm{}, err
	}
	if dispatchErr != nil {
		s.retries.Schedule(alarm)
		return alarm, dispatchErr
	}
	return alarm, nil
}

// Redeliver re-attempts only the channels that have not been delivered yet
// and returns the alarm with the merged channel results.
func (s *DeliveryService) Redeliver(ctx context.Context, alarm model.Alarm) (model.Alarm, error) {
	results, dispatchErr := s.dispatcher.Retry(ctx, alarm, alarm.Channels)
	return alarm.WithChannels(results, s.now()), dispatchErr
}

// Record appends the delivery outcome to the alarm journal so recovery and
// queries observe the same state.
func (s *DeliveryService) Record(alarm model.Alarm) error {
	event, err := model.NewEvent("alarms", "alarm.delivery", alarm.ID.String(), alarm, s.now())
	if err != nil {
		return err
	}
	_, err = s.journal.Append(event)
	return err
}

// CancelRetries drops any pending retry for the alarm.
func (s *DeliveryService) CancelRetries(id uuid.UUID) {
	s.retries.Cancel(id)
}
