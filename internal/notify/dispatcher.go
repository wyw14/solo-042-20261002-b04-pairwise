package notify

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

type Dispatcher struct {
	mu       sync.RWMutex
	channels map[string]Channel
	now      func() time.Time
}

func NewDispatcher(now func() time.Time, channels ...Channel) (*Dispatcher, error) {
	if now == nil {
		now = time.Now
	}
	dispatcher := &Dispatcher{channels: make(map[string]Channel), now: now}
	for _, channel := range channels {
		if channel == nil || channel.Name() == "" {
			return nil, errors.New("invalid notification channel")
		}
		if _, exists := dispatcher.channels[channel.Name()]; exists {
			return nil, errors.New("duplicate notification channel")
		}
		dispatcher.channels[channel.Name()] = channel
	}
	return dispatcher, nil
}

func (d *Dispatcher) Dispatch(ctx context.Context, alarm model.Alarm) ([]model.ChannelResult, error) {
	d.mu.RLock()
	channels := make([]Channel, 0, len(d.channels))
	for _, channel := range d.channels {
		channels = append(channels, channel)
	}
	d.mu.RUnlock()
	if len(channels) == 0 {
		return nil, errors.New("no notification channels configured")
	}
	results := make([]model.ChannelResult, len(channels))
	var wait sync.WaitGroup
	for index, channel := range channels {
		wait.Add(1)
		go func(index int, channel Channel) {
			defer wait.Done()
			err := channel.Send(ctx, alarm)
			result := model.ChannelResult{
				Channel:   channel.Name(),
				Required:  channel.Required(),
				Delivered: err == nil,
				Attempted: d.now().UTC(),
			}
			if err != nil {
				result.Error = err.Error()
			}
			results[index] = result
		}(index, channel)
	}
	wait.Wait()
	for _, result := range results {
		if result.Required && !result.Delivered {
			return results, errors.New("one or more required notification channels failed")
		}
	}
	return results, nil
}

func (d *Dispatcher) Retry(ctx context.Context, alarm model.Alarm, previous []model.ChannelResult) ([]model.ChannelResult, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	results := append([]model.ChannelResult(nil), previous...)
	var firstErr error
	for index, previousResult := range previous {
		if previousResult.Delivered {
			// Already delivered channels are never resent.
			continue
		}
		channel := d.channels[previousResult.Channel]
		if channel == nil {
			results[index].Error = "notification channel no longer exists"
			if previousResult.Required && firstErr == nil {
				firstErr = errors.New("notification channel no longer exists")
			}
			continue
		}
		err := channel.Send(ctx, alarm)
		results[index].Attempted = d.now().UTC()
		results[index].Delivered = err == nil
		results[index].Error = ""
		if err != nil {
			results[index].Error = err.Error()
			// Only required-channel failures keep the alarm partial and the
			// retry alive; a failed optional channel is recorded but terminal.
			if channel.Required() && firstErr == nil {
				firstErr = err
			}
		}
	}
	return results, firstErr
}
