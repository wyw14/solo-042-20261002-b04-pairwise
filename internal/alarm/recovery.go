package alarm

import (
	"encoding/json"

	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
)

type Recovery struct {
	store   *journal.Store
	service *Service
}

func NewRecovery(store *journal.Store, service *Service) *Recovery {
	return &Recovery{store: store, service: service}
}

func (r *Recovery) Restore() error {
	events, err := r.store.Events("alarms", 0)
	if err != nil {
		return err
	}
	// Fold the event stream in order: later retry/acknowledge/clear events
	// override the initial delivery snapshot, so a successful retry survives a
	// restart and a resolved alarm is not rescheduled.
	alarmKinds := map[string]bool{
		model.EventAlarmDelivery:      true,
		model.EventAlarmDeliveryRetry: true,
		model.EventAlarmAcknowledged:  true,
		model.EventAlarmCleared:       true,
	}
	latest := make(map[string]model.Alarm)
	for _, event := range events {
		if !alarmKinds[event.Kind] {
			continue
		}
		var alarm model.Alarm
		if err := json.Unmarshal(event.Payload, &alarm); err != nil {
			return err
		}
		latest[alarm.ID.String()] = alarm
	}
	for _, alarm := range latest {
		r.service.Restore(alarm)
	}
	return nil
}
