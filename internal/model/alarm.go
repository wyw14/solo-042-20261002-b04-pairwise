package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type AlarmKind string

const (
	AlarmLowLevel AlarmKind = "low_level"
	AlarmPressure AlarmKind = "over_pressure"
	AlarmLeak     AlarmKind = "nitrogen_leak"
	AlarmProbe    AlarmKind = "probe_disconnected"
)

type AlarmState string

const (
	AlarmDetected     AlarmState = "detected"
	AlarmDispatching  AlarmState = "dispatching"
	AlarmPartial      AlarmState = "partial"
	AlarmDelivered    AlarmState = "delivered"
	AlarmAcknowledged AlarmState = "acknowledged"
	AlarmCleared      AlarmState = "cleared"
)

// Alarm lifecycle event kinds persisted to the alarms journal partition.
const (
	EventAlarmDelivery      = "alarm.delivery"
	EventAlarmDeliveryRetry = "alarm.delivery.retry"
	EventAlarmAcknowledged  = "alarm.acknowledged"
	EventAlarmCleared       = "alarm.cleared"
)

type ChannelResult struct {
	Channel   string    `json:"channel"`
	Required  bool      `json:"required"`
	Delivered bool      `json:"delivered"`
	Error     string    `json:"error,omitempty"`
	Attempted time.Time `json:"attempted_at"`
}

type Alarm struct {
	ID              uuid.UUID       `json:"id"`
	VesselID        string          `json:"vessel_id"`
	Kind            AlarmKind       `json:"kind"`
	State           AlarmState      `json:"state"`
	Message         string          `json:"message"`
	Channels        []ChannelResult `json:"channels"`
	SuppressedUntil time.Time       `json:"suppressed_until,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

func NewAlarm(vesselID string, kind AlarmKind, message string, now time.Time) (Alarm, error) {
	if vesselID == "" || message == "" {
		return Alarm{}, errors.New("alarm vessel and message are required")
	}
	switch kind {
	case AlarmLowLevel, AlarmPressure, AlarmLeak, AlarmProbe:
	default:
		return Alarm{}, errors.New("unknown alarm kind")
	}
	return Alarm{
		ID:        uuid.New(),
		VesselID:  vesselID,
		Kind:      kind,
		State:     AlarmDetected,
		Message:   message,
		CreatedAt: now.UTC(),
		UpdatedAt: now.UTC(),
	}, nil
}

func (a Alarm) WithChannels(results []ChannelResult, now time.Time) Alarm {
	a.Channels = append([]ChannelResult(nil), results...)
	a.State = AlarmDelivered
	for _, result := range results {
		if result.Required && !result.Delivered {
			a.State = AlarmPartial
			break
		}
	}
	a.UpdatedAt = now.UTC()
	return a
}

// Resolved reports whether the alarm reached a user-driven terminal state
// (acknowledged or cleared), after which delivery must not reorder the state.
func (a Alarm) Resolved() bool {
	return a.State == AlarmAcknowledged || a.State == AlarmCleared
}

func (a Alarm) Acknowledge(now time.Time) Alarm {
	a.State = AlarmAcknowledged
	a.UpdatedAt = now.UTC()
	return a
}

func (a Alarm) Clear(now time.Time) Alarm {
	a.State = AlarmCleared
	a.UpdatedAt = now.UTC()
	return a
}
