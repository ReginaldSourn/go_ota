package models

import "time"

type DeviceState string

const (
	DeviceStatePending     DeviceState = "pending"
	DeviceStateDownloading DeviceState = "downloading"
	DeviceStateInstalling  DeviceState = "installing"
	DeviceStateSucceeded   DeviceState = "succeeded"
	DeviceStateFailed      DeviceState = "failed"
	DeviceStateSkipped     DeviceState = "skipped"
)

type OTACommand struct {
	CampaignID  string `json:"campaign_id"`
	FirmwareURL string `json:"firmware_url"`
	SHA256      string `json:"sha256"`
	Force       bool   `json:"force"`
}

type DeviceEvent struct {
	DeviceID  string      `json:"device_id"`
	State     DeviceState `json:"state"`
	Progress  int         `json:"progress,omitempty"`
	Error     string      `json:"error,omitempty"`
	Timestamp time.Time   `json:"timestamp,omitempty"`
}

type DeviceReport struct {
	DeviceID    string      `json:"device_id"`
	State       DeviceState `json:"state"`
	Attempts    int         `json:"attempts"`
	Progress    int         `json:"progress"`
	LastError   string      `json:"last_error,omitempty"`
	StartedAt   time.Time   `json:"started_at,omitempty"`
	CompletedAt time.Time   `json:"completed_at,omitempty"`
}

type CampaignTotals struct {
	Total     int `json:"total"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Skipped   int `json:"skipped"`
}

type CampaignReport struct {
	CampaignID   string         `json:"campaign_id"`
	FirmwareURL  string         `json:"firmware_url"`
	SHA256       string         `json:"sha256"`
	Force        bool           `json:"force"`
	StartedAt    time.Time      `json:"started_at"`
	CompletedAt  time.Time      `json:"completed_at"`
	Duration     time.Duration  `json:"duration"`
	Devices      []DeviceReport `json:"devices"`
	Totals       CampaignTotals `json:"totals"`
	FailedReason string         `json:"failed_reason,omitempty"`
}
