package campaign

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rolork/go_ota/internal/broker"
	"github.com/rolork/go_ota/internal/models"
	"golang.org/x/sync/errgroup"
	"log/slog"
)

type Config struct {
	CommandTopicPrefix     string
	EventTopicPrefix       string
	FirmwareURL            string
	FirmwareSHA256         string
	Force                  bool
	MaxConcurrentDownloads int
	RetryMax               int
	RetryBase              time.Duration
	ReportDir              string
	CampaignTimeout        time.Duration
	AttemptTimeout         time.Duration
	QoS                    byte
}

type Runner struct {
	broker *broker.Client
	log    *slog.Logger
	cfg    Config
}

func New(cfg Config, b *broker.Client, logger *slog.Logger) (*Runner, error) {
	if b == nil {
		return nil, errors.New("campaign: broker client is required")
	}
	if cfg.CommandTopicPrefix == "" {
		return nil, errors.New("campaign: command topic prefix is required")
	}
	if cfg.EventTopicPrefix == "" {
		return nil, errors.New("campaign: event topic prefix is required")
	}
	if cfg.FirmwareURL == "" {
		return nil, errors.New("campaign: firmware URL is required")
	}
	if cfg.ReportDir == "" {
		cfg.ReportDir = "reports"
	}
	if cfg.MaxConcurrentDownloads <= 0 {
		cfg.MaxConcurrentDownloads = 5
	}
	if cfg.RetryBase <= 0 {
		cfg.RetryBase = 500 * time.Millisecond
	}
	if cfg.AttemptTimeout <= 0 {
		cfg.AttemptTimeout = 2 * time.Minute
	}
	if cfg.QoS > 2 {
		cfg.QoS = 0
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &Runner{
		broker: b,
		log:    logger,
		cfg:    cfg,
	}, nil
}

func (r *Runner) Run(ctx context.Context, campaignID string, deviceIDs []string) (*models.CampaignReport, error) {
	if campaignID == "" {
		return nil, errors.New("campaign: id is required")
	}
	if len(deviceIDs) == 0 {
		return nil, errors.New("campaign: device list is empty")
	}

	if r.cfg.CampaignTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.cfg.CampaignTimeout)
		defer cancel()
	}

	if err := r.broker.Connect(ctx); err != nil {
		return nil, err
	}

	report := &models.CampaignReport{
		CampaignID:  campaignID,
		FirmwareURL: r.cfg.FirmwareURL,
		SHA256:      r.cfg.FirmwareSHA256,
		Force:       r.cfg.Force,
		StartedAt:   time.Now().UTC(),
	}

	tracker := newDeviceTracker(deviceIDs)

	eventTopic := fmt.Sprintf("%s/+/ota", r.cfg.EventTopicPrefix)
	handler := r.makeEventHandler(ctx, tracker)

	if err := r.broker.Subscribe(ctx, eventTopic, r.cfg.QoS, handler); err != nil {
		return nil, fmt.Errorf("campaign subscribe: %w", err)
	}
	defer func() {
		if err := r.broker.Unsubscribe(context.Background(), eventTopic); err != nil {
			r.log.Warn("unsubscribe failed", "topic", eventTopic, "error", err)
		}
	}()

	sem := make(chan struct{}, r.cfg.MaxConcurrentDownloads)
	g, gctx := errgroup.WithContext(ctx)

	for _, id := range deviceIDs {
		deviceID := id
		g.Go(func() error {
			return r.runDevice(gctx, campaignID, deviceID, tracker, sem)
		})
	}

	err := g.Wait()
	report.CompletedAt = time.Now().UTC()
	report.Duration = report.CompletedAt.Sub(report.StartedAt)

	report.Devices = tracker.report()
	totals := models.CampaignTotals{}
	for _, d := range report.Devices {
		totals.Total++
		switch d.State {
		case models.DeviceStateSucceeded:
			totals.Succeeded++
		case models.DeviceStateFailed:
			totals.Failed++
		case models.DeviceStateSkipped:
			totals.Skipped++
		}
	}
	report.Totals = totals

	if err != nil {
		report.FailedReason = err.Error()
	}

	if wErr := r.writeReport(report); wErr != nil {
		return report, wErr
	}

	return report, err
}

func (r *Runner) runDevice(ctx context.Context, campaignID, deviceID string, tracker *deviceTracker, sem chan struct{}) error {
	state := tracker.status(deviceID)
	if state == nil {
		return fmt.Errorf("device %s not registered", deviceID)
	}

	events := tracker.eventChannel(deviceID)
	if events == nil {
		return fmt.Errorf("device %s event channel missing", deviceID)
	}
	defer tracker.closeEvents(deviceID)

	maxAttempts := r.cfg.RetryMax
	if maxAttempts <= 0 {
		maxAttempts = 1
	} else {
		maxAttempts++
	}

	backoff := r.cfg.RetryBase
	if backoff <= 0 {
		backoff = 500 * time.Millisecond
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if mu, ok := tracker.lockDevice(deviceID); ok {
			state.Attempts = attempt
			if state.StartedAt.IsZero() {
				state.StartedAt = time.Now().UTC()
			}
			state.State = models.DeviceStatePending
			mu.Unlock()
		}

		if err := acquire(ctx, sem); err != nil {
			return err
		}

		err := r.publishStart(ctx, campaignID, deviceID)
		if err != nil {
			release(sem)
			if mu, ok := tracker.lockDevice(deviceID); ok {
				state.LastError = err.Error()
				mu.Unlock()
			}
			r.log.Error("publish failed", "device", deviceID, "error", err)
			continue
		}

		r.log.Info("OTA command dispatched", "device", deviceID, "attempt", attempt)
		attemptCtx, cancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
		outcome, waitErr := waitForOutcome(attemptCtx, events)
		cancel()
		release(sem)

		if waitErr != nil {
			if mu, ok := tracker.lockDevice(deviceID); ok {
				state.LastError = waitErr.Error()
				state.State = models.DeviceStateFailed
				state.CompletedAt = time.Now().UTC()
				mu.Unlock()
			}
			if attempt == maxAttempts {
				return waitErr
			}
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		if mu, ok := tracker.lockDevice(deviceID); ok {
			state.Progress = outcome.Progress
			if outcome.Error != "" {
				state.LastError = outcome.Error
			}

			switch outcome.State {
			case models.DeviceStateSucceeded, models.DeviceStateSkipped:
				state.State = outcome.State
				state.CompletedAt = time.Now().UTC()
				mu.Unlock()
				return nil
			case models.DeviceStateFailed:
				state.State = outcome.State
				state.LastError = firstNonEmpty(outcome.Error, state.LastError)
				if attempt == maxAttempts {
					state.CompletedAt = time.Now().UTC()
					mu.Unlock()
					return fmt.Errorf("device %s failed after %d attempts: %s", deviceID, attempt, state.LastError)
				}
				mu.Unlock()
				time.Sleep(backoff)
				backoff *= 2
			default:
				mu.Unlock()
				// retry if we did not receive a terminal state
				time.Sleep(backoff)
				backoff *= 2
			}
		}
	}

	return fmt.Errorf("device %s exhausted retries", deviceID)
}

func (r *Runner) publishStart(ctx context.Context, campaignID, deviceID string) error {
	command := models.OTACommand{
		CampaignID:  campaignID,
		FirmwareURL: r.cfg.FirmwareURL,
		SHA256:      r.cfg.FirmwareSHA256,
		Force:       r.cfg.Force,
	}

	payload, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("marshal ota command: %w", err)
	}

	topic := fmt.Sprintf("%s/%s/ota/start", r.cfg.CommandTopicPrefix, deviceID)
	return r.broker.Publish(ctx, topic, payload, r.cfg.QoS, false)
}

func (r *Runner) makeEventHandler(ctx context.Context, tracker *deviceTracker) mqtt.MessageHandler {
	return func(_ mqtt.Client, msg mqtt.Message) {
		var evt models.DeviceEvent
		if err := json.Unmarshal(msg.Payload(), &evt); err != nil {
			r.log.Error("event unmarshal failed", "topic", msg.Topic(), "error", err)
			return
		}

		if evt.DeviceID == "" {
			r.log.Warn("event missing device id", "topic", msg.Topic())
			return
		}

		events := tracker.eventChannel(evt.DeviceID)
		if events == nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case events <- evt:
		default:
			// avoid blocking: drop oldest event if channel full
			select {
			case <-events:
			default:
			}
			select {
			case events <- evt:
			default:
				r.log.Warn("event dropped", "device", evt.DeviceID)
			}
		}

		if state := tracker.status(evt.DeviceID); state != nil {
			if lock, ok := tracker.lockDevice(evt.DeviceID); ok {
				if state.StartedAt.IsZero() && evt.State != models.DeviceStatePending {
					state.StartedAt = time.Now().UTC()
				}
				if evt.Progress > state.Progress {
					state.Progress = evt.Progress
				}
				if evt.State != "" {
					state.State = evt.State
					if isTerminal(evt.State) && state.CompletedAt.IsZero() {
						state.CompletedAt = time.Now().UTC()
					}
				}
				if evt.Error != "" {
					state.LastError = evt.Error
				}
				lock.Unlock()
			}
		}
	}
}

func (r *Runner) writeReport(report *models.CampaignReport) error {
	if err := os.MkdirAll(r.cfg.ReportDir, 0o755); err != nil {
		return fmt.Errorf("create report dir: %w", err)
	}

	filename := fmt.Sprintf("cmp_%s.json", report.CampaignID)
	path := filepath.Join(r.cfg.ReportDir, filename)

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")

	if err := enc.Encode(report); err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	r.log.Info("campaign report written", "path", path)
	return nil
}

func waitForOutcome(ctx context.Context, events <-chan models.DeviceEvent) (models.DeviceEvent, error) {
	for {
		select {
		case <-ctx.Done():
			return models.DeviceEvent{}, ctx.Err()
		case evt := <-events:
			if isTerminal(evt.State) {
				return evt, nil
			}
		}
	}
}

func isTerminal(state models.DeviceState) bool {
	return state == models.DeviceStateSucceeded ||
		state == models.DeviceStateFailed ||
		state == models.DeviceStateSkipped
}

func acquire(ctx context.Context, sem chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case sem <- struct{}{}:
		return nil
	}
}

func release(sem chan struct{}) {
	select {
	case <-sem:
	default:
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

type deviceTracker struct {
	mu     sync.RWMutex
	items  map[string]*models.DeviceReport
	events map[string]chan models.DeviceEvent
	locks  map[string]*sync.Mutex
}

func newDeviceTracker(deviceIDs []string) *deviceTracker {
	items := make(map[string]*models.DeviceReport, len(deviceIDs))
	events := make(map[string]chan models.DeviceEvent, len(deviceIDs))
	locks := make(map[string]*sync.Mutex, len(deviceIDs))
	for _, id := range deviceIDs {
		items[id] = &models.DeviceReport{
			DeviceID: id,
			State:    models.DeviceStatePending,
		}
		events[id] = make(chan models.DeviceEvent, 32)
		locks[id] = &sync.Mutex{}
	}
	return &deviceTracker{
		items:  items,
		events: events,
		locks:  locks,
	}
}

func (t *deviceTracker) eventChannel(id string) chan models.DeviceEvent {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.events[id]
}

func (t *deviceTracker) closeEvents(id string) {
	t.mu.Lock()
	delete(t.events, id)
	t.mu.Unlock()
}

func (t *deviceTracker) status(id string) *models.DeviceReport {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.items[id]
}

func (t *deviceTracker) lockDevice(id string) (*sync.Mutex, bool) {
	t.mu.RLock()
	m, ok := t.locks[id]
	t.mu.RUnlock()
	if !ok {
		return nil, false
	}
	m.Lock()
	return m, true
}

func (t *deviceTracker) report() []models.DeviceReport {
	t.mu.RLock()
	defer t.mu.RUnlock()

	reports := make([]models.DeviceReport, 0, len(t.items))
	for _, v := range t.items {
		reports = append(reports, *v)
	}
	sort.Slice(reports, func(i, j int) bool {
		return reports[i].DeviceID < reports[j].DeviceID
	})
	return reports
}
