package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/joho/godotenv"
	"github.com/rolork/go_ota/internal/broker"
	"github.com/rolork/go_ota/internal/models"
)

type simulatorConfig struct {
	CommandTopicPrefix string
	EventTopicPrefix   string
	QoS                byte
	FailureRate        float64
}

type simulator struct {
	log     *slog.Logger
	client  *broker.Client
	cfg     simulatorConfig
	devices map[string]*simDevice
	mu      sync.RWMutex
	rnd     *rand.Rand
	rndMu   sync.Mutex
	wg      sync.WaitGroup
}

type simDevice struct {
	id      string
	running bool
	mu      sync.Mutex
}

func newSimulator(log *slog.Logger, client *broker.Client, cfg simulatorConfig, deviceIDs []string) *simulator {
	devices := make(map[string]*simDevice, len(deviceIDs))
	for _, id := range deviceIDs {
		devices[id] = &simDevice{id: id}
	}
	return &simulator{
		log:     log,
		client:  client,
		cfg:     cfg,
		devices: devices,
		rnd:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *simulator) run(ctx context.Context) error {
	if err := s.client.Connect(ctx); err != nil {
		return err
	}

	topic := path.Join(s.cfg.CommandTopicPrefix, "+", "ota", "start")
	if err := s.client.Subscribe(ctx, topic, s.cfg.QoS, s.messageHandler(ctx)); err != nil {
		return err
	}
	defer func() {
		_ = s.client.Unsubscribe(context.Background(), topic)
	}()

	s.log.Info("device simulator ready", "devices", len(s.devices), "topic", topic)
	<-ctx.Done()
	s.log.Info("device simulator stopping")

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		s.log.Warn("simulator exit timeout; background routines may still be running")
	}

	s.client.Disconnect()
	return nil
}

func (s *simulator) messageHandler(ctx context.Context) mqtt.MessageHandler {
	return func(_ mqtt.Client, msg mqtt.Message) {
		deviceID, err := s.deviceIDFromTopic(msg.Topic())
		if err != nil {
			s.log.Debug("ignoring topic", "topic", msg.Topic(), "error", err)
			return
		}

		cmd := models.OTACommand{}
		if err := json.Unmarshal(msg.Payload(), &cmd); err != nil {
			s.log.Error("command decode failed", "device", deviceID, "error", err)
			return
		}

		s.mu.RLock()
		device, ok := s.devices[deviceID]
		s.mu.RUnlock()
		if !ok {
			s.log.Debug("command for unmanaged device", "device", deviceID)
			return
		}

		s.startCycle(ctx, device, cmd)
	}
}

func (s *simulator) deviceIDFromTopic(topic string) (string, error) {
	prefix := s.cfg.CommandTopicPrefix
	if !strings.HasPrefix(topic, prefix) {
		return "", fmt.Errorf("invalid prefix")
	}
	trimmed := strings.TrimPrefix(topic, prefix)
	trimmed = strings.TrimPrefix(trimmed, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 3 {
		return "", fmt.Errorf("topic too short")
	}
	return parts[0], nil
}

func (s *simulator) startCycle(ctx context.Context, dev *simDevice, cmd models.OTACommand) {
	dev.mu.Lock()
	if dev.running {
		s.log.Debug("device busy", "device", dev.id)
		dev.mu.Unlock()
		return
	}
	dev.running = true
	dev.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			dev.mu.Lock()
			dev.running = false
			dev.mu.Unlock()
		}()

		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		s.sendEvent(runCtx, dev.id, models.DeviceStatePending, 0, "")
		if !s.sleep(runCtx, 200*time.Millisecond, 600*time.Millisecond) {
			return
		}

		for progress := 10; progress <= 90; progress += s.randInt(10, 25) {
			s.sendEvent(runCtx, dev.id, models.DeviceStateDownloading, clamp(progress, 0, 99), "")
			if !s.sleep(runCtx, 300*time.Millisecond, 900*time.Millisecond) {
				return
			}
		}
		s.sendEvent(runCtx, dev.id, models.DeviceStateDownloading, 100, "")

		if s.shouldFail() {
			if !s.sleep(runCtx, 200*time.Millisecond, 800*time.Millisecond) {
				return
			}
			errMsg := fmt.Sprintf("download failed for campaign %s", cmd.CampaignID)
			s.sendEvent(runCtx, dev.id, models.DeviceStateFailed, 100, errMsg)
			return
		}

		s.sendEvent(runCtx, dev.id, models.DeviceStateInstalling, 100, "")
		if !s.sleep(runCtx, 500*time.Millisecond, 1500*time.Millisecond) {
			return
		}

		s.sendEvent(runCtx, dev.id, models.DeviceStateSucceeded, 100, "")
	}()
}

func (s *simulator) sendEvent(ctx context.Context, deviceID string, state models.DeviceState, progress int, errMsg string) {
	evt := models.DeviceEvent{
		DeviceID:  deviceID,
		State:     state,
		Progress:  progress,
		Error:     errMsg,
		Timestamp: time.Now().UTC(),
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		s.log.Error("event marshal failed", "error", err)
		return
	}

	topic := path.Join(s.cfg.EventTopicPrefix, deviceID, "ota")
	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := s.client.Publish(pubCtx, topic, payload, s.cfg.QoS, false); err != nil {
		s.log.Error("event publish failed", "device", deviceID, "state", state, "error", err)
	}
}

func (s *simulator) shouldFail() bool {
	if s.cfg.FailureRate <= 0 {
		return false
	}
	s.rndMu.Lock()
	defer s.rndMu.Unlock()
	return s.rnd.Float64() < s.cfg.FailureRate
}

func (s *simulator) randInt(min, max int) int {
	if max <= min {
		return min
	}
	s.rndMu.Lock()
	defer s.rndMu.Unlock()
	return s.rnd.Intn(max-min) + min
}

func (s *simulator) sleep(ctx context.Context, min, max time.Duration) bool {
	delay := min
	if max > min {
		s.rndMu.Lock()
		delay += time.Duration(s.rnd.Int63n(int64(max - min)))
		s.rndMu.Unlock()
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func clamp(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func main() {
	godotenv.Load()

	count := flag.Int("count", 1, "number of devices to simulate")
	idPrefix := flag.String("id-prefix", "dev-", "device ID prefix")
	startIndex := flag.Int("start-index", 1, "starting index for generated device IDs")
	failureRate := flag.Float64("failure-rate", 0.0, "fraction (0-1) of downloads that fail")
	qos := flag.Int("qos", 1, "MQTT QoS level to use for events")
	flag.Parse()

	if *count <= 0 {
		exit(errors.New("device count must be positive"))
	}

	commandPrefix := strings.TrimSuffix(os.Getenv("TOPIC_CMD_PREFIX"), "/")
	eventPrefix := strings.TrimSuffix(os.Getenv("TOPIC_EVT_PREFIX"), "/")
	if commandPrefix == "" || eventPrefix == "" {
		exit(errors.New("TOPIC_CMD_PREFIX and TOPIC_EVT_PREFIX are required"))
	}

	deviceIDs := make([]string, *count)
	width := len(fmt.Sprintf("%d", *startIndex+*count-1))
	if width < 4 {
		width = 4
	}
	for i := 0; i < *count; i++ {
		deviceIDs[i] = fmt.Sprintf("%s%0*d", *idPrefix, width, *startIndex+i)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	brokerURL := os.Getenv("MQTT_BROKER_URL")
	if brokerURL == "" {
		exit(errors.New("MQTT_BROKER_URL is required"))
	}

	clientID := fmt.Sprintf("device-sim-%d", time.Now().UnixNano())
	client, err := broker.New(broker.Config{
		BrokerURL: brokerURL,
		ClientID:  clientID,
		Logger:    logger,
	})
	if err != nil {
		exit(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sim := newSimulator(logger, client, simulatorConfig{
		CommandTopicPrefix: commandPrefix,
		EventTopicPrefix:   eventPrefix,
		QoS:                byte(*qos),
		FailureRate:        clampFloat(*failureRate, 0, 1),
	}, deviceIDs)

	if err := sim.run(ctx); err != nil {
		exit(err)
	}
}

func clampFloat(value, min, max float64) float64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func exit(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
