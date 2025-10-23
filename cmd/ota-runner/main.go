package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/rolork/go_ota/internal/broker"
	"github.com/rolork/go_ota/internal/campaign"
	"github.com/rolork/go_ota/internal/models"
)

func main() {
	godotenv.Load()

	campaignID := flag.String("campaign-id", "", "campaign identifier")
	devicesArg := flag.String("devices", "", "comma separated list of device IDs")
	forceFlag := flag.Bool("force", false, "force OTA even if firmware already applied")
	reportDir := flag.String("report-dir", "", "directory to write campaign reports")
	flag.Parse()

	if *campaignID == "" {
		exit(errors.New("--campaign-id is required"))
	}
	if *devicesArg == "" {
		exit(errors.New("--devices list is required"))
	}

	deviceIDs := parseDeviceIDs(*devicesArg)
	if len(deviceIDs) == 0 {
		exit(errors.New("no valid device IDs provided"))
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	brokerURL := getEnvOrExit("MQTT_BROKER_URL")
	cmdPrefix := strings.TrimSuffix(getEnvOrExit("TOPIC_CMD_PREFIX"), "/")
	eventPrefix := strings.TrimSuffix(getEnvOrExit("TOPIC_EVT_PREFIX"), "/")
	firmwareBase := valueOrDefault(os.Getenv("FW_BASE_URL"), "http://localhost:8080/firmware")
	firmwareFile := valueOrDefault(os.Getenv("FW_FILE"), "firmware/demo.bin")
	firmwareURL := buildFirmwareURL(firmwareBase, firmwareFile)
	firmwareSHA := os.Getenv("FW_SHA256")

	maxConcurrent := getIntEnv("MAX_CONCURRENT_DOWNLOADS", 5)
	retryMax := getIntEnv("RETRY_MAX", 3)
	retryBase := time.Duration(getIntEnv("RETRY_BASE_MS", 500)) * time.Millisecond
	campaignTimeout := time.Duration(getIntEnv("CAMPAIGN_TIMEOUT_SEC", 0)) * time.Second
	attemptTimeout := time.Duration(getIntEnv("ATTEMPT_TIMEOUT_SEC", 120)) * time.Second

	if attemptTimeout <= 0 {
		attemptTimeout = 2 * time.Minute
	}

	clientID := fmt.Sprintf("ota-runner-%d", time.Now().UnixNano())
	mqttClient, err := broker.New(broker.Config{
		BrokerURL: brokerURL,
		ClientID:  clientID,
		Logger:    logger,
	})
	if err != nil {
		exit(err)
	}
	defer mqttClient.Disconnect()

	cfg := campaign.Config{
		CommandTopicPrefix:     cmdPrefix,
		EventTopicPrefix:       eventPrefix,
		FirmwareURL:            firmwareURL,
		FirmwareSHA256:         firmwareSHA,
		Force:                  *forceFlag,
		MaxConcurrentDownloads: maxConcurrent,
		RetryMax:               retryMax,
		RetryBase:              retryBase,
		ReportDir:              firstNonEmpty(*reportDir, "reports"),
		CampaignTimeout:        campaignTimeout,
		AttemptTimeout:         attemptTimeout,
		QoS:                    1,
	}

	runner, err := campaign.New(cfg, mqttClient, logger)
	if err != nil {
		exit(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	report, runErr := runner.Run(ctx, *campaignID, deviceIDs)
	if runErr != nil {
		logger.Error("campaign finished with error", "error", runErr)
	} else {
		logger.Info("campaign completed", "campaign", *campaignID)
	}

	reportPath := filepath.Join(cfg.ReportDir, fmt.Sprintf("cmp_%s.json", *campaignID))
	printSummary(report, reportPath)
	if runErr != nil {
		exit(runErr)
	}
}

func parseDeviceIDs(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		id := strings.TrimSpace(part)
		if id != "" {
			out = append(out, id)
		}
	}
	return out
}

func printSummary(report *models.CampaignReport, path string) {
	if report == nil {
		return
	}
	fmt.Printf("Campaign %s: %d succeeded, %d failed, %d skipped (total %d)\n",
		report.CampaignID,
		report.Totals.Succeeded,
		report.Totals.Failed,
		report.Totals.Skipped,
		report.Totals.Total,
	)
	fmt.Printf("Report written to %s\n", path)
}

func buildFirmwareURL(base, file string) string {
	base = strings.TrimSuffix(base, "/")
	file = strings.TrimPrefix(file, "/")
	return fmt.Sprintf("%s/%s", base, file)
}

func getEnvOrExit(key string) string {
	val := os.Getenv(key)
	if val == "" {
		exit(fmt.Errorf("%s is required", key))
	}
	return val
}

func valueOrDefault(value string, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func getIntEnv(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			return parsed
		}
	}
	return fallback
}

func exit(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
