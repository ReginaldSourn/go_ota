# Go OTA Concurrency Tester

Go-based orchestration toolkit that stress-tests over-the-air (OTA) firmware updates across large batches of ESP32-S3 class devices. The suite coordinates MQTT command delivery, hosts firmware over HTTP, and simulates device lifecycles so you can rehearse OTA campaigns before hitting physical hardware.

## Highlights

- Concurrent OTA scheduling with configurable worker limits and exponential backoff retry logic.
- Device simulator publishes realistic progress/failure events over MQTT with optional failure injection.
- Lightweight firmware file server with `/healthz` endpoint for automation.
- Structured JSON campaign reports summarising per-device outcomes and attempt metadata.

## Project Layout

- `cmd/serve-fw` – HTTP firmware hosting service.
- `cmd/device-sim` – ESP32-S3 OTA lifecycle simulator.
- `cmd/ota-runner` – Orchestrates OTA campaigns end-to-end.
- `internal/broker` – MQTT client wrapper with reconnect-aware helpers.
- `internal/campaign` – Concurrency, retry strategy, and report writer.
- `internal/models` – Shared command, event, and reporting types.
- `configs/sample.env` – Baseline environment configuration.
- `firmware/demo.bin` – Example firmware payload.
- `reports/` – Generated campaign reports (`cmp_<id>.json`).

## Prerequisites

- Go 1.24+ (Go 1.25 recommended).
- Running MQTT broker (tested with Eclipse Mosquitto).
- Firmware file accessible on disk.
- Optional: Docker (for quickly launching Mosquitto).

## Setup

1. Copy the sample env file and adjust values as needed:
   ```bash
   cp configs/sample.env .env
   ```
2. Ensure `FW_FILE` points to the firmware binary you want to serve.
3. If you do not already have a broker, start one (see below).

## Configuration

Environment variables are loaded from `.env` (via `github.com/joho/godotenv`). Key settings:

| Variable | Description |
| --- | --- |
| `MQTT_BROKER_URL` | MQTT broker address (e.g. `tcp://localhost:1883`). |
| `TOPIC_CMD_PREFIX` | Command topic prefix (devices subscribe for OTA start commands). |
| `TOPIC_EVT_PREFIX` | Event topic prefix (devices publish OTA status updates). |
| `FW_HTTP_ADDR` | Listen address for the firmware server. |
| `FW_BASE_URL` | Public base URL for firmware downloads. |
| `FW_FILE` | Path to the firmware binary served over HTTP. |
| `FW_SHA256` | Expected SHA256 checksum the OTA runner distributes. |
| `MAX_CONCURRENT_DOWNLOADS` | Cap on in-flight OTA jobs. |
| `RETRY_MAX` | Number of retries before giving up (per device). |
| `RETRY_BASE_MS` | Initial backoff duration (milliseconds). |
| `CAMPAIGN_TIMEOUT_SEC` | Optional overall timeout (seconds). |
| `ATTEMPT_TIMEOUT_SEC` | Optional per-attempt timeout (seconds, defaults to 120). |

## Quick Start

Open four terminals (broker, firmware server, simulator, runner):

1. **MQTT broker (Mosquitto via Docker):**
   ```bash
   docker run --rm -it -p 1883:1883 eclipse-mosquitto
   ```
2. **Firmware server:**
   ```bash
   go run ./cmd/serve-fw
   ```
3. **Device simulator (50 devices, IDs `dev-0001` → `dev-0050`):**
   ```bash
   go run ./cmd/device-sim --count 50 --id-prefix dev- --start-index 1
   ```
4. **OTA runner (campaign `cmp_1` targeting those devices):**
   ```bash
   go run ./cmd/ota-runner \
     --campaign-id cmp_1 \
     --devices "$(seq -f 'dev-%04g' 1 50 | paste -sd , -)"
   ```

When the campaign finishes, check `reports/cmp_cmp_1.json` (or the `report-dir` you configured) for the detailed JSON summary.

## Command Reference

### Firmware Server (`cmd/serve-fw`)

```
go run ./cmd/serve-fw [--addr :8080]
```

- `--addr` overrides `FW_HTTP_ADDR`.
- Serves files under `/firmware/` rooted at `FW_FILE`'s directory.
- `/healthz` returns `200 OK` when the server is healthy.

### Device Simulator (`cmd/device-sim`)

```
go run ./cmd/device-sim \
  --count 10 \
  --id-prefix dev- \
  --start-index 1 \
  --failure-rate 0.1 \
  --qos 1
```

- Generates sequential device IDs (`<prefix><index>` padded to four digits by default).
- `--failure-rate` (0–1) controls randomized download failures.
- Gracefully handles `SIGINT`/`SIGTERM`.

### OTA Runner (`cmd/ota-runner`)

```
go run ./cmd/ota-runner \
  --campaign-id cmp_1 \
  --devices dev-0001,dev-0002 \
  --force \
  --report-dir reports
```

- Pushes OTA start commands via MQTT respecting `MAX_CONCURRENT_DOWNLOADS`.
- Retries up to `RETRY_MAX` times with exponential backoff (`RETRY_BASE_MS`).
- Writes campaign JSON to `report-dir` (defaults to `reports`).
- Prints a summary like `Campaign cmp_1: 48 succeeded, 2 failed, 0 skipped (total 50)`.

## Reports

Each campaign produces `reports/cmp_<campaign_id>.json` with:

- Per-device attempts, progress, terminal state, and timestamps.
- Aggregated totals for succeeded/failed/skipped devices.
- Optional `failed_reason` when the campaign exits with an error.

## Development Tips

- Run linters/tests as needed: `go test ./...`.
- Adjust logging verbosity by configuring `slog` handlers in each command.
- Update `configs/sample.env` whenever you add new environment knobs so they remain discoverable.

## Troubleshooting

- Ensure the firmware server URL produced by `FW_BASE_URL` + `FW_FILE` is reachable from the devices (simulator uses the same host).
- MQTT topics must stay aligned between simulator and runner (`TOPIC_CMD_PREFIX` and `TOPIC_EVT_PREFIX` without trailing slashes).
- If campaign jobs hang, check per-attempt timeouts and broker connectivity; increase logging by running commands with `SLOG_LEVEL=debug go run ...`.

