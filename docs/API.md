# Control API

This page documents the Seine local control API for frontend integrations, automation, and observability.

Canonical machine-readable docs:

- OpenAPI: `docs/openapi/seine-api-v1.yaml`
- Request/response schemas: `docs/schemas/`
- SSE event catalog: `docs/llm/sse-event-catalog.md`

## API Modes

Seine supports two API startup modes.

### Service Mode (idle until API start)

```bash
./seine --service --api-bind 127.0.0.1:9977
```

Behavior:

- API starts immediately.
- Miner stays idle until `POST /v1/miner/start`.
- Best for external controllers and dashboards.

### Embedded Mode (mine immediately + API)

```bash
./seine --api-server --api-bind 127.0.0.1:9977
```

Behavior:

- API starts immediately.
- Miner start is triggered automatically at process startup.

## Security and Binding

- Every endpoint except `GET /v1/health` requires `Authorization: Bearer <token>`.
- Seine generates a random token at startup, writes it to `<data-dir>/control-api.cookie` (owner-only permissions on Unix), and deletes the file on shutdown. Use `--api-cookie <path>` to move the file or `--api-token <token>` to set a fixed token instead.
- Default bind is loopback only: `127.0.0.1:9977`. On a loopback bind, requests whose `Host` header is not `localhost` or a loopback IP are rejected with `403`, which blocks DNS-rebinding pages.
- Non-loopback bind requires `--api-allow-unsafe-bind`.
- No CORS headers are sent by default, so web pages on other origins cannot call the API. Pass `--api-cors <origin>` to allow a browser frontend.

## Mode-Specific Start Requirements

- Pool mode (`mode=pool`) requires `mining_address`, `pool_url`, and `pool_worker`. `pool_url` takes the same forms as `--pool-url`, including `stratum+ssl://` and a comma-separated failover list.
- Daemon mode (`mode=daemon`) requires daemon auth token.
- Pool mode may also include daemon auth to enable local daemon wallet balance alongside pool stats when a local daemon is available.

Daemon auth can be provided via:

- Startup flags: `--token` or `--cookie`
- Start request payload fields: `token` or `cookie_path`

Rules:

- `token` and `cookie_path` are mutually exclusive in a start/patch payload.
- Missing required mode-specific fields make `/v1/miner/start` return `400`.

## Endpoint Summary

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/health` | API liveness/version metadata |
| `GET` | `/v1/runtime/state` | Full runtime state snapshot |
| `GET` | `/v1/runtime/config/defaults` | Baseline config from process startup |
| `GET` | `/v1/runtime/config/effective` | Effective config for current/next session |
| `POST` | `/v1/miner/start` | Start mining with optional sparse overrides |
| `POST` | `/v1/miner/stop` | Stop mining (optional wait budget) |
| `POST` | `/v1/miner/restart` | Stop then start with optional sparse overrides |
| `PATCH` | `/v1/miner/live-config` | Stage config changes for next start/session |
| `GET` | `/v1/miner/stats` | Live hashrate, share counters and per-device rates |
| `GET` | `/v1/backends` | Backend phase visibility, configured backends, and detected GPUs (`available_devices`) |
| `POST` | `/v1/wallet/unlock` | Store wallet password for future startup attempts |
| `GET` | `/v1/events/stream` | Runtime SSE event stream |
| `GET` | `/metrics` | Prometheus metrics |

## Common Response Shapes

- Action endpoints return `ActionResponse` (`ok`, `message`, `requires_restart`, `state`).
- Error responses use `ErrorBody` (`code`, `message`).
- State endpoints return `RuntimeState` or wrappers around effective/default config.

See schema files:

- `docs/schemas/action-response.schema.json`
- `docs/schemas/runtime-state.schema.json`
- `docs/schemas/start-request.schema.json`

## Start/Restart/Patch Payload (`StartRequest`)

`StartRequest` is sparse/optional: send only keys you want to override.

High-impact groups:

- Mode/connectivity/auth: `mode`, `api_url`, `token`, `cookie_path`, `mining_address`, `pool_url`, `pool_worker`
  Optional in pool mode: `token` or `cookie_path` lets Seine read local daemon wallet balance alongside pool stats without switching out of pool mining.
- Backend topology: `backend_specs`, `threads`, `cpu_affinity`, `cpu_profile`
- Runtime timing: `refresh_secs`, `request_timeout_secs`, `stats_secs`
- CPU/NVIDIA/Metal tuning knobs (same names as runtime config fields)
- Scheduling/accounting: `work_allocation`, `strict_round_accounting`, `nonce_iters_per_lane`
- UI/SSE behavior: `ui_mode`, `sse_enabled`, `refresh_on_same_height`

For the full field list and value constraints, use `docs/schemas/start-request.schema.json`.

## Live Stats (`GET /v1/miner/stats`)

Returns current and average hashrate (H/s), uptime, submitted/accepted/rejected
counters, and one entry per backend instance with its GPU index and PCI address.
Mining-OS stats scripts and dashboards can poll it every few seconds. `mining` is
`null` until the first sample of a session, and current rates read as zero while
the miner is not hashing (for example during a pool reconnect).

```json
{
  "version": "0.2.46",
  "lifecycle": "running",
  "running": true,
  "mining": {
    "state": "working",
    "uptime_secs": 3605.2,
    "updated_at_unix_ms": 1767225600000,
    "hashrate_hps": 412.5,
    "average_hashrate_hps": 409.8,
    "total_hashes": 1477390,
    "submitted": 52,
    "accepted": 51,
    "rejected": 1,
    "devices": [
      {"label": "nvidia#1", "kind": "nvidia", "device_index": 0,
       "pci_address": "0000:01:00.0", "lanes": 11,
       "hashrate_hps": 206.1, "average_hashrate_hps": 204.9}
    ]
  }
}
```

## Live Config Semantics (`PATCH /v1/miner/live-config`)

`PATCH /v1/miner/live-config` always updates staged config.

- If miner is not active: config becomes effective immediately for next start.
- If miner is active (`starting`/`running`/`stopping`): response sets `requires_restart: true` and change is queued for next start.

## SSE Stream (`GET /v1/events/stream`)

SSE frame fields:

- `id`: monotonic sequence number
- `event`: event type
- `data`: JSON payload

Core event types:

- `state.changed`
- `miner.log`
- `backend.phase`
- `nvidia.init.progress`
- `autotune.progress`
- `autotune.completed`
- `wallet.required`
- `wallet.loaded`
- `solution.found`
- `submit.result`
- `devfee.mode`

Delivery notes:

- In-memory broadcast (best effort).
- Slow consumers may lag and miss older events.
- Use `GET /v1/runtime/state` after reconnect to resync state.

## Metrics (`GET /metrics`)

Prometheus metrics exposed by the control API:

- `seine_miner_running`
- `seine_miner_lifecycle_info{state="..."}`
- `seine_miner_pending_nvidia`
- `seine_miner_pending_amd`
- `seine_miner_start_requests_total`
- `seine_miner_stop_requests_total`
- `seine_miner_restart_requests_total`
- `seine_miner_live_config_patches_total`
- `seine_miner_wallet_unlock_requests_total`
- `seine_miner_log_events_total`
- `seine_miner_submitted_blocks_total`
- `seine_miner_accepted_blocks_total`
- `seine_miner_stale_submits_total`
- `seine_miner_backend_quarantines_total`

## Practical Curl Flows

### 1. Start Service, Check Health

```bash
./seine --service --api-bind 127.0.0.1:9977
curl -s http://127.0.0.1:9977/v1/health
export SEINE_API_TOKEN="$(cat seine-data/control-api.cookie)"
```

### 2. Start Mining with Sparse Overrides

```bash
curl -s -X POST http://127.0.0.1:9977/v1/miner/start \
  -H "Authorization: Bearer $SEINE_API_TOKEN" \
  -H 'content-type: application/json' \
  -d '{
    "mode":"pool",
    "mining_address":"PpkFxY...",
    "pool_url":"stratum+tcp://pool.example.com:3333",
    "pool_worker":"rig-01",
    "threads":2,
    "work_allocation":"adaptive",
    "stats_secs":5
  }'
```

### 3. Watch Live Events

```bash
curl -N -H "Authorization: Bearer $SEINE_API_TOKEN" http://127.0.0.1:9977/v1/events/stream
```

### 4. Queue Config Patch

```bash
curl -s -X PATCH http://127.0.0.1:9977/v1/miner/live-config \
  -H "Authorization: Bearer $SEINE_API_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"ui_mode":"plain","stats_secs":3}'
```

Check `requires_restart` in the response.

### 5. Wallet Unlock When Required

```bash
curl -s -X POST http://127.0.0.1:9977/v1/wallet/unlock \
  -H "Authorization: Bearer $SEINE_API_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"password":"<wallet-password>"}'
```

### 6. Stop Miner

```bash
curl -s -X POST http://127.0.0.1:9977/v1/miner/stop \
  -H "Authorization: Bearer $SEINE_API_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"wait_ms":10000}'
```
