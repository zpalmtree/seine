# Integration Playbook

## 1. Boot API Service

Run:
```bash
./seine --service --api-bind 127.0.0.1:9977
```

Check health:
```bash
curl -s http://127.0.0.1:9977/v1/health
```

## 2. Read Defaults and Start with Sparse Overrides

```bash
curl -s http://127.0.0.1:9977/v1/runtime/config/defaults
```

```bash
curl -s -X POST http://127.0.0.1:9977/v1/miner/start \
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

If starting in daemon mode and daemon auth is not preconfigured, include one of:
- `token` in `POST /v1/miner/start`
- `cookie_path` in `POST /v1/miner/start`

In pool mode, those same fields remain optional and enable local daemon wallet balance when a local daemon is available.

## 3. Observe Live Progress

```bash
curl -N http://127.0.0.1:9977/v1/events/stream
```

Watch for:
- NVIDIA compile/init progress (`nvidia.init.progress`)
- autotune messages (`autotune.progress`)
- phase transitions (`state.changed`, `backend.phase`)

## 4. Wallet Unlock Flow

If `wallet.required` occurs:

```bash
curl -s -X POST http://127.0.0.1:9977/v1/wallet/unlock \
  -H 'content-type: application/json' \
  -d '{"password":"<wallet-password>"}'
```

Then restart mining if needed.

## 5. Stop/Restart

```bash
curl -s -X POST http://127.0.0.1:9977/v1/miner/stop
curl -s -X POST http://127.0.0.1:9977/v1/miner/restart -H 'content-type: application/json' -d '{}'
```

## 6. Metrics

```bash
curl -s http://127.0.0.1:9977/metrics
```
