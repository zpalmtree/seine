# Hardware telemetry

Seine hardware and performance reporting, with a separate Rust/Axum
collector and PostgreSQL database. Pool Stratum, wallet and daemon contracts are
unchanged. A collector outage drops reports without stopping mining.

Schema 14 adds operational summaries for the v0.3.0 release, all fixed booleans,
counts and enums:

- `diagnostics.update`: whether this process installs releases by itself
  (`auto_install`), release lookups answered by the GitHub API, by the release
  page, or failed both ways, plus installs and failed installs in the window and
  the stage of the most recent failure (`no_build`, `missing_asset`, `download`,
  `signature`, `checksum`, `extract`, `startup_probe` or `replace`).
- `diagnostics.pool` (pool mode only): whether the primary pool uses TLS, pools
  are reached through SOCKS5, and a payout memo is configured; the number of
  backup pools; and failovers, seconds on a backup pool, reconnects and
  nonce-range refills in the window, for the miner's own pool session (dev-fee
  sessions are not counted).
- `diagnostics.amd_detection`: whether the HIP runtime was unavailable, AMD GPUs
  found and selected, integrated GPUs and unsupported-wavefront GPUs skipped,
  and `pci_devices`, the AMD display devices the OS lists (Linux sysfs, vendor
  0x1002) whether or not HIP can use them; absent where unknown, such as Windows.
- `diagnostics.unsupported_work_paused_secs`: seconds in the window that mining
  was paused on work this build cannot mine, with the diagnostic reason
  `unsupported_work` and operation `receive_work`. This measures readiness for
  the Faraday fork: paused miners need an update.
- `diagnostics.backend_startup`: backend instances the run expected per kind, and
  milliseconds from the start of the run to each instance's first hash, including
  any wait for pool work. An expected backend with no entry has not hashed. Every
  report of a run repeats this record.
- `backends[].settings.gpu_arch` (`gfx1100` or `sm_86`) and
  `gpu_kernel_source` for NVIDIA and AMD backends (`prebuilt_cubin`,
  `prebuilt_ptx`, `nvrtc`, `nvrtc_cached`, `nvrtc_failed_prebuilt_ptx`,
  `builtin_code_object` or `hiprtc`), plus `amd_perf_level` for AMD backends
  (`already_high`, `raised`, `needs_root`, `kept_by_operator`, `unavailable` or
  `not_raised_for_arch`).

Privacy: these fields never contain payout memos, wallet addresses, pool hosts or
URLs, proxy addresses, IP addresses, release URLs or error text, and AMD
detection reports counts, not device identifiers. The
[schema-14 fixture](schema14-fixture.json) populates every new field.
`release_health.sql` aggregates them for the agent briefing, and the fleet alert
runner raises an alert when three installations of one version report failed
installs or paused mining within 24 hours. The GPU fields stay out of performance
cohorts, because the kernel source differs between otherwise identical runs. Deploy the
schema-14 collector before schema-14 miners. Schemas 1–13 remain accepted; no
SQL storage migration is required.

Schema 13 records backend control failures with the backend type, cancel/fence
operation, and enqueue/execution timeout stage. A backend-returned error remains
`backend_control_failed`; its category is a hint, not a proven timeout stage.
The fixed dependency-source allowlist also includes Tokio 1.49.0's
`src/sync/notify.rs`, without recording panic contents or local paths.
Deploy the schema-13 collector before schema-13 miners. Schemas 1–12 remain
accepted; no SQL storage migration is required.

Schema 12 adds the diagnostic reason `pool_connection_limit`: the pool sent a
`connection_limit` notification before closing the session, so the miner waits
the pool's suggested delay before reconnecting. Deploy the schema-12 collector
before releasing schema-12 miners; schemas 1-11 remain accepted, with no SQL
storage migration.

Schema 11 adds `backends[].settings.pow`: `blake2b` while a backend mines
Faraday BLAKE2b work, absent for Argon2id (so Argon2 samples are unchanged).
BLAKE2b samples may report up to 10^18 hashes per window; Argon2 keeps the
10^15 bound. Performance views group by the whole `settings` object, so the two
algorithms never share a cohort. Deploy the schema-11 collector before
releasing schema-11 miners; schemas 1-10 remain accepted, with no SQL storage
migration.

Schema 15 adds the search's settled result under `tuning.settled` and
`diagnostics.previous_run_oom`; see [TUNING.md](TUNING.md). Deploy the schema-15
collector before releasing schema-15 miners; schemas 1-14 remain accepted, with no
SQL storage migration.

Schema 10 adds `hardware.memory_setup`: the populated memory module count, DDR
type, whether the modules are registered, and the lowest rated and configured
speed in MT/s, read from SMBIOS (Windows always, Linux only as root; absent
otherwise). It sizes single-channel and below-rated-speed desktop setups. Deploy
the schema-10 collector before releasing schema-10 miners; schemas 1-9 remain
accepted, with no SQL storage migration.

Schema 9 adds `hardware.resources.requested_threads` (the explicit `--threads`
value, absent when sized automatically) and `hardware.resources.cpu_autotune_threads`,
so fleet analysis can separate operator choices from automatic sizing, the host's
`hardware.numa_nodes` (so multi-socket placement work can be sized against the
fleet), plus the `auto_update` shutdown-request origin written when
`--auto-update` restarts the miner. Deploy the schema-9 collector before releasing schema-9 miners: a schema-8
collector rejects their reports. Schemas 1-8 remain accepted; no SQL storage
migration or alert-threshold change is required.

Schema 8 adds fixed API-stop, API-restart, TUI-quit and TUI-thread-change origins,
plus the last entered cleanup step. Current-run reports and interrupted-run
checkpoints retain these independently. Cleanup transitions are saved directly,
including while an upload is blocked; ordinary checkpointing remains periodic.
A stop request is not a termination cause, and entering a cleanup step does not
prove it completed. Unknown/absent values remain unknown.

NVIDIA diagnostics include the loaded driver's CUDA compatibility version and
loaded NVRTC compiler version, encoded as `major * 1000 + minor * 10`. The driver
value is **not** the display-driver release number or installed toolkit version.
Seine uses the CUDA driver API and NVRTC, not the CUDA Runtime (`cudart`), so it
does not invent a CUDA Runtime version. Optional version symbols are queried
without creating a GPU context or launching an external status process; missing
symbols and failed queries leave fields absent. The fixed dependency-source
allowlist now also covers the linked cudarc 0.19.5 release.

Deploy the schema-8 collector before releasing schema-8 miners: a schema-7
collector rejects their reports. Historical schemas remain accepted; no SQL
storage migration or alert-threshold change is required. `diagnostics.sql` exposes
software versions, interrupted cleanup steps and current shutdown-context clusters.
Those clusters count reporting installations, not clean exits or proven crashes.

Schema 7 adds a fixed `previous_run.shutdown_request` enum and typed unavailable
NVIDIA driver/NVRTC library causes. The collector continues to accept schemas 1–6;
deploy schema-7 support before releasing schema-7 miners. No database migration is
needed. The request is checkpoint context, not proof of a clean exit or a crash
cause; existing unclean-run alert thresholds remain unchanged. Windows console
close now permits up to four seconds of normal cleanup, and a completed run clears
its marker before final HTTP delivery. Ordinary checkpoints remain five seconds;
the reporting worker checks for a shutdown request every second, subject to ongoing
IO. Abrupt termination can still beat a checkpoint. Windows automatic large-page
fallbacks now retain their numeric error at the source.

Schema 6 adds [paired CPU trial evidence](TUNING.md), including inconclusive and
aborted comparisons, bounded delivery and installation-weighted research hints.
The productive CPU kernel tuner emits these trials when telemetry is enabled.
Worker-count and joint producers are still pending. Kernel records can qualify
with complete, stable measured CPU masks and topology on supported Linux and
single-processor-group Windows hosts. Unknown or changed context remains coverage
only. Missing trials do not imply no gain.

## Miner preferences and collection

Telemetry is enabled by default when a collector URL is configured, for both
interactive and unattended mining. There is no startup prompt or routine notice.
Users can disable it persistently with `--telemetry off`. The user-facing disclosure
is [PRIVACY.md](../PRIVACY.md), also served at `/privacy/` on the website.
Benchmark runs do not upload reports.

For an operator-provided collector:

```sh
seine --telemetry on --telemetry-url https://telemetry.example.com/v1/reports
seine --telemetry off
```

Use these alongside the normal mining arguments on a fresh installation.
Preferences are saved to `<data-dir>/seine.telemetry.json`. Changing the URL resets
the random installation ID and preserves any explicit opt-out. Disabling is checked
before every upload;
an already in-flight request may finish. Deleting the settings file disables
future uploads from the running worker; a future launch uses the default enabled
setting if a collector is configured.

No production domain is hard-coded. After provisioning and testing the collector,
the release builder can set `SEINE_TELEMETRY_URL` to the complete HTTPS ingest URL
when building Seine. This supplies the default destination with no startup prompt.
Until then, ordinary builds make no telemetry requests.

Official release builds read the GitHub repository variable
`SEINE_TELEMETRY_URL`; set it to the verified HTTPS ingest URL before tagging a
release. Clearing it disables the default in future builds, not already
distributed binaries. Release archives include `PRIVACY.md`. The website workflow
also publishes changes made only to that policy file.

Regular reports start after five minutes and continue hourly. Runs ending with
diagnostics or paired trials can send an early final report. Requests have a five-second timeout
and no redirects. One bounded evidence report can be retained for recovery;
see reliability reporting below. HTTP is allowed only for loopback testing.
CPU-only builds need no database or collector dependencies.

The strict upload schema is in [src/lib.rs](src/lib.rs), with an example in
[fixture.json](fixture.json). It includes:

- CPU model/vendor/family/model number, logical and physical core counts,
  rounded-up GiB RAM, OS/version, native/WSL classification and supported kernels.
- Report-time allowed CPU count and OS-estimated available parallelism, effective
  memory limit and available memory (including cgroup limits where supported).
- Miner version, target, build features and whether it is a debug build.
- Active mining backend model and memory where available, enabled workers/lanes,
  CPU kernel, requested page/affinity policy and allocator-observed memory backing,
  GPU register/launch settings where available.
- Per-backend hashes and observation duration, plus a flag excluding windows with
  changed settings or missed telemetry samples from performance comparisons.
- Schema 3 reliability summaries: fixed warning/error codes and category hints,
  repository source locations, counts and elapsed times, build revision and source
  fingerprint, requested mining settings and an unclean-previous-run marker.
- Schema 14 operational summaries: self-update checks, installs and the failed
  install stage; pool TLS/SOCKS/payout-memo booleans, backup-pool count and
  failover/reconnect/refill counts (never hosts or memos); AMD GPUs found,
  selected, skipped and listed by the OS; seconds paused on unsupported work;
  expected backends and time to first hash; and GPU architecture, kernel source and
  AMD performance level.

### Reliability reporting

The [reliability fixture](reliability-fixture.json) demonstrates schema 5; the
[diagnostic fixture](diagnostics-fixture.json) retains schema-4 compatibility coverage.
The collector accepts schemas 1–15. Deploy schema-15 support before schema-15 miners.
The recorder starts after configuration parsing, before address resolution,
backend initialization and tuning. UI warnings/errors identify their original
call site through `track_caller`; backend error and quarantine paths also report
backend type. Rust panic hooks retain source locations without panic contents.
Category hints are heuristic; inspect the exact source revision before diagnosing.
Schema 5 preserves fixed source identifiers for cudarc 0.19.2's library loader,
driver bindings and NVRTC bindings instead of collapsing those panic locations
to `unknown`. Only these exact dependency paths are allowlisted; Cargo home,
usernames, arbitrary dependency filenames and panic contents are excluded.
CPU worker readiness timeouts have a typed reason and ready/expected worker counts.
Schema 4 also captures typed reasons at CPU allocation and pool DNS/connect/read/write/login
failure sites, before errors become display strings. Details contain only fixed enums,
OS error numbers and requested allocation bytes. Linux mandatory HugeTLB and Windows
mandatory large-page failures retain their cause; ordinary mapping failures retain theirs
before a possible heap fallback. A warning can describe a recoverable attempt. Backend
startup failures retain the backend type and any typed IO error available in their chain.
Windows repository-relative source paths are normalized without accepting absolute paths.
Generic warnings and uninstrumented paths still have only category/source information.
One failure can generate several warning/error codes: event counts are not counts
of independent incidents. Repeated identical events increment counters; at most
32 distinct event groups are retained per window. Dropped events and backend
samples are explicit. Structured causes can displace generic warnings at capacity; error groups can
also displace other warnings.

Early failed runs produce valid short final reports; these are excluded from the
periodic performance-completeness alarm. A local file lock protects run markers
and one pending report (32 KiB maximum) from concurrent writers sharing a data
directory. The latest diagnostic report replaces older pending diagnostics.
Pending reports preserve their report ID on replay and expire when checked after
24 hours. They are retried on a later cycle/launch, with a persisted five-minute
cooldown. Paired-trial evidence also uses this bounded pending report; ordinary
rate-only reports are not spooled. Opt-out is checked
before every upload, including replays; it clears pending state when available.
Disk/network work stays in the reporting thread, outside mining control loops.
The thread checkpoints the current run phase, elapsed time, build and release every five seconds,
including before the first upload. The bounded sidecar tied to the installation and marker write is cleared on clean
shutdown/opt-out and read only with a fresh matching unfinished-run marker. Legacy markers
remain readable. Checkpoints can lag by a tick plus reporting/network work, or be missing
on very short runs or IO failure. `mining` includes waiting/reconnecting and live tuning;
`tuning` describes startup tuning. Parallel GPU initialization may overlap that phase.
Neither checkpoint phase nor available memory establishes an OOM kill or a crash cause.
Schema-5 checkpoints also retain up to eight diagnostic groups across upload windows,
prioritizing panics, errors and typed causes before generic warnings; omission is explicit.
The sidecar is limited to 8 KiB and uses the same freshness and installation checks.
These are observations from before termination, not a crash dump. Events after the
last checkpoint can be lost, especially on an abrupt panic/abort. The panic hook
does no disk/network work. Recovered events may already have been uploaded and are
shown separately in `previous_run_issue_clusters`, without adding occurrence counts
or triggering fresh current-run error alerts.

Run `python3 telemetry/agent_report.py` for installation-weighted issue clusters
by build, source, CPU, OS, runtime environment and backend. It includes diagnostic
coverage separately from legacy clients. The scheduled health and Discord checks
use `diagnostic_alerts.sql`, which evaluates the same reliability and connection
thresholds without constructing the full seven-day diagnostic clusters. Failed
alert queries log a bounded failure category (including statement or lock timeout)
without logging SQL, reports or connection credentials. No reports does not mean
no failures.
The full on-demand diagnostic report has a 30-second statement deadline;
scheduled diagnostic checks retain a 15-second deadline.
The briefing groups typed details and phases separately and prioritizes recorded errors
and structured causes before common warnings. Its `unclean_run_clusters` includes hardware,
previous-build/phase/uptime and installation counts with hashes in the restart report.
Unclean `version` refers to the interrupted release: use the checkpoint release,
or an unambiguous build-to-version mapping observed in the seven-day window for
older checkpoints. Missing/ambiguous mappings remain `unknown`; the restarting
release is exposed as `reporting_versions`, never assumed to have caused the exit.
Hardware labels describe the reporting machine after restart. Previous-run event
clusters are checkpoint context and may overlap earlier current-run issue clusters.
Null fields mean unavailable evidence, including reports from older miners. Infrastructure
health is evaluated separately from sample-quality and miner-reliability assessments.
`diagnostics.sql` also counts installations with pool connection warnings; the alert
service flags a fleet-wide spike when at least 20 installations, and three times the
prior day's hourly average, report them within an hour.
`diagnostics.sql` flags a version after at least three installations report errors,
quarantines or unclean runs within 24 hours. Single-installation failures and
warnings remain visible below that threshold. Unattributed interrupted runs have a
separate explicitly unknown-version bucket. The five-minute server check records
these incidents in systemd/journald; the separate alert timer sends aggregate incidents to the configured Discord destination
(see the production runbook). It never sends raw report payloads.

Limits: configuration/CLI parsing failures occur before reporting starts. Native
aborts, OOM kills and power loss cannot reliably run an upload; an unclean marker
can surface on a subsequent launch, and is not proof of a crash. Panic hooks do
not supply crash dumps. Backend models/settings are available when sampled; a
device that fails before initialization may have only its requested backend type.
GPU driver versions are not currently reported. These reports guide reproduction
on matching hardware; they are not a complete replay of a user's environment.

GPU details describe active mining devices, rather than every installed adapter.
Metal currently reports backend/memory/lanes; the CPU model identifies the Apple
system. Schema 2 reports allocator-observed CPU backing as MiB in this order:
explicit large pages, explicit 1 GiB pages, transparent huge pages, regular pages,
and heap. These counters include resident parked arenas and reflect the allocator's
backing observations; they are not a continuous OS page-table survey. Affinity is
configured policy, not measured per-worker NUMA placement. Resource fields are
report-time snapshots; available memory excludes reserved HugeTLB pools. GPU
launch settings are configured limits, not every adaptive launch's depth.
These self-reported live rates include machine load and mining idle time; use them
to prioritize controlled optimization experiments, not as benchmark proof.

Wallets, worker names, hostnames, hardware serials, MAC addresses, pool URLs,
credentials, command lines and raw logs are excluded. The installation ID is
random, persisted locally, and unrelated to a wallet or hardware identifier.
It permits deduplication and is pseudonymous, not anonymous. The collector sees
the connection IP but does not store it. The supplied Caddy config suppresses both
access and error request logging; any additional CDN/proxy needs the same policy.

## Deploy

Start with one small Linux server (planning estimate: 2 vCPU, 2–4 GiB RAM), running
collector, PostgreSQL and Caddy together. A separate DB host is unnecessary for
the initial rollout. Provisioning, DNS changes and deployment are separate from
landing this code.

Point the chosen hostname at that server, allow ports 80/443, clone the repo, and
create `telemetry/deploy/.env` (ignored by Git):

```dotenv
TELEMETRY_HOST=telemetry.example.com
TELEMETRY_DB_PASSWORD=replace_with_a_random_hex_password
```

Use a random hex password (for example `openssl rand -hex 32`) so it is safe in
the PostgreSQL connection string. Then:

```sh
docker compose -f telemetry/deploy/compose.yaml --env-file telemetry/deploy/.env up -d --build
```

Only Caddy publishes ports. PostgreSQL and the collector stay on the private
Compose network; database and certificate state use persistent volumes. Caddy
obtains HTTPS certificates. Configure disk monitoring and backups with retention
no longer than the data policy. The collector reconnects after DB connection loss.
`GET /healthz` is process liveness, not database readiness; an ingest check exercises
the DB. No admin or raw-report HTTP endpoint is exposed.

Reports are capped at 32 KiB and validated against a versioned allowlist. Admission
is limited to 20 reports/second globally and one per installation per five minutes,
with at most eight waiting database requests. IDs are client-generated: these
controls bound normal ingestion, but do not authenticate self-reported hardware.
Database report IDs deduplicate replays. Quotas reset on collector restart.

Raw reports expire after 30 days, pruned hourly in bounded batches. The analysis
views use this retained window; no permanent device inventory is kept. At 1,000
always-running installations, expect about 24,000 reports/day. At an assumed
4 KiB per report, that is about 3 GB of raw payload per 30 days, before PostgreSQL
indexes, WAL and backups. Revisit disk sizing using measured production growth.

### Schema rollout

For schema 5, deploy the collector and updated analysis scripts before releasing the
miner. A schema-4-only collector rejects the new reports. Schemas 1–4 remain accepted,
without fabricating missing detail. No database migration is needed; raw JSON retains
the added fields. Test the updated fixture against the collector, then release the miner.
Previously uploaded reports cannot recover causes they never recorded.

All schemas use the existing `/v1/reports` URL. Old reports omit newer fields and
remain readable. No database table rewrite or miner/pool restart is needed for
the collector upgrade; the performance view is replaced in place.

The pool-mining hash sampling path feeds the same recorder as daemon mining.
The pool regression test exercises its real sampling/accounting function without
running a hashing workload. A fixed binary must reach users before their missing
performance samples can recover; deploying the collector alone cannot recover
hash counts from past inventory-only reports.

### Recovery backups

For a checkout at `/opt/seine`, install the supplied daily backup timer:

```sh
sudo install -m 644 telemetry/deploy/seine-telemetry-backup.{service,timer} /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now seine-telemetry-backup.timer
sudo systemctl start seine-telemetry-backup.service
```

`backup.sh` writes private schema and compressed CSV files under
`/var/backups/seine-telemetry`. Each backup contains only the most recent seven
days of reports and expires after seven days, so backed-up records stay well
inside the 30-day raw-data limit. Expiration runs even if the database is down.
The timer must keep running; purge expired backups before bringing an offline
host back into service. Full-volume snapshots must not retain telemetry beyond
the same limit. These local backups protect against accidental database changes,
not loss of the server; an off-host copy needs the same access and expiry rules.

Test recovery into a **separate empty database**, using PostgreSQL 17 or later:

```sh
psql "$RESTORE_DATABASE_URL" -v ON_ERROR_STOP=1 -f /path/to/backup/schema.sql
gzip -dc /path/to/backup/reports.csv.gz | psql "$RESTORE_DATABASE_URL" -v ON_ERROR_STOP=1 -c 'COPY telemetry_reports (report_id, installation_id, received_at, payload) FROM STDIN WITH (FORMAT csv)'
```

Check `systemctl status seine-telemetry-backup.timer`, backup file freshness and
free disk space during operations. Restored records retain their original receipt
times and remain subject to collector cleanup.

After the first backup succeeds, install and enable
`seine-telemetry-check.{service,timer}` the same way. Its five-minute checks cover
HTTPS, database connectivity, retention, disk headroom, backup freshness and
performance-sample completeness;
failures appear in systemd/journald. It does not send external notifications.

## Analysis

### Agent analysis

Agents can handle collection checks and optimization prioritization directly:

```sh
python3 telemetry/agent_report.py
```

This runs over the existing SSH connection to the production collector (override
with `--host`), uses a read-only database transaction with bounded query time, and
prints JSON containing health, adoption by version, CPU/GPU inventory, daily
activity and qualifying performance cohorts. It exposes no installation IDs or
raw reports. No browser, dashboard, local database client or additional Python
packages are required. It exits nonzero on health, access or query failures while
preserving any successful part of the briefing.

The window is seven days, debug builds are excluded, and inventory uses each
installation's latest report. Performance groups match version, CPU, OS/version,
native/WSL runtime,
RAM/core counts, build target/features, resource limits and backend settings
(including measured CPU backing), plus exact build commit/source fingerprint and
pool/daemon mode where reported. Missing legacy fields form separate groups.
Shutdown and failure reports remain in inventory and diagnostics but cannot
contribute performance samples, even when their final window exceeds a minute.
Free memory is diagnostic only and does not split cohorts on every report. Each
installation contributes one median; identical devices
in one report do not count as additional observations. A cohort must contain at
least five installations, each with three valid reports spanning at least 24 hours.
Changed/missed sampling windows and samples under a minute are excluded. These
are conservative screening rules, not statistical confidence guarantees.

Use the results to select hardware for controlled paired experiments. Consult the
optimization logs before proposing work, and verify improvements on the target
hardware before changing automatic defaults. Sparse data means "keep collecting,"
not "hardware is slow."
A separate six-hour data-quality query counts inventory-only, sampled and stable
reports. Five installations of the same version, each with at least three reports
spanning two hours and no backend samples, fail both the agent briefing and the
five-minute service check. This flags sustained missing samples even when HTTPS
and the database are healthy. `inventory_only_clusters` separately shows observed
pool connection warnings and mining phase; neither a mining checkpoint nor the
absence of a connection warning proves that usable work was available. Low report counts alone are not an outage; unstable
tuning windows remain visible in the stable-sample count.
The `release_health` section (`release_health.sql`, seven days) covers schema-14
reporters only; `coverage` shows how many installations per version send these
fields. It reports, by version, self-update checks, installs, failed installs and
their last failed stage, and installations with automatic installs on; fork
readiness as installations and seconds paused on unsupported work (by version
and pool/daemon mode) plus `unsupported_work` events; AMD detection cohorts,
including `amd_gpu_not_mining` (the OS lists an AMD display device while HIP is
unavailable or no AMD GPU was selected, usually missing ROCm; `hip_unavailable`
alone is normal without an AMD GPU); AMD performance level by architecture and
OS; GPU kernel source by backend and architecture; and pool TLS/SOCKS/memo/backup
shares with failover, reconnect, refill and backup-time totals, per-installation
quantiles, by-version and daily series. Settings use each installation's latest
report; counts are summed over report windows. Like `diagnostics.sql`, it flags
`attention` after three installations of one version report failed installs, or
mining paused on unsupported work, within 24 hours.
Send the user a concise interpretation and next action rather than raw SQL output.

The command is invoked during agent work; it does not schedule unattended agent
runs or notifications. Existing service health checks continue every five minutes.
It neither modifies miner settings nor deploys changes. Backups remain local and
a separate stateful Discord alert timer handles fleet incidents.

### Manual SQL

Connect privately with `psql` or a preferred SQL client:

```sql
SELECT * FROM telemetry_cpu_population ORDER BY installations DESC;
SELECT * FROM telemetry_backend_performance ORDER BY installations DESC;

-- Active GPU inventory, one latest report per installation.
SELECT sample #>> '{settings,device_model}' AS gpu,
       count(DISTINCT installation_id) AS installations
FROM telemetry_installations
CROSS JOIN LATERAL jsonb_array_elements(payload->'backends') AS sample
WHERE sample #>> '{settings,backend}' IN ('nvidia', 'amd', 'metal')
GROUP BY 1 ORDER BY 2 DESC;
```

Popularity counts installations, not messages or necessarily people. Performance
first takes each installation's median, then reports population median/p10/p90
for matching version, CPU, environment, resources and backend settings. Debug
builds, unstable windows, shutdown/failure reports and samples shorter than a
minute are excluded. Exact source builds and pool/daemon modes form separate
absolute-rate groups. Release-regression alerts match workload mode while
allowing source identity to change between the compared releases. Results
describe reporting installations and may not represent all miners.

## Verification and showcase

```sh
python3 -m unittest discover -s telemetry -p 'test_a*.py'
python3 -m unittest discover -s telemetry -p 'test_performance_sql.py'
python3 -m unittest discover -s telemetry -p 'test_*_sql.py'
# Includes disposable PostgreSQL cohort/alert/diagnostic/release-health tests when PostgreSQL 16 is installed.
cargo +1.93.0 test -p seine-telemetry --features server
cargo +1.93.0 test --bin seine --no-default-features hardware_telemetry
cargo +1.93.0 check --workspace --all-targets --features seine-telemetry/server,amd
```

For a disposable local PostgreSQL and collector, set `TELEMETRY_TEST_DATABASE`
to that database's connection string, then run `python3 telemetry/test_integration.py`.
It writes synthetic reports and checks validation, throttling, legacy uploads,
storage, cohort separation, duplicate-device weighting and data-quality detection/recovery.
Do not run it against production. Set `TELEMETRY_TEST_TRIAL_FIXTURE` to the runtime
Monitor artifact to also verify producer-to-collector-to-SQL qualification.

Run the real collector against that disposable DB:

```sh
TELEMETRY_DATABASE_URL='host=127.0.0.1 port=55439 user=postgres dbname=telemetry' \
  cargo run -p seine-telemetry --features server
python3 telemetry/showcase/serve.py
```

The stable local showcase is <http://localhost:9081>. It exercises real collector
responses through a local fixture proxy. Its checkbox controls fixture submissions
only; it is not a replacement for the miner's persistent preference. It supports disabled,
ready, loading, success, validation errors, empty reports, throttling and collector
unavailability. No real miner or hardware workload is needed.
