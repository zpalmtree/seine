use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use blocknet_pow_spec::CPU_LANE_MEMORY_BYTES;
use clap::{ArgAction, Parser, ValueEnum};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, ValueEnum)]
pub enum BackendKind {
    Cpu,
    Nvidia,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum BenchKind {
    Kernel,
    Backend,
    EndToEnd,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum BenchBaselinePolicy {
    Strict,
    IgnoreEnvironment,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum CpuAffinityMode {
    Off,
    Auto,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum CpuPerformanceProfile {
    Balanced,
    Throughput,
    Efficiency,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum UiMode {
    Auto,
    Tui,
    Plain,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum WorkAllocation {
    Static,
    Adaptive,
}

const DEFAULT_HASH_POLL_MS: u64 = 200;
const DEFAULT_CPU_HASH_BATCH_SIZE: u64 = 64;
const DEFAULT_CPU_CONTROL_CHECK_INTERVAL_HASHES: u64 = 1;
const DEFAULT_CPU_HASH_FLUSH_MS: u64 = 50;
const DEFAULT_CPU_EVENT_DISPATCH_CAPACITY: usize = 256;
const DEFAULT_CPU_AUTOTUNE_SECS: u64 = 2;
const DEFAULT_CPU_AUTOTUNE_CONFIG_FILE: &str = "seine.cpu-autotune.json";

#[derive(Debug, Clone, Copy)]
struct CpuProfileDefaults {
    threads: usize,
    hash_poll_ms: u64,
    cpu_hash_batch_size: u64,
    cpu_control_check_interval_hashes: u64,
    cpu_hash_flush_ms: u64,
    cpu_event_dispatch_capacity: usize,
}

fn cpu_profile_defaults(
    profile: CpuPerformanceProfile,
    auto_threads_cap: usize,
) -> CpuProfileDefaults {
    let auto_threads_cap = auto_threads_cap.max(1);
    match profile {
        CpuPerformanceProfile::Balanced => CpuProfileDefaults {
            threads: auto_threads_cap,
            hash_poll_ms: DEFAULT_HASH_POLL_MS,
            cpu_hash_batch_size: DEFAULT_CPU_HASH_BATCH_SIZE,
            cpu_control_check_interval_hashes: DEFAULT_CPU_CONTROL_CHECK_INTERVAL_HASHES,
            cpu_hash_flush_ms: DEFAULT_CPU_HASH_FLUSH_MS,
            cpu_event_dispatch_capacity: DEFAULT_CPU_EVENT_DISPATCH_CAPACITY,
        },
        CpuPerformanceProfile::Throughput => CpuProfileDefaults {
            threads: auto_threads_cap,
            hash_poll_ms: 100,
            cpu_hash_batch_size: 256,
            cpu_control_check_interval_hashes: 8,
            cpu_hash_flush_ms: 200,
            cpu_event_dispatch_capacity: 1024,
        },
        CpuPerformanceProfile::Efficiency => CpuProfileDefaults {
            threads: (auto_threads_cap / 2).max(1),
            hash_poll_ms: 400,
            cpu_hash_batch_size: 32,
            cpu_control_check_interval_hashes: 1,
            cpu_hash_flush_ms: 25,
            cpu_event_dispatch_capacity: 128,
        },
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct BackendSpec {
    pub kind: BackendKind,
    pub device_index: Option<u32>,
    pub cpu_threads: Option<usize>,
    pub cpu_affinity: Option<CpuAffinityMode>,
    pub assign_timeout_override: Option<Duration>,
    pub control_timeout_override: Option<Duration>,
    pub assign_timeout_strikes_override: Option<u32>,
}

#[derive(Debug, Parser)]
#[command(name = "seine", version, about = "Seine net miner for Blocknet")]
struct Cli {
    /// API base URL for the blocknet daemon.
    #[arg(long = "api-url", default_value = "http://127.0.0.1:8332")]
    api_url: String,

    /// Bearer token for authenticated API access.
    #[arg(long)]
    token: Option<String>,

    /// Wallet password used for automatic wallet load when daemon starts without a wallet.
    #[arg(long)]
    wallet_password: Option<String>,

    /// Path to a file containing the wallet password for automatic wallet load.
    #[arg(long)]
    wallet_password_file: Option<PathBuf>,

    /// Path to api.cookie file (defaults to <data-dir>/api.cookie).
    #[arg(long)]
    cookie: Option<PathBuf>,

    /// Daemon data directory (used to locate api.cookie when --cookie is unset).
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// One or more mining backends. Repeat the flag or pass comma-separated values.
    #[arg(
        long = "backend",
        value_enum,
        value_delimiter = ',',
        num_args = 1..,
        default_value = "cpu"
    )]
    backends: Vec<BackendKind>,

    /// Explicit NVIDIA device indices; creates one NVIDIA backend instance per index.
    /// Requires selecting NVIDIA in --backend.
    #[arg(long = "nvidia-devices", value_delimiter = ',', num_args = 1..)]
    nvidia_devices: Vec<u32>,

    /// Number of CPU mining threads per CPU backend instance (each uses ~2GB RAM for Argon2id).
    /// When omitted, threads auto-size from available CPU parallelism and RAM budget.
    #[arg(long, alias = "cpu-threads")]
    threads: Option<usize>,

    /// CPU pinning policy for CPU mining workers.
    #[arg(long, value_enum, default_value_t = CpuAffinityMode::Auto)]
    cpu_affinity: CpuAffinityMode,

    /// CPU mining profile preset; applies default threads/poll/flush knobs unless explicitly set.
    #[arg(long = "cpu-profile", value_enum, default_value_t = CpuPerformanceProfile::Balanced)]
    cpu_profile: CpuPerformanceProfile,

    /// Optional per-CPU-instance thread counts (comma-separated).
    /// Length must match the number of configured CPU backend instances.
    #[arg(long = "cpu-threads-per-instance", value_delimiter = ',', num_args = 1..)]
    cpu_threads_per_instance: Vec<usize>,

    /// Optional per-CPU-instance affinity policies (comma-separated).
    /// Length must match the number of configured CPU backend instances.
    #[arg(
        long = "cpu-affinity-per-instance",
        value_enum,
        value_delimiter = ',',
        num_args = 1..
    )]
    cpu_affinity_per_instance: Vec<CpuAffinityMode>,

    /// Allow starting when configured CPU lanes exceed detected system RAM.
    #[arg(long, default_value_t = false)]
    allow_oversubscribe: bool,

    /// Maximum time to work on one block template before refreshing.
    #[arg(long, default_value_t = 20)]
    refresh_secs: u64,

    /// Timeout for JSON API requests (blocktemplate/submitblock/wallet/load), in seconds.
    #[arg(long, default_value_t = 10)]
    request_timeout_secs: u64,

    /// Timeout for each SSE stream connection attempt, in seconds.
    #[arg(long, default_value_t = 10)]
    events_stream_timeout_secs: u64,

    /// Maximum lifetime for one SSE stream request before reconnecting, in seconds.
    #[arg(long, default_value_t = 90)]
    events_idle_timeout_secs: u64,

    /// Interval for periodic stats printing.
    #[arg(long, default_value_t = 10)]
    stats_secs: u64,

    /// Capacity of the bounded backend event queue.
    #[arg(long, default_value_t = 1024)]
    backend_event_capacity: usize,

    /// Backend hash polling interval in milliseconds.
    #[arg(long)]
    hash_poll_ms: Option<u64>,

    /// CPU worker hash flush batch size.
    #[arg(long)]
    cpu_hash_batch_size: Option<u64>,

    /// CPU worker control polling cadence in hashes (deadline checks are also time-bounded).
    #[arg(long)]
    cpu_control_check_interval_hashes: Option<u64>,

    /// CPU worker hash flush interval in milliseconds.
    #[arg(long)]
    cpu_hash_flush_ms: Option<u64>,

    /// CPU backend internal event dispatch queue capacity.
    #[arg(long)]
    cpu_event_dispatch_capacity: Option<usize>,

    /// Enable startup CPU kernel autotuning and select the best thread count by measured H/s.
    #[arg(long, action = ArgAction::SetTrue)]
    cpu_autotune_threads: bool,

    /// Disable startup CPU kernel autotuning.
    #[arg(long, action = ArgAction::SetTrue)]
    disable_cpu_autotune_threads: bool,

    /// Minimum per-instance CPU thread count considered by autotuner.
    #[arg(long, default_value_t = 1)]
    cpu_autotune_min_threads: usize,

    /// Maximum per-instance CPU thread count considered by autotuner.
    #[arg(long)]
    cpu_autotune_max_threads: Option<usize>,

    /// Benchmark duration (seconds) for each autotuner thread candidate.
    #[arg(long, default_value_t = DEFAULT_CPU_AUTOTUNE_SECS)]
    cpu_autotune_secs: u64,

    /// Path for persisted CPU autotune result (default: <data-dir>/seine.cpu-autotune.json).
    #[arg(long)]
    cpu_autotune_config: Option<PathBuf>,

    /// Maximum time to wait for one backend assignment dispatch call, in milliseconds.
    #[arg(long, default_value_t = 1000)]
    backend_assign_timeout_ms: u64,

    /// Optional per-backend-instance assignment dispatch timeouts in milliseconds.
    /// Length must match total backend instances after expansion.
    #[arg(
        long = "backend-assign-timeout-ms-per-instance",
        value_delimiter = ',',
        num_args = 1..
    )]
    backend_assign_timeout_ms_per_instance: Vec<u64>,

    /// Consecutive assignment timeouts before backend quarantine.
    #[arg(long, default_value_t = 3)]
    backend_assign_timeout_strikes: u32,

    /// Optional per-backend-instance assignment-timeout strike thresholds.
    /// Length must match total backend instances after expansion.
    #[arg(
        long = "backend-assign-timeout-strikes-per-instance",
        value_delimiter = ',',
        num_args = 1..
    )]
    backend_assign_timeout_strikes_per_instance: Vec<u32>,

    /// Maximum time to wait for backend cancel/fence control calls, in milliseconds.
    #[arg(long, default_value_t = 60_000)]
    backend_control_timeout_ms: u64,

    /// Optional per-backend-instance cancel/fence timeouts in milliseconds.
    /// Length must match total backend instances after expansion.
    #[arg(
        long = "backend-control-timeout-ms-per-instance",
        value_delimiter = ',',
        num_args = 1..
    )]
    backend_control_timeout_ms_per_instance: Vec<u64>,

    /// Allow backends with best-effort deadline semantics.
    #[arg(long, default_value_t = false)]
    allow_best_effort_deadlines: bool,

    /// Maximum time to wait for a prefetched block template before falling back.
    #[arg(long, default_value_t = 250)]
    prefetch_wait_ms: u64,

    /// Maximum time to wait for tip-listener shutdown before detaching.
    #[arg(long, default_value_t = 250)]
    tip_listener_join_wait_ms: u64,

    /// Maximum time to wait for submit-worker shutdown before detaching.
    #[arg(long, default_value_t = 2_000)]
    submit_join_wait_ms: u64,

    /// Enable strict per-round quiesce barriers for exact accounting (lower throughput).
    #[arg(long, action = ArgAction::SetTrue)]
    strict_round_accounting: bool,

    /// Deprecated alias kept for compatibility; relaxed accounting is now the default.
    #[arg(long, action = ArgAction::SetTrue, hide = true)]
    relaxed_accounting: bool,

    /// Optional nonce seed.
    #[arg(long)]
    start_nonce: Option<u64>,

    /// Maximum iterations each lane will scan before switching to next reservation.
    #[arg(long, default_value_t = 1u64 << 36)]
    nonce_iters_per_lane: u64,

    /// Nonce chunk allocation strategy across active backends.
    #[arg(long, value_enum, default_value_t = WorkAllocation::Adaptive)]
    work_allocation: WorkAllocation,

    /// Optional periodic in-round redistribution interval in milliseconds (0 disables).
    #[arg(long, default_value_t = 0)]
    sub_round_rebalance_ms: u64,

    /// Disable SSE tip notifications (/api/events) and rely only on refresh timer.
    #[arg(long, action = ArgAction::SetTrue)]
    disable_sse: bool,

    /// Refresh on same-height new_block hash changes (disabled by default to avoid replay churn).
    #[arg(long, action = ArgAction::SetTrue)]
    refresh_on_same_height: bool,

    /// UI mode: auto (TTY-detected), tui (force full-screen TUI), plain (stdout logs only).
    #[arg(long, value_enum, default_value_t = UiMode::Auto)]
    ui: UiMode,

    /// Run local performance benchmark instead of mining over API.
    #[arg(long, default_value_t = false)]
    bench: bool,

    /// Benchmark mode.
    #[arg(long, value_enum, default_value_t = BenchKind::Backend)]
    bench_kind: BenchKind,

    /// Per-round benchmark duration in seconds.
    #[arg(long, default_value_t = 20)]
    bench_secs: u64,

    /// Number of benchmark rounds.
    #[arg(long, default_value_t = 3)]
    bench_rounds: u32,

    /// Number of warmup rounds run before measured benchmark rounds.
    #[arg(long, default_value_t = 0)]
    bench_warmup_rounds: u32,

    /// Write benchmark report JSON to this file.
    #[arg(long)]
    bench_output: Option<PathBuf>,

    /// Compare benchmark average H/s against a previous JSON report.
    #[arg(long)]
    bench_baseline: Option<PathBuf>,

    /// Fail benchmark run if average H/s regresses below baseline by more than this percent.
    #[arg(long)]
    bench_fail_below_pct: Option<f64>,

    /// Baseline compatibility mode: strict validates environment/build identity;
    /// ignore-environment allows cross-build comparisons while keeping runtime/config/PoW checks.
    #[arg(long, value_enum, default_value_t = BenchBaselinePolicy::Strict)]
    bench_baseline_policy: BenchBaselinePolicy,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub api_url: String,
    pub token: Option<String>,
    pub token_cookie_path: Option<PathBuf>,
    pub wallet_password: Option<String>,
    pub wallet_password_file: Option<PathBuf>,
    pub backend_specs: Vec<BackendSpec>,
    pub threads: usize,
    pub cpu_auto_threads_cap: usize,
    pub cpu_affinity: CpuAffinityMode,
    pub cpu_profile: CpuPerformanceProfile,
    pub refresh_interval: Duration,
    pub request_timeout: Duration,
    pub events_stream_timeout: Duration,
    pub events_idle_timeout: Duration,
    pub stats_interval: Duration,
    pub backend_event_capacity: usize,
    pub hash_poll_interval: Duration,
    pub cpu_hash_batch_size: u64,
    pub cpu_control_check_interval_hashes: u64,
    pub cpu_hash_flush_interval: Duration,
    pub cpu_event_dispatch_capacity: usize,
    pub cpu_autotune_threads: bool,
    pub cpu_autotune_min_threads: usize,
    pub cpu_autotune_max_threads: Option<usize>,
    pub cpu_autotune_secs: u64,
    pub cpu_autotune_config_path: PathBuf,
    pub backend_assign_timeout: Duration,
    pub backend_assign_timeout_strikes: u32,
    pub backend_control_timeout: Duration,
    pub allow_best_effort_deadlines: bool,
    pub prefetch_wait: Duration,
    pub tip_listener_join_wait: Duration,
    pub submit_join_wait: Duration,
    pub strict_round_accounting: bool,
    pub start_nonce: u64,
    pub nonce_iters_per_lane: u64,
    pub work_allocation: WorkAllocation,
    pub sub_round_rebalance_interval: Option<Duration>,
    pub sse_enabled: bool,
    pub refresh_on_same_height: bool,
    pub ui_mode: UiMode,
    pub bench: bool,
    pub bench_kind: BenchKind,
    pub bench_secs: u64,
    pub bench_rounds: u32,
    pub bench_warmup_rounds: u32,
    pub bench_output: Option<PathBuf>,
    pub bench_baseline: Option<PathBuf>,
    pub bench_fail_below_pct: Option<f64>,
    pub bench_baseline_policy: BenchBaselinePolicy,
}

impl Config {
    pub fn parse() -> Result<Self> {
        let cli = Cli::parse();
        if let Some(threads) = cli.threads {
            if threads == 0 {
                bail!("threads must be >= 1");
            }
        }
        if cli.nonce_iters_per_lane == 0 {
            bail!("nonce-iters-per-lane must be >= 1");
        }
        if cli.backend_event_capacity == 0 {
            bail!("backend-event-capacity must be >= 1");
        }
        if let Some(value) = cli.hash_poll_ms {
            if value == 0 {
                bail!("hash-poll-ms must be >= 1");
            }
        }
        if let Some(value) = cli.cpu_hash_batch_size {
            if value == 0 {
                bail!("cpu-hash-batch-size must be >= 1");
            }
        }
        if let Some(value) = cli.cpu_control_check_interval_hashes {
            if value == 0 {
                bail!("cpu-control-check-interval-hashes must be >= 1");
            }
        }
        if let Some(value) = cli.cpu_hash_flush_ms {
            if value == 0 {
                bail!("cpu-hash-flush-ms must be >= 1");
            }
        }
        if let Some(value) = cli.cpu_event_dispatch_capacity {
            if value == 0 {
                bail!("cpu-event-dispatch-capacity must be >= 1");
            }
        }
        if cli.cpu_autotune_threads && cli.disable_cpu_autotune_threads {
            bail!("cannot use both --cpu-autotune-threads and --disable-cpu-autotune-threads");
        }
        if cli.cpu_autotune_min_threads == 0 {
            bail!("cpu-autotune-min-threads must be >= 1");
        }
        if let Some(max_threads) = cli.cpu_autotune_max_threads {
            if max_threads == 0 {
                bail!("cpu-autotune-max-threads must be >= 1");
            }
            if max_threads < cli.cpu_autotune_min_threads {
                bail!(
                    "cpu-autotune-max-threads ({max_threads}) must be >= cpu-autotune-min-threads ({})",
                    cli.cpu_autotune_min_threads
                );
            }
        }
        if cli.cpu_autotune_secs == 0 {
            bail!("cpu-autotune-secs must be >= 1");
        }
        if cli.backend_assign_timeout_ms == 0 {
            bail!("backend-assign-timeout-ms must be >= 1");
        }
        if cli.backend_assign_timeout_ms_per_instance.contains(&0) {
            bail!("backend-assign-timeout-ms-per-instance values must be >= 1");
        }
        if cli.backend_assign_timeout_strikes == 0 {
            bail!("backend-assign-timeout-strikes must be >= 1");
        }
        if cli.backend_assign_timeout_strikes_per_instance.contains(&0) {
            bail!("backend-assign-timeout-strikes-per-instance values must be >= 1");
        }
        if cli.backend_control_timeout_ms == 0 {
            bail!("backend-control-timeout-ms must be >= 1");
        }
        if cli.backend_control_timeout_ms_per_instance.contains(&0) {
            bail!("backend-control-timeout-ms-per-instance values must be >= 1");
        }
        if cli.prefetch_wait_ms == 0 {
            bail!("prefetch-wait-ms must be >= 1");
        }
        if cli.tip_listener_join_wait_ms == 0 {
            bail!("tip-listener-join-wait-ms must be >= 1");
        }
        if cli.submit_join_wait_ms == 0 {
            bail!("submit-join-wait-ms must be >= 1");
        }
        if cli.strict_round_accounting && cli.relaxed_accounting {
            bail!("cannot use both --strict-round-accounting and --relaxed-accounting");
        }
        if let Some(threshold) = cli.bench_fail_below_pct {
            if !cli.bench {
                bail!("--bench-fail-below-pct can only be used with --bench");
            }
            if !threshold.is_finite() || threshold < 0.0 {
                bail!("--bench-fail-below-pct must be a finite number >= 0");
            }
            if cli.bench_baseline.is_none() {
                bail!("--bench-fail-below-pct requires --bench-baseline");
            }
        }

        let backends = cli.backends.clone();
        if backends.is_empty() {
            bail!("at least one backend is required");
        }
        let cpu_backend_instances = backends
            .iter()
            .filter(|kind| matches!(kind, BackendKind::Cpu))
            .count();
        let auto_threads_cap = match cpu_backend_instances {
            0 => 1,
            instances => auto_cpu_threads(instances),
        };
        let profile_defaults = cpu_profile_defaults(cli.cpu_profile, auto_threads_cap);
        let resolved_threads = cli.threads.unwrap_or(profile_defaults.threads);
        let cpu_autotune_default_enabled = cli.threads.is_none() && cpu_backend_instances > 0;
        let cpu_autotune_threads = if cli.disable_cpu_autotune_threads {
            false
        } else {
            cli.cpu_autotune_threads || cpu_autotune_default_enabled
        };
        let cpu_autotune_config_path = cli
            .cpu_autotune_config
            .clone()
            .unwrap_or_else(|| cli.data_dir.join(DEFAULT_CPU_AUTOTUNE_CONFIG_FILE));
        let effective_hash_poll_ms = cli.hash_poll_ms.unwrap_or(profile_defaults.hash_poll_ms);
        let effective_cpu_hash_batch_size = cli
            .cpu_hash_batch_size
            .unwrap_or(profile_defaults.cpu_hash_batch_size);
        let effective_cpu_control_check_interval_hashes = cli
            .cpu_control_check_interval_hashes
            .unwrap_or(profile_defaults.cpu_control_check_interval_hashes);
        let effective_cpu_hash_flush_ms = cli
            .cpu_hash_flush_ms
            .unwrap_or(profile_defaults.cpu_hash_flush_ms);
        let effective_cpu_event_dispatch_capacity = cli
            .cpu_event_dispatch_capacity
            .unwrap_or(profile_defaults.cpu_event_dispatch_capacity);
        let backend_specs = expand_backend_specs(
            &backends,
            &cli.nvidia_devices,
            BackendExpansionOptions {
                default_cpu_threads: resolved_threads,
                default_cpu_affinity: cli.cpu_affinity,
                cpu_threads_per_instance: &cli.cpu_threads_per_instance,
                cpu_affinity_per_instance: &cli.cpu_affinity_per_instance,
                backend_assign_timeout_ms_per_instance: &cli.backend_assign_timeout_ms_per_instance,
                backend_control_timeout_ms_per_instance: &cli
                    .backend_control_timeout_ms_per_instance,
                backend_assign_timeout_strikes_per_instance: &cli
                    .backend_assign_timeout_strikes_per_instance,
            },
        )?;

        validate_cpu_memory(&backend_specs, resolved_threads, cli.allow_oversubscribe)?;

        let (token, token_cookie_path) = if cli.bench {
            (None, None)
        } else {
            let (token, cookie_path) = resolve_token_with_source(&cli)?;
            (Some(token), cookie_path)
        };
        let api_url = normalize_api_url(&cli.api_url);

        Ok(Self {
            api_url,
            token,
            token_cookie_path,
            wallet_password: cli.wallet_password,
            wallet_password_file: cli.wallet_password_file,
            backend_specs,
            threads: resolved_threads,
            cpu_auto_threads_cap: auto_threads_cap,
            cpu_affinity: cli.cpu_affinity,
            cpu_profile: cli.cpu_profile,
            refresh_interval: Duration::from_secs(cli.refresh_secs.max(1)),
            request_timeout: Duration::from_secs(cli.request_timeout_secs.max(1)),
            events_stream_timeout: Duration::from_secs(cli.events_stream_timeout_secs.max(1)),
            events_idle_timeout: Duration::from_secs(cli.events_idle_timeout_secs.max(1)),
            stats_interval: Duration::from_secs(cli.stats_secs.max(1)),
            backend_event_capacity: cli.backend_event_capacity,
            hash_poll_interval: Duration::from_millis(effective_hash_poll_ms),
            cpu_hash_batch_size: effective_cpu_hash_batch_size,
            cpu_control_check_interval_hashes: effective_cpu_control_check_interval_hashes,
            cpu_hash_flush_interval: Duration::from_millis(effective_cpu_hash_flush_ms),
            cpu_event_dispatch_capacity: effective_cpu_event_dispatch_capacity,
            cpu_autotune_threads,
            cpu_autotune_min_threads: cli.cpu_autotune_min_threads,
            cpu_autotune_max_threads: cli.cpu_autotune_max_threads,
            cpu_autotune_secs: cli.cpu_autotune_secs.max(1),
            cpu_autotune_config_path,
            backend_assign_timeout: Duration::from_millis(cli.backend_assign_timeout_ms),
            backend_assign_timeout_strikes: cli.backend_assign_timeout_strikes.max(1),
            backend_control_timeout: Duration::from_millis(cli.backend_control_timeout_ms),
            allow_best_effort_deadlines: cli.allow_best_effort_deadlines,
            prefetch_wait: Duration::from_millis(cli.prefetch_wait_ms),
            tip_listener_join_wait: Duration::from_millis(cli.tip_listener_join_wait_ms),
            submit_join_wait: Duration::from_millis(cli.submit_join_wait_ms),
            strict_round_accounting: cli.strict_round_accounting && !cli.relaxed_accounting,
            start_nonce: cli.start_nonce.unwrap_or_else(|| {
                if cli.bench {
                    0
                } else {
                    default_nonce_seed()
                }
            }),
            nonce_iters_per_lane: cli.nonce_iters_per_lane,
            work_allocation: cli.work_allocation,
            sub_round_rebalance_interval: optional_duration_from_millis(cli.sub_round_rebalance_ms),
            sse_enabled: !cli.disable_sse,
            refresh_on_same_height: cli.refresh_on_same_height,
            ui_mode: cli.ui,
            bench: cli.bench,
            bench_kind: cli.bench_kind,
            bench_secs: cli.bench_secs.max(1),
            bench_rounds: cli.bench_rounds.max(1),
            bench_warmup_rounds: cli.bench_warmup_rounds,
            bench_output: cli.bench_output,
            bench_baseline: cli.bench_baseline,
            bench_fail_below_pct: cli.bench_fail_below_pct,
            bench_baseline_policy: cli.bench_baseline_policy,
        })
    }
}

fn resolve_token_with_source(cli: &Cli) -> Result<(String, Option<PathBuf>)> {
    if let Some(token) = &cli.token {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            bail!("--token is empty");
        }
        return Ok((trimmed.to_string(), None));
    }

    let cookie_path = cli
        .cookie
        .clone()
        .unwrap_or_else(|| cli.data_dir.join("api.cookie"));

    let token = read_token_from_cookie_file(&cookie_path)?;
    Ok((token, Some(cookie_path)))
}

pub fn read_token_from_cookie_file(cookie_path: &Path) -> Result<String> {
    let token = fs::read_to_string(cookie_path)
        .with_context(|| format!("failed to read cookie file at {}", cookie_path.display()))?;

    let trimmed = token.trim();
    if trimmed.is_empty() {
        bail!("cookie file is empty: {}", cookie_path.display());
    }

    Ok(trimmed.to_string())
}

fn optional_duration_from_millis(value: u64) -> Option<Duration> {
    if value == 0 {
        None
    } else {
        Some(Duration::from_millis(value))
    }
}

fn normalize_api_url(input: &str) -> String {
    let trimmed = input.trim().trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return trimmed.to_string();
    }
    format!("http://{trimmed}")
}

fn default_nonce_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn dedupe_device_indexes(device_indexes: &[u32]) -> Vec<u32> {
    let mut seen = HashSet::new();
    let mut ordered = Vec::with_capacity(device_indexes.len());
    for device_index in device_indexes {
        if seen.insert(*device_index) {
            ordered.push(*device_index);
        }
    }
    ordered
}

#[derive(Debug, Clone, Copy)]
struct BackendExpansionOptions<'a> {
    default_cpu_threads: usize,
    default_cpu_affinity: CpuAffinityMode,
    cpu_threads_per_instance: &'a [usize],
    cpu_affinity_per_instance: &'a [CpuAffinityMode],
    backend_assign_timeout_ms_per_instance: &'a [u64],
    backend_control_timeout_ms_per_instance: &'a [u64],
    backend_assign_timeout_strikes_per_instance: &'a [u32],
}

fn expand_backend_specs(
    backends: &[BackendKind],
    nvidia_devices: &[u32],
    options: BackendExpansionOptions<'_>,
) -> Result<Vec<BackendSpec>> {
    if !nvidia_devices.is_empty() && !backends.contains(&BackendKind::Nvidia) {
        bail!("--nvidia-devices requires selecting nvidia in --backend");
    }

    let nvidia_devices = dedupe_device_indexes(nvidia_devices);
    let mut specs = Vec::new();
    for backend in backends {
        match backend {
            BackendKind::Cpu => specs.push(BackendSpec {
                kind: BackendKind::Cpu,
                device_index: None,
                cpu_threads: None,
                cpu_affinity: None,
                assign_timeout_override: None,
                control_timeout_override: None,
                assign_timeout_strikes_override: None,
            }),
            BackendKind::Nvidia => {
                if nvidia_devices.is_empty() {
                    specs.push(BackendSpec {
                        kind: BackendKind::Nvidia,
                        device_index: None,
                        cpu_threads: None,
                        cpu_affinity: None,
                        assign_timeout_override: None,
                        control_timeout_override: None,
                        assign_timeout_strikes_override: None,
                    });
                } else {
                    for device_index in &nvidia_devices {
                        specs.push(BackendSpec {
                            kind: BackendKind::Nvidia,
                            device_index: Some(*device_index),
                            cpu_threads: None,
                            cpu_affinity: None,
                            assign_timeout_override: None,
                            control_timeout_override: None,
                            assign_timeout_strikes_override: None,
                        });
                    }
                }
            }
        }
    }

    if specs.is_empty() {
        bail!("at least one backend is required");
    }

    let cpu_instance_indexes = specs
        .iter()
        .enumerate()
        .filter_map(|(idx, spec)| (spec.kind == BackendKind::Cpu).then_some(idx))
        .collect::<Vec<_>>();

    if !options.cpu_threads_per_instance.is_empty()
        && options.cpu_threads_per_instance.len() != cpu_instance_indexes.len()
    {
        bail!(
            "--cpu-threads-per-instance length ({}) must match CPU backend instances ({})",
            options.cpu_threads_per_instance.len(),
            cpu_instance_indexes.len()
        );
    }
    if !options.cpu_affinity_per_instance.is_empty()
        && options.cpu_affinity_per_instance.len() != cpu_instance_indexes.len()
    {
        bail!(
            "--cpu-affinity-per-instance length ({}) must match CPU backend instances ({})",
            options.cpu_affinity_per_instance.len(),
            cpu_instance_indexes.len()
        );
    }

    for (cpu_idx, spec_idx) in cpu_instance_indexes.iter().copied().enumerate() {
        let threads = options
            .cpu_threads_per_instance
            .get(cpu_idx)
            .copied()
            .unwrap_or(options.default_cpu_threads)
            .max(1);
        let affinity = options
            .cpu_affinity_per_instance
            .get(cpu_idx)
            .copied()
            .unwrap_or(options.default_cpu_affinity);
        specs[spec_idx].cpu_threads = Some(threads);
        specs[spec_idx].cpu_affinity = Some(affinity);
    }

    let instance_count = specs.len();
    if !options.backend_assign_timeout_ms_per_instance.is_empty()
        && options.backend_assign_timeout_ms_per_instance.len() != instance_count
    {
        bail!(
            "--backend-assign-timeout-ms-per-instance length ({}) must match backend instances ({})",
            options.backend_assign_timeout_ms_per_instance.len(),
            instance_count
        );
    }
    if !options.backend_control_timeout_ms_per_instance.is_empty()
        && options.backend_control_timeout_ms_per_instance.len() != instance_count
    {
        bail!(
            "--backend-control-timeout-ms-per-instance length ({}) must match backend instances ({})",
            options.backend_control_timeout_ms_per_instance.len(),
            instance_count
        );
    }
    if !options
        .backend_assign_timeout_strikes_per_instance
        .is_empty()
        && options.backend_assign_timeout_strikes_per_instance.len() != instance_count
    {
        bail!(
            "--backend-assign-timeout-strikes-per-instance length ({}) must match backend instances ({})",
            options.backend_assign_timeout_strikes_per_instance.len(),
            instance_count
        );
    }

    for (idx, spec) in specs.iter_mut().enumerate() {
        spec.assign_timeout_override = options
            .backend_assign_timeout_ms_per_instance
            .get(idx)
            .map(|value| Duration::from_millis(*value));
        spec.control_timeout_override = options
            .backend_control_timeout_ms_per_instance
            .get(idx)
            .map(|value| Duration::from_millis(*value));
        spec.assign_timeout_strikes_override = options
            .backend_assign_timeout_strikes_per_instance
            .get(idx)
            .copied();
    }

    Ok(specs)
}

fn auto_cpu_threads(cpu_backend_instances: usize) -> usize {
    let cpu_parallelism = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1)
        .max(1);
    let memory_cap_total = detect_memory_budget_bytes()
        .map(|budget| (budget.effective_available / CPU_LANE_MEMORY_BYTES) as usize)
        .unwrap_or(cpu_parallelism)
        .max(1);
    let total_lanes_cap = cpu_parallelism.min(memory_cap_total).max(1);
    distribute_auto_cpu_threads(total_lanes_cap, cpu_backend_instances)
}

fn distribute_auto_cpu_threads(total_lanes_cap: usize, cpu_backend_instances: usize) -> usize {
    let lanes = total_lanes_cap.max(1);
    let instances = cpu_backend_instances.max(1);
    (lanes / instances).max(1)
}

fn validate_cpu_memory(
    backend_specs: &[BackendSpec],
    threads: usize,
    allow_oversubscribe: bool,
) -> Result<()> {
    let cpu_lane_configs = backend_specs
        .iter()
        .filter(|spec| spec.kind == BackendKind::Cpu)
        .map(|spec| spec.cpu_threads.unwrap_or(threads).max(1) as u64)
        .collect::<Vec<_>>();
    if cpu_lane_configs.is_empty() {
        return Ok(());
    }

    let total_cpu_lanes = cpu_lane_configs
        .iter()
        .fold(0u64, |acc, lanes| acc.saturating_add(*lanes));
    let required = CPU_LANE_MEMORY_BYTES.saturating_mul(total_cpu_lanes);
    let Some(budget) = detect_memory_budget_bytes() else {
        return Ok(());
    };

    if required > budget.effective_total && !allow_oversubscribe {
        bail!(
            "configured CPU lanes need ~{} RAM ({} CPU lane(s) * 2GB), but effective memory limit is ~{}. Use fewer CPU lanes or pass --allow-oversubscribe to bypass this safety check.",
            human_bytes(required),
            total_cpu_lanes,
            human_bytes(budget.effective_total),
        );
    }

    if required > budget.effective_available && !allow_oversubscribe {
        bail!(
            "configured CPU lanes need ~{} RAM ({} CPU lane(s) * 2GB), but currently available memory is ~{} (effective limit ~{}). Reduce CPU lanes or pass --allow-oversubscribe if you accept potential swap/OOM risk.",
            human_bytes(required),
            total_cpu_lanes,
            human_bytes(budget.effective_available),
            human_bytes(budget.effective_total),
        );
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct MemoryBudgetBytes {
    effective_total: u64,
    effective_available: u64,
}

fn detect_memory_budget_bytes() -> Option<MemoryBudgetBytes> {
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();

    let total = sys.total_memory();
    if total == 0 {
        None
    } else {
        let mut effective_total = total;
        let mut effective_available = sys.available_memory();
        if effective_available == 0 {
            effective_available = total;
        }

        if let Some(cgroup) = sys.cgroup_limits() {
            if cgroup.total_memory > 0 {
                effective_total = effective_total.min(cgroup.total_memory);
            }
            if cgroup.free_memory > 0 {
                effective_available = effective_available.min(cgroup.free_memory);
            }
        }

        effective_available = effective_available.min(effective_total);
        Some(MemoryBudgetBytes {
            effective_total,
            effective_available,
        })
    }
}

fn human_bytes(bytes: u64) -> String {
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;

    if bytes >= 1024 * 1024 * 1024 {
        return format!("{:.2} GiB", (bytes as f64) / GIB);
    }
    format!("{:.2} MiB", (bytes as f64) / MIB)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be >= unix epoch")
            .as_nanos();
        dir.push(format!("seine-test-{}-{}", std::process::id(), now));
        dir
    }

    fn sample_cli() -> Cli {
        Cli {
            api_url: "http://127.0.0.1:8332".to_string(),
            token: None,
            wallet_password: None,
            wallet_password_file: None,
            cookie: None,
            data_dir: PathBuf::from("./data"),
            backends: vec![BackendKind::Cpu],
            nvidia_devices: Vec::new(),
            threads: Some(1),
            cpu_affinity: CpuAffinityMode::Auto,
            cpu_profile: CpuPerformanceProfile::Balanced,
            cpu_threads_per_instance: Vec::new(),
            cpu_affinity_per_instance: Vec::new(),
            allow_oversubscribe: false,
            refresh_secs: 20,
            request_timeout_secs: 10,
            events_stream_timeout_secs: 10,
            events_idle_timeout_secs: 90,
            stats_secs: 10,
            backend_event_capacity: 1024,
            hash_poll_ms: Some(DEFAULT_HASH_POLL_MS),
            cpu_hash_batch_size: Some(DEFAULT_CPU_HASH_BATCH_SIZE),
            cpu_control_check_interval_hashes: Some(DEFAULT_CPU_CONTROL_CHECK_INTERVAL_HASHES),
            cpu_hash_flush_ms: Some(DEFAULT_CPU_HASH_FLUSH_MS),
            cpu_event_dispatch_capacity: Some(DEFAULT_CPU_EVENT_DISPATCH_CAPACITY),
            cpu_autotune_threads: false,
            disable_cpu_autotune_threads: false,
            cpu_autotune_min_threads: 1,
            cpu_autotune_max_threads: None,
            cpu_autotune_secs: DEFAULT_CPU_AUTOTUNE_SECS,
            cpu_autotune_config: None,
            backend_assign_timeout_ms: 1000,
            backend_assign_timeout_ms_per_instance: Vec::new(),
            backend_assign_timeout_strikes: 3,
            backend_assign_timeout_strikes_per_instance: Vec::new(),
            backend_control_timeout_ms: 60_000,
            backend_control_timeout_ms_per_instance: Vec::new(),
            allow_best_effort_deadlines: false,
            prefetch_wait_ms: 250,
            tip_listener_join_wait_ms: 250,
            submit_join_wait_ms: 2_000,
            strict_round_accounting: false,
            relaxed_accounting: false,
            start_nonce: None,
            nonce_iters_per_lane: 1u64 << 36,
            work_allocation: WorkAllocation::Adaptive,
            sub_round_rebalance_ms: 0,
            disable_sse: false,
            refresh_on_same_height: false,
            ui: UiMode::Auto,
            bench: false,
            bench_kind: BenchKind::Backend,
            bench_secs: 20,
            bench_rounds: 3,
            bench_warmup_rounds: 0,
            bench_output: None,
            bench_baseline: None,
            bench_fail_below_pct: None,
            bench_baseline_policy: BenchBaselinePolicy::Strict,
        }
    }

    #[test]
    fn normalize_api_url_adds_scheme() {
        assert_eq!(
            normalize_api_url("127.0.0.1:8332/"),
            "http://127.0.0.1:8332"
        );
        assert_eq!(
            normalize_api_url("https://node.example.com/"),
            "https://node.example.com"
        );
    }

    #[test]
    fn resolve_token_prefers_explicit_token() {
        let mut cli = sample_cli();
        cli.token = Some("  abc123  ".to_string());

        let (token, source) = resolve_token_with_source(&cli).expect("token should parse");
        assert_eq!(token, "abc123");
        assert!(source.is_none());
    }

    #[test]
    fn resolve_token_reads_cookie() {
        let dir = unique_temp_dir();
        fs::create_dir_all(&dir).expect("temp dir should be created");
        let cookie = dir.join("api.cookie");
        fs::write(&cookie, "deadbeef\n").expect("cookie should be written");

        let mut cli = sample_cli();
        cli.cookie = Some(cookie.clone());
        cli.data_dir = dir.clone();

        let (token, source) = resolve_token_with_source(&cli).expect("cookie should be read");
        assert_eq!(token, "deadbeef");
        assert_eq!(source.as_deref(), Some(cookie.as_path()));
        let _ = fs::remove_file(cookie);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn duplicate_backends_are_preserved_for_instance_shaping() {
        let out = expand_backend_specs(
            &[BackendKind::Cpu, BackendKind::Cpu],
            &[],
            BackendExpansionOptions {
                default_cpu_threads: 1,
                default_cpu_affinity: CpuAffinityMode::Auto,
                cpu_threads_per_instance: &[],
                cpu_affinity_per_instance: &[],
                backend_assign_timeout_ms_per_instance: &[],
                backend_control_timeout_ms_per_instance: &[],
                backend_assign_timeout_strikes_per_instance: &[],
            },
        )
        .expect("duplicate CPU backends should be preserved");
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].kind, BackendKind::Cpu);
        assert_eq!(out[1].kind, BackendKind::Cpu);
    }

    #[test]
    fn expand_backend_specs_expands_nvidia_devices() {
        let out = expand_backend_specs(
            &[BackendKind::Cpu, BackendKind::Nvidia],
            &[2, 0, 2],
            BackendExpansionOptions {
                default_cpu_threads: 1,
                default_cpu_affinity: CpuAffinityMode::Auto,
                cpu_threads_per_instance: &[],
                cpu_affinity_per_instance: &[],
                backend_assign_timeout_ms_per_instance: &[],
                backend_control_timeout_ms_per_instance: &[],
                backend_assign_timeout_strikes_per_instance: &[],
            },
        )
        .expect("backend specs should parse");
        assert_eq!(
            out,
            vec![
                BackendSpec {
                    kind: BackendKind::Cpu,
                    device_index: None,
                    cpu_threads: Some(1),
                    cpu_affinity: Some(CpuAffinityMode::Auto),
                    assign_timeout_override: None,
                    control_timeout_override: None,
                    assign_timeout_strikes_override: None,
                },
                BackendSpec {
                    kind: BackendKind::Nvidia,
                    device_index: Some(2),
                    cpu_threads: None,
                    cpu_affinity: None,
                    assign_timeout_override: None,
                    control_timeout_override: None,
                    assign_timeout_strikes_override: None,
                },
                BackendSpec {
                    kind: BackendKind::Nvidia,
                    device_index: Some(0),
                    cpu_threads: None,
                    cpu_affinity: None,
                    assign_timeout_override: None,
                    control_timeout_override: None,
                    assign_timeout_strikes_override: None,
                }
            ]
        );
    }

    #[test]
    fn expand_backend_specs_requires_nvidia_backend_for_devices() {
        let err = expand_backend_specs(
            &[BackendKind::Cpu],
            &[0],
            BackendExpansionOptions {
                default_cpu_threads: 1,
                default_cpu_affinity: CpuAffinityMode::Auto,
                cpu_threads_per_instance: &[],
                cpu_affinity_per_instance: &[],
                backend_assign_timeout_ms_per_instance: &[],
                backend_control_timeout_ms_per_instance: &[],
                backend_assign_timeout_strikes_per_instance: &[],
            },
        )
        .expect_err("nvidia devices without backend should fail");
        assert!(format!("{err:#}").contains("--nvidia-devices requires selecting nvidia"));
    }

    #[test]
    fn expand_backend_specs_applies_cpu_instance_overrides() {
        let out = expand_backend_specs(
            &[BackendKind::Cpu, BackendKind::Cpu],
            &[],
            BackendExpansionOptions {
                default_cpu_threads: 2,
                default_cpu_affinity: CpuAffinityMode::Auto,
                cpu_threads_per_instance: &[3, 5],
                cpu_affinity_per_instance: &[CpuAffinityMode::Off, CpuAffinityMode::Auto],
                backend_assign_timeout_ms_per_instance: &[],
                backend_control_timeout_ms_per_instance: &[],
                backend_assign_timeout_strikes_per_instance: &[],
            },
        )
        .expect("cpu instance overrides should apply");

        assert_eq!(out[0].cpu_threads, Some(3));
        assert_eq!(out[0].cpu_affinity, Some(CpuAffinityMode::Off));
        assert_eq!(out[1].cpu_threads, Some(5));
        assert_eq!(out[1].cpu_affinity, Some(CpuAffinityMode::Auto));
    }

    #[test]
    fn expand_backend_specs_applies_per_instance_timeout_overrides() {
        let out = expand_backend_specs(
            &[BackendKind::Cpu, BackendKind::Nvidia],
            &[],
            BackendExpansionOptions {
                default_cpu_threads: 1,
                default_cpu_affinity: CpuAffinityMode::Auto,
                cpu_threads_per_instance: &[],
                cpu_affinity_per_instance: &[],
                backend_assign_timeout_ms_per_instance: &[900, 1500],
                backend_control_timeout_ms_per_instance: &[30_000, 90_000],
                backend_assign_timeout_strikes_per_instance: &[2, 4],
            },
        )
        .expect("timeout overrides should apply");

        assert_eq!(
            out[0].assign_timeout_override,
            Some(Duration::from_millis(900))
        );
        assert_eq!(
            out[1].assign_timeout_override,
            Some(Duration::from_millis(1500))
        );
        assert_eq!(
            out[0].control_timeout_override,
            Some(Duration::from_millis(30_000))
        );
        assert_eq!(
            out[1].control_timeout_override,
            Some(Duration::from_millis(90_000))
        );
        assert_eq!(out[0].assign_timeout_strikes_override, Some(2));
        assert_eq!(out[1].assign_timeout_strikes_override, Some(4));
    }

    #[test]
    fn human_bytes_formats_units() {
        assert_eq!(human_bytes(1024 * 1024), "1.00 MiB");
        assert_eq!(human_bytes(1024 * 1024 * 1024), "1.00 GiB");
    }

    #[test]
    fn auto_cpu_threads_is_never_zero_and_scales_per_instance() {
        let single_instance = auto_cpu_threads(1);
        let dual_instance = auto_cpu_threads(2);

        assert!(single_instance >= 1);
        assert!(dual_instance >= 1);
        assert!(dual_instance <= single_instance);
    }

    #[test]
    fn distribute_auto_cpu_threads_divides_total_lane_budget() {
        assert_eq!(distribute_auto_cpu_threads(12, 1), 12);
        assert_eq!(distribute_auto_cpu_threads(12, 2), 6);
        assert_eq!(distribute_auto_cpu_threads(12, 5), 2);
        assert_eq!(distribute_auto_cpu_threads(1, 4), 1);
    }

    #[test]
    fn cpu_profile_defaults_adjust_tuning_knobs() {
        let throughput = cpu_profile_defaults(CpuPerformanceProfile::Throughput, 8);
        assert_eq!(throughput.threads, 8);
        assert_eq!(throughput.hash_poll_ms, 100);
        assert_eq!(throughput.cpu_hash_batch_size, 256);

        let efficiency = cpu_profile_defaults(CpuPerformanceProfile::Efficiency, 8);
        assert_eq!(efficiency.threads, 4);
        assert_eq!(efficiency.hash_poll_ms, 400);
        assert_eq!(efficiency.cpu_hash_batch_size, 32);
    }

    #[test]
    fn cpu_autotune_defaults_on_when_threads_omitted() {
        let mut cli = sample_cli();
        cli.threads = None;
        let backends = cli.backends.clone();
        let cpu_backend_instances = backends
            .iter()
            .filter(|kind| matches!(kind, BackendKind::Cpu))
            .count();
        let auto_threads_cap = auto_cpu_threads(cpu_backend_instances);
        let defaults = cpu_profile_defaults(cli.cpu_profile, auto_threads_cap);
        let cpu_autotune_default_enabled = cli.threads.is_none() && cpu_backend_instances > 0;
        let cpu_autotune_threads = if cli.disable_cpu_autotune_threads {
            false
        } else {
            cli.cpu_autotune_threads || cpu_autotune_default_enabled
        };
        let resolved_threads = cli.threads.unwrap_or(defaults.threads);
        assert!(cpu_autotune_threads);
        assert!(resolved_threads >= 1);
    }

    #[test]
    fn cpu_autotune_disable_flag_wins_over_default() {
        let mut cli = sample_cli();
        cli.threads = None;
        cli.disable_cpu_autotune_threads = true;
        let backends = cli.backends.clone();
        let cpu_backend_instances = backends
            .iter()
            .filter(|kind| matches!(kind, BackendKind::Cpu))
            .count();
        let cpu_autotune_default_enabled = cli.threads.is_none() && cpu_backend_instances > 0;
        let cpu_autotune_threads = if cli.disable_cpu_autotune_threads {
            false
        } else {
            cli.cpu_autotune_threads || cpu_autotune_default_enabled
        };
        assert!(!cpu_autotune_threads);
    }
}
