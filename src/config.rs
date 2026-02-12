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
pub enum CpuAffinityMode {
    Off,
    Auto,
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

    /// Number of CPU mining threads (each uses ~2GB RAM for Argon2id).
    #[arg(long, alias = "cpu-threads", default_value_t = 1)]
    threads: usize,

    /// CPU pinning policy for CPU mining workers.
    #[arg(long, value_enum, default_value_t = CpuAffinityMode::Auto)]
    cpu_affinity: CpuAffinityMode,

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

    /// Interval for periodic stats printing.
    #[arg(long, default_value_t = 10)]
    stats_secs: u64,

    /// Capacity of the bounded backend event queue.
    #[arg(long, default_value_t = 1024)]
    backend_event_capacity: usize,

    /// Backend hash polling interval in milliseconds.
    #[arg(long, default_value_t = 200)]
    hash_poll_ms: u64,

    /// Disable strict round quiesce barriers to favor peak throughput over exact per-round accounting.
    #[arg(long, action = ArgAction::SetTrue)]
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

    /// Write benchmark report JSON to this file.
    #[arg(long)]
    bench_output: Option<PathBuf>,

    /// Compare benchmark average H/s against a previous JSON report.
    #[arg(long)]
    bench_baseline: Option<PathBuf>,

    /// Fail benchmark run if average H/s regresses below baseline by more than this percent.
    #[arg(long)]
    bench_fail_below_pct: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub api_url: String,
    pub token: Option<String>,
    pub token_cookie_path: Option<PathBuf>,
    pub wallet_password: Option<String>,
    pub wallet_password_file: Option<PathBuf>,
    pub backends: Vec<BackendKind>,
    pub threads: usize,
    pub cpu_affinity: CpuAffinityMode,
    pub refresh_interval: Duration,
    pub request_timeout: Duration,
    pub events_stream_timeout: Duration,
    pub stats_interval: Duration,
    pub backend_event_capacity: usize,
    pub hash_poll_interval: Duration,
    pub strict_round_accounting: bool,
    pub start_nonce: u64,
    pub nonce_iters_per_lane: u64,
    pub work_allocation: WorkAllocation,
    pub sse_enabled: bool,
    pub refresh_on_same_height: bool,
    pub ui_mode: UiMode,
    pub bench: bool,
    pub bench_kind: BenchKind,
    pub bench_secs: u64,
    pub bench_rounds: u32,
    pub bench_output: Option<PathBuf>,
    pub bench_baseline: Option<PathBuf>,
    pub bench_fail_below_pct: Option<f64>,
}

impl Config {
    pub fn parse() -> Result<Self> {
        let cli = Cli::parse();
        if cli.threads == 0 {
            bail!("threads must be >= 1");
        }
        if cli.nonce_iters_per_lane == 0 {
            bail!("nonce-iters-per-lane must be >= 1");
        }
        if cli.backend_event_capacity == 0 {
            bail!("backend-event-capacity must be >= 1");
        }
        if cli.hash_poll_ms == 0 {
            bail!("hash-poll-ms must be >= 1");
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

        let backends = dedupe_backends(&cli.backends);
        if backends.is_empty() {
            bail!("at least one backend is required");
        }

        validate_cpu_memory(&backends, cli.threads, cli.allow_oversubscribe)?;

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
            backends,
            threads: cli.threads,
            cpu_affinity: cli.cpu_affinity,
            refresh_interval: Duration::from_secs(cli.refresh_secs.max(1)),
            request_timeout: Duration::from_secs(cli.request_timeout_secs.max(1)),
            events_stream_timeout: Duration::from_secs(cli.events_stream_timeout_secs.max(1)),
            stats_interval: Duration::from_secs(cli.stats_secs.max(1)),
            backend_event_capacity: cli.backend_event_capacity,
            hash_poll_interval: Duration::from_millis(cli.hash_poll_ms),
            strict_round_accounting: !cli.relaxed_accounting,
            start_nonce: cli.start_nonce.unwrap_or_else(default_nonce_seed),
            nonce_iters_per_lane: cli.nonce_iters_per_lane,
            work_allocation: cli.work_allocation,
            sse_enabled: !cli.disable_sse,
            refresh_on_same_height: cli.refresh_on_same_height,
            ui_mode: cli.ui,
            bench: cli.bench,
            bench_kind: cli.bench_kind,
            bench_secs: cli.bench_secs.max(1),
            bench_rounds: cli.bench_rounds.max(1),
            bench_output: cli.bench_output,
            bench_baseline: cli.bench_baseline,
            bench_fail_below_pct: cli.bench_fail_below_pct,
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

fn dedupe_backends(backends: &[BackendKind]) -> Vec<BackendKind> {
    let mut seen = HashSet::new();
    let mut ordered = Vec::with_capacity(backends.len());
    for backend in backends {
        if seen.insert(*backend) {
            ordered.push(*backend);
        }
    }
    ordered
}

fn validate_cpu_memory(
    backends: &[BackendKind],
    threads: usize,
    allow_oversubscribe: bool,
) -> Result<()> {
    if !backends.contains(&BackendKind::Cpu) {
        return Ok(());
    }

    let required = CPU_LANE_MEMORY_BYTES.saturating_mul(threads as u64);
    let Some(budget) = detect_memory_budget_bytes() else {
        return Ok(());
    };

    if required > budget.effective_total && !allow_oversubscribe {
        bail!(
            "configured CPU lanes need ~{} RAM ({} thread(s) * 2GB), but effective memory limit is ~{}. Use fewer threads or pass --allow-oversubscribe to bypass this safety check.",
            human_bytes(required),
            threads,
            human_bytes(budget.effective_total),
        );
    }

    if required > budget.effective_available && !allow_oversubscribe {
        bail!(
            "configured CPU lanes need ~{} RAM ({} thread(s) * 2GB), but currently available memory is ~{} (effective limit ~{}). Reduce threads or pass --allow-oversubscribe if you accept potential swap/OOM risk.",
            human_bytes(required),
            threads,
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
            threads: 1,
            cpu_affinity: CpuAffinityMode::Auto,
            allow_oversubscribe: false,
            refresh_secs: 20,
            request_timeout_secs: 10,
            events_stream_timeout_secs: 10,
            stats_secs: 10,
            backend_event_capacity: 1024,
            hash_poll_ms: 200,
            relaxed_accounting: false,
            start_nonce: None,
            nonce_iters_per_lane: 1u64 << 36,
            work_allocation: WorkAllocation::Adaptive,
            disable_sse: false,
            refresh_on_same_height: false,
            ui: UiMode::Auto,
            bench: false,
            bench_kind: BenchKind::Backend,
            bench_secs: 20,
            bench_rounds: 3,
            bench_output: None,
            bench_baseline: None,
            bench_fail_below_pct: None,
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
    fn dedupe_backends_preserves_order() {
        let out = dedupe_backends(&[BackendKind::Cpu, BackendKind::Nvidia, BackendKind::Cpu]);
        assert_eq!(out, vec![BackendKind::Cpu, BackendKind::Nvidia]);
    }

    #[test]
    fn human_bytes_formats_units() {
        assert_eq!(human_bytes(1024 * 1024), "1.00 MiB");
        assert_eq!(human_bytes(1024 * 1024 * 1024), "1.00 GiB");
    }
}
