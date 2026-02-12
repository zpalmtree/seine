use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
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

#[derive(Debug, Parser)]
#[command(name = "bnminer", version, about = "External Blocknet miner")]
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

    /// Allow starting when configured CPU lanes exceed detected system RAM.
    #[arg(long, default_value_t = false)]
    allow_oversubscribe: bool,

    /// Maximum time to work on one block template before refreshing.
    #[arg(long, default_value_t = 20)]
    refresh_secs: u64,

    /// Interval for periodic stats printing.
    #[arg(long, default_value_t = 10)]
    stats_secs: u64,

    /// Optional nonce seed.
    #[arg(long)]
    start_nonce: Option<u64>,

    /// Maximum iterations each lane will scan before switching to next reservation.
    #[arg(long, default_value_t = 1u64 << 36)]
    nonce_iters_per_lane: u64,

    /// Disable SSE tip notifications (/api/events) and rely only on refresh timer.
    #[arg(long, action = ArgAction::SetTrue)]
    disable_sse: bool,

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
}

#[derive(Debug, Clone)]
pub struct Config {
    pub api_url: String,
    pub token: Option<String>,
    pub wallet_password: Option<String>,
    pub wallet_password_file: Option<PathBuf>,
    pub backends: Vec<BackendKind>,
    pub threads: usize,
    pub refresh_interval: Duration,
    pub stats_interval: Duration,
    pub start_nonce: u64,
    pub nonce_iters_per_lane: u64,
    pub sse_enabled: bool,
    pub bench: bool,
    pub bench_kind: BenchKind,
    pub bench_secs: u64,
    pub bench_rounds: u32,
    pub bench_output: Option<PathBuf>,
    pub bench_baseline: Option<PathBuf>,
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

        let backends = dedupe_backends(&cli.backends);
        if backends.is_empty() {
            bail!("at least one backend is required");
        }

        validate_cpu_memory(&backends, cli.threads, cli.allow_oversubscribe)?;

        let token = if cli.bench {
            None
        } else {
            Some(resolve_token(&cli)?)
        };
        let api_url = normalize_api_url(&cli.api_url);

        Ok(Self {
            api_url,
            token,
            wallet_password: cli.wallet_password,
            wallet_password_file: cli.wallet_password_file,
            backends,
            threads: cli.threads,
            refresh_interval: Duration::from_secs(cli.refresh_secs.max(1)),
            stats_interval: Duration::from_secs(cli.stats_secs.max(1)),
            start_nonce: cli.start_nonce.unwrap_or_else(default_nonce_seed),
            nonce_iters_per_lane: cli.nonce_iters_per_lane,
            sse_enabled: !cli.disable_sse,
            bench: cli.bench,
            bench_kind: cli.bench_kind,
            bench_secs: cli.bench_secs.max(1),
            bench_rounds: cli.bench_rounds.max(1),
            bench_output: cli.bench_output,
            bench_baseline: cli.bench_baseline,
        })
    }
}

fn resolve_token(cli: &Cli) -> Result<String> {
    if let Some(token) = &cli.token {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            bail!("--token is empty");
        }
        return Ok(trimmed.to_string());
    }

    let cookie_path = cli
        .cookie
        .clone()
        .unwrap_or_else(|| cli.data_dir.join("api.cookie"));

    let token = fs::read_to_string(&cookie_path)
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
        dir.push(format!("bnminer-test-{}-{}", std::process::id(), now));
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
            allow_oversubscribe: false,
            refresh_secs: 20,
            stats_secs: 10,
            start_nonce: None,
            nonce_iters_per_lane: 1u64 << 36,
            disable_sse: false,
            bench: false,
            bench_kind: BenchKind::Backend,
            bench_secs: 20,
            bench_rounds: 3,
            bench_output: None,
            bench_baseline: None,
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

        assert_eq!(resolve_token(&cli).expect("token should parse"), "abc123");
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

        assert_eq!(
            resolve_token(&cli).expect("cookie should be read"),
            "deadbeef"
        );
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
