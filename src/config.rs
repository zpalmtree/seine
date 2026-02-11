use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum BackendKind {
    Cpu,
    Nvidia,
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

    /// Path to api.cookie file (defaults to <data-dir>/api.cookie).
    #[arg(long)]
    cookie: Option<PathBuf>,

    /// Daemon data directory (used to locate api.cookie when --cookie is unset).
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Mining backend implementation.
    #[arg(long, value_enum, default_value_t = BackendKind::Cpu)]
    backend: BackendKind,

    /// Number of mining threads (each thread uses ~2GB RAM for Argon2id).
    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// Maximum time to work on one block template before refreshing.
    #[arg(long, default_value_t = 20)]
    refresh_secs: u64,

    /// Interval for periodic stats printing.
    #[arg(long, default_value_t = 10)]
    stats_secs: u64,

    /// Optional nonce seed.
    #[arg(long)]
    start_nonce: Option<u64>,

    /// Run local performance benchmark instead of mining over API.
    #[arg(long, default_value_t = false)]
    bench: bool,

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
    pub backend: BackendKind,
    pub threads: usize,
    pub refresh_interval: Duration,
    pub stats_interval: Duration,
    pub start_nonce: u64,
    pub bench: bool,
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

        let token = if cli.bench {
            None
        } else {
            Some(resolve_token(&cli)?)
        };
        let api_url = normalize_api_url(&cli.api_url);

        Ok(Self {
            api_url,
            token,
            backend: cli.backend,
            threads: cli.threads,
            refresh_interval: Duration::from_secs(cli.refresh_secs.max(1)),
            stats_interval: Duration::from_secs(cli.stats_secs.max(1)),
            start_nonce: cli.start_nonce.unwrap_or_else(default_nonce_seed),
            bench: cli.bench,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        dir.push(format!("bnminer-test-{}-{}", std::process::id(), now));
        dir
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
        let cli = Cli {
            api_url: "http://127.0.0.1:8332".to_string(),
            token: Some("  abc123  ".to_string()),
            cookie: None,
            data_dir: PathBuf::from("./data"),
            backend: BackendKind::Cpu,
            threads: 1,
            refresh_secs: 20,
            stats_secs: 10,
            start_nonce: None,
            bench: false,
            bench_secs: 20,
            bench_rounds: 3,
            bench_output: None,
            bench_baseline: None,
        };

        assert_eq!(resolve_token(&cli).unwrap(), "abc123");
    }

    #[test]
    fn resolve_token_reads_cookie() {
        let dir = unique_temp_dir();
        fs::create_dir_all(&dir).unwrap();
        let cookie = dir.join("api.cookie");
        fs::write(&cookie, "deadbeef\n").unwrap();

        let cli = Cli {
            api_url: "http://127.0.0.1:8332".to_string(),
            token: None,
            cookie: Some(cookie.clone()),
            data_dir: dir.clone(),
            backend: BackendKind::Cpu,
            threads: 1,
            refresh_secs: 20,
            stats_secs: 10,
            start_nonce: None,
            bench: false,
            bench_secs: 20,
            bench_rounds: 3,
            bench_output: None,
            bench_baseline: None,
        };

        assert_eq!(resolve_token(&cli).unwrap(), "deadbeef");
        let _ = fs::remove_file(cookie);
        let _ = fs::remove_dir_all(dir);
    }
}
