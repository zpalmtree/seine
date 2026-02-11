mod api;
mod backend;
mod config;
mod types;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

use api::ApiClient;
use backend::cpu::CpuBackend;
use backend::nvidia::NvidiaBackend;
use backend::{MiningJob, PowBackend};
use config::{BackendKind, Config};
use types::{
    decode_hex, parse_target, set_block_nonce, template_difficulty, template_height,
    BlockTemplateResponse,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const TEMPLATE_RETRY_DELAY: Duration = Duration::from_secs(2);

struct Stats {
    started_at: Instant,
    hashes: AtomicU64,
    templates: AtomicU64,
    submitted: AtomicU64,
    accepted: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            hashes: AtomicU64::new(0),
            templates: AtomicU64::new(0),
            submitted: AtomicU64::new(0),
            accepted: AtomicU64::new(0),
        }
    }

    fn print(&self) {
        let elapsed = self.started_at.elapsed().as_secs_f64().max(0.001);
        let hashes = self.hashes.load(Ordering::Relaxed);
        let templates = self.templates.load(Ordering::Relaxed);
        let submitted = self.submitted.load(Ordering::Relaxed);
        let accepted = self.accepted.load(Ordering::Relaxed);
        let hps = hashes as f64 / elapsed;

        println!(
            "[stats] {:.1}s elapsed | {} hashes | {} | templates={} submitted={} accepted={}",
            elapsed,
            hashes,
            format_hashrate(hps),
            templates,
            submitted,
            accepted,
        );
    }
}

fn main() {
    if let Err(err) = run() {
        eprintln!("fatal: {err:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cfg = Config::parse()?;

    let backend: Box<dyn PowBackend> = match cfg.backend {
        BackendKind::Cpu => Box::new(CpuBackend::new()),
        BackendKind::Nvidia => Box::new(NvidiaBackend::new()),
    };

    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let shutdown = Arc::clone(&shutdown);
        ctrlc::set_handler(move || {
            shutdown.store(true, Ordering::SeqCst);
        })?;
    }

    if cfg.bench {
        return run_benchmark(&cfg, backend.as_ref(), &shutdown);
    }

    let token = cfg
        .token
        .clone()
        .ok_or_else(|| anyhow!("missing API token in mining mode"))?;
    let client = ApiClient::new(cfg.api_url.clone(), token, REQUEST_TIMEOUT)?;

    println!(
        "starting bnminer | backend={} | threads={} (~{}GB RAM) | api={}",
        backend.name(),
        cfg.threads,
        cfg.threads * 2,
        cfg.api_url
    );

    let stats = Stats::new();
    let mut nonce_cursor = cfg.start_nonce;
    let mut last_stats_print = Instant::now();

    while !shutdown.load(Ordering::Relaxed) {
        let template = match fetch_template_with_retry(&client, &shutdown) {
            Some(t) => t,
            None => break,
        };

        let header_base = match decode_hex(&template.header_base, "header_base") {
            Ok(v) => v,
            Err(err) => {
                eprintln!("template decode error: {err:#}");
                continue;
            }
        };

        if header_base.len() != 92 {
            eprintln!(
                "template header_base length mismatch: expected 92 bytes, got {}",
                header_base.len()
            );
            continue;
        }

        let target = match parse_target(&template.target) {
            Ok(t) => t,
            Err(err) => {
                eprintln!("target parse error: {err:#}");
                continue;
            }
        };

        let height = template_height(&template.block)
            .map(|h| h.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let difficulty = template_difficulty(&template.block)
            .map(|d| d.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let stop_at = Instant::now() + cfg.refresh_interval;

        stats.templates.fetch_add(1, Ordering::Relaxed);

        println!(
            "[template] height={} difficulty={} nonce_seed={} refresh={}s",
            height,
            difficulty,
            nonce_cursor,
            cfg.refresh_interval.as_secs(),
        );

        let job = MiningJob {
            header_base: header_base.into(),
            target,
            start_nonce: nonce_cursor,
            threads: cfg.threads,
            stop_at,
        };

        match backend.mine(&job, &shutdown, &stats.hashes)? {
            Some(solution) => {
                println!(
                    "[solution] nonce={} hashes={} elapsed={:.2}s",
                    solution.nonce,
                    solution.hashes,
                    solution.elapsed.as_secs_f64(),
                );

                let mut solved_block = template.block;
                if let Err(err) = set_block_nonce(&mut solved_block, solution.nonce) {
                    eprintln!("failed to set nonce on solved block: {err:#}");
                    nonce_cursor =
                        nonce_cursor.wrapping_add((cfg.threads as u64).saturating_mul(4096));
                    continue;
                }

                stats.submitted.fetch_add(1, Ordering::Relaxed);

                match client.submit_block(&solved_block) {
                    Ok(resp) => {
                        if resp.accepted {
                            stats.accepted.fetch_add(1, Ordering::Relaxed);
                            println!(
                                "[submit] accepted=true height={} hash={}",
                                resp.height
                                    .map(|h| h.to_string())
                                    .unwrap_or_else(|| "unknown".to_string()),
                                resp.hash.unwrap_or_else(|| "unknown".to_string())
                            );
                        } else {
                            println!("[submit] accepted=false");
                        }
                    }
                    Err(err) => {
                        eprintln!("submit failed: {err:#}");
                    }
                }
            }
            None => {
                println!("[template] no solution before refresh or shutdown");
            }
        }

        nonce_cursor = nonce_cursor.wrapping_add((cfg.threads as u64).saturating_mul(4096));

        if last_stats_print.elapsed() >= cfg.stats_interval {
            stats.print();
            last_stats_print = Instant::now();
        }
    }

    stats.print();
    println!("bnminer stopped");
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchRun {
    round: u32,
    hashes: u64,
    elapsed_secs: f64,
    hps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchReport {
    backend: String,
    threads: usize,
    bench_secs: u64,
    rounds: u32,
    avg_hps: f64,
    median_hps: f64,
    min_hps: f64,
    max_hps: f64,
    runs: Vec<BenchRun>,
}

fn run_benchmark(cfg: &Config, backend: &dyn PowBackend, shutdown: &AtomicBool) -> Result<()> {
    println!(
        "benchmark mode | backend={} | threads={} | rounds={} | seconds_per_round={}",
        backend.name(),
        cfg.threads,
        cfg.bench_rounds,
        cfg.bench_secs
    );

    let header_base: Arc<[u8]> = (0..92u8)
        .map(|x| x.wrapping_mul(37).wrapping_add(11))
        .collect::<Vec<u8>>()
        .into();
    let impossible_target = [0u8; 32];
    let total_hashes = AtomicU64::new(0);
    let mut runs = Vec::with_capacity(cfg.bench_rounds as usize);

    for round in 0..cfg.bench_rounds {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let start_hashes = total_hashes.load(Ordering::Relaxed);
        let round_start = Instant::now();
        let stop_at = round_start + Duration::from_secs(cfg.bench_secs);
        let nonce_seed = cfg.start_nonce.wrapping_add(
            (round as u64).saturating_mul((cfg.threads as u64).saturating_mul(10_000)),
        );

        let job = MiningJob {
            header_base: Arc::clone(&header_base),
            target: impossible_target,
            start_nonce: nonce_seed,
            threads: cfg.threads,
            stop_at,
        };

        let maybe_solution = backend.mine(&job, shutdown, &total_hashes)?;
        if let Some(solution) = maybe_solution {
            println!(
                "[bench] unexpected solution found at nonce={} after {:.2}s; continuing",
                solution.nonce,
                solution.elapsed.as_secs_f64()
            );
        }

        let elapsed = round_start.elapsed().as_secs_f64().max(0.001);
        let hashes = total_hashes
            .load(Ordering::Relaxed)
            .saturating_sub(start_hashes);
        let hps = hashes as f64 / elapsed;

        println!(
            "[bench] round {}/{} | hashes={} | elapsed={:.2}s | {}",
            round + 1,
            cfg.bench_rounds,
            hashes,
            elapsed,
            format_hashrate(hps),
        );

        runs.push(BenchRun {
            round: round + 1,
            hashes,
            elapsed_secs: elapsed,
            hps,
        });
    }

    if runs.is_empty() {
        println!("benchmark aborted before first round");
        return Ok(());
    }

    let mut sorted_hps: Vec<f64> = runs.iter().map(|r| r.hps).collect();
    sorted_hps.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let avg_hps = sorted_hps.iter().sum::<f64>() / sorted_hps.len() as f64;
    let median_hps = median(&sorted_hps);
    let min_hps = *sorted_hps.first().unwrap_or(&0.0);
    let max_hps = *sorted_hps.last().unwrap_or(&0.0);

    let report = BenchReport {
        backend: backend.name().to_string(),
        threads: cfg.threads,
        bench_secs: cfg.bench_secs,
        rounds: runs.len() as u32,
        avg_hps,
        median_hps,
        min_hps,
        max_hps,
        runs,
    };

    println!(
        "[bench] summary | avg={} | median={} | min={} | max={}",
        format_hashrate(report.avg_hps),
        format_hashrate(report.median_hps),
        format_hashrate(report.min_hps),
        format_hashrate(report.max_hps),
    );

    if let Some(path) = &cfg.bench_baseline {
        let baseline_text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read baseline file {}", path.display()))?;
        let baseline: BenchReport = serde_json::from_str(&baseline_text)
            .with_context(|| format!("failed to parse baseline JSON {}", path.display()))?;
        if baseline.avg_hps > 0.0 {
            let delta_pct = ((report.avg_hps - baseline.avg_hps) / baseline.avg_hps) * 100.0;
            println!(
                "[bench] baseline compare | baseline_avg={} | delta={:+.2}%",
                format_hashrate(baseline.avg_hps),
                delta_pct
            );
        }
    }

    if let Some(path) = &cfg.bench_output {
        let json = serde_json::to_string_pretty(&report)
            .context("failed to serialize benchmark report")?;
        std::fs::write(path, json)
            .with_context(|| format!("failed to write benchmark report {}", path.display()))?;
        println!("[bench] wrote report to {}", path.display());
    }

    Ok(())
}

fn fetch_template_with_retry(
    client: &ApiClient,
    shutdown: &AtomicBool,
) -> Option<BlockTemplateResponse> {
    while !shutdown.load(Ordering::Relaxed) {
        match client.get_block_template() {
            Ok(template) => return Some(template),
            Err(err) => {
                eprintln!("blocktemplate request failed: {err:#}");
                thread::sleep(TEMPLATE_RETRY_DELAY);
            }
        }
    }

    None
}

fn format_hashrate(hps: f64) -> String {
    if hps >= 1_000_000_000.0 {
        return format!("{:.3} GH/s", hps / 1_000_000_000.0);
    }
    if hps >= 1_000_000.0 {
        return format!("{:.3} MH/s", hps / 1_000_000.0);
    }
    if hps >= 1_000.0 {
        return format!("{:.3} KH/s", hps / 1_000.0);
    }
    format!("{hps:.3} H/s")
}

fn median(sorted: &[f64]) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_handles_even_and_odd() {
        assert_eq!(median(&[]), 0.0);
        assert_eq!(median(&[5.0]), 5.0);
        assert_eq!(median(&[1.0, 3.0, 5.0]), 3.0);
        assert_eq!(median(&[1.0, 3.0, 5.0, 7.0]), 4.0);
    }

    #[test]
    fn format_hashrate_units() {
        assert_eq!(format_hashrate(5.0), "5.000 H/s");
        assert_eq!(format_hashrate(5_000.0), "5.000 KH/s");
        assert_eq!(format_hashrate(5_000_000.0), "5.000 MH/s");
    }
}
