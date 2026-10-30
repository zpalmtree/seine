mod address;
mod backend;
mod config;
mod console_exit;
mod control_api;
mod daemon_api;
mod dev_fee;
mod diagnostics;
mod hardware_telemetry;
mod memory_modules;
mod miner;
mod pool;
mod proxy;
mod runtime_identity;
mod self_update;
mod shortname;
mod shutdown;
mod types;
mod update_check;
mod user_config;
mod version;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use anyhow::Result;

use config::Config;

fn main() {
    if let Err(err) = run() {
        eprintln!("fatal: {err:#}");
        console_exit::pause_after_error();
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    // Handled before config parsing: updating needs no address, pool or data dir.
    if std::env::args().skip(1).any(|arg| arg == "--self-update") {
        return self_update::run_cli();
    }
    // Release builds: compile the AMD kernels with hipRTC for embedding.
    let args: Vec<String> = std::env::args().collect();
    if let Some(pos) = args
        .iter()
        .position(|arg| arg == "--amd-export-code-objects")
    {
        let dir = args
            .get(pos + 1)
            .ok_or_else(|| anyhow::anyhow!("--amd-export-code-objects needs a directory"))?;
        return backend::amd::export_code_objects(std::path::Path::new(dir));
    }
    self_update::cleanup_leftovers();
    let cfg = Config::parse()?;
    let shutdown = Arc::new(AtomicBool::new(false));
    if cfg.update_check_enabled && !cfg.bench {
        update_check::spawn(cfg.auto_update, Arc::clone(&shutdown));
    }

    let _shutdown_guard = shutdown::install(Arc::clone(&shutdown))?;

    let result = if cfg.proxy_listen.is_some() {
        proxy::run(&cfg, shutdown)
    } else if cfg.api_server_enabled || cfg.service_mode {
        control_api::run(cfg, shutdown)
    } else if !cfg.bench && miner::should_enable_tui(&cfg) {
        control_api::run_interactive(cfg, shutdown)
    } else {
        miner::run(&cfg, shutdown)
    };
    backend::amd::restore_performance_levels();
    if result.is_ok() && self_update::restart_pending() {
        self_update::restart()?;
    }
    result
}
