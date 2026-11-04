mod address;
mod backend;
mod config;
mod console_exit;
mod console_output;
mod console_window;
mod control_api;
mod daemon_api;
mod dev_fee;
mod diagnostics;
mod hardware_telemetry;
mod memory_modules;
mod miner;
mod payout_destination;
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
use console_output::eprintln;

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
    // Config parsing may raise AMD GPU performance levels; put them back on
    // every return path, including config errors after detection.
    let _restore_perf_levels = RestorePerfLevels;
    console_window::prepare_own_console();
    let shutdown = Arc::new(AtomicBool::new(false));
    let mut shutdown_guard = None;
    let mut dashboard = None;
    // Prompts come first and need the default Ctrl+C; everything after them
    // happens on the dashboard.
    let cfg = Config::parse(|startup| {
        shutdown_guard = Some(shutdown::install(Arc::clone(&shutdown))?);
        miner::ui::set_verbose(startup.verbose);
        if startup.opens_dashboard {
            dashboard = miner::start_dashboard(Arc::clone(&shutdown), &startup);
        }
        Ok(())
    })?;
    let _shutdown_guard = shutdown_guard;
    if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
        return Ok(());
    }
    update_check::report_installed(&cfg.data_dir);
    if cfg.update_check_enabled && !cfg.bench {
        update_check::spawn(
            cfg.auto_update,
            cfg.api_server_enabled,
            cfg.data_dir.clone(),
            Arc::clone(&shutdown),
        );
    }

    let result = if cfg.proxy_listen.is_some() {
        proxy::run(&cfg, shutdown)
    } else if cfg.api_server_enabled || cfg.service_mode {
        control_api::run(cfg, shutdown)
    } else if !cfg.bench && miner::should_enable_tui(&cfg) {
        control_api::run_interactive(cfg, shutdown)
    } else {
        miner::run(&cfg, shutdown)
    };
    backend::amd::on_process_exit();
    drop(dashboard);
    if result.is_ok() && self_update::restart_pending() {
        self_update::restart()?;
    }
    result
}

struct RestorePerfLevels;

impl Drop for RestorePerfLevels {
    fn drop(&mut self) {
        backend::amd::on_process_exit();
    }
}
