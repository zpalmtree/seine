mod backend;
mod config;
mod control_api;
mod daemon_api;
mod dev_fee;
mod miner;
mod types;
mod version;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;

use config::Config;

fn main() {
    if let Err(err) = run() {
        eprintln!("fatal: {err:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cfg = Config::parse()?;

    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let shutdown = Arc::clone(&shutdown);
        ctrlc::set_handler(move || {
            if shutdown.swap(true, Ordering::SeqCst) {
                // Second Ctrl+C — force exit immediately (e.g. stuck in FFI).
                std::process::exit(130);
            }
        })?;
    }

    if cfg.api_server_enabled || cfg.service_mode {
        return control_api::run(cfg, shutdown);
    }

    miner::run(&cfg, shutdown)
}
