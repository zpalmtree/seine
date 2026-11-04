//! Export the real Ratatui renderer with synthetic data, without mining/network I/O.
use super::*;
use ratatui::backend::TestBackend;

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn color(c: Color) -> String {
    match c {
        Color::Rgb(r, g, b) => format!("#{r:02x}{g:02x}{b:02x}"),
        _ => "#b4bec8".to_string(),
    }
}

fn capture(state: &TuiStateInner, width: u16, height: u16) -> String {
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal
        .draw(|frame| draw_dashboard(frame, frame.area(), state))
        .unwrap();
    let buffer = terminal.backend().buffer();
    let mut html = String::from("<pre>");
    for y in 0..height {
        let mut previous = None;
        for x in 0..width {
            let cell = &buffer[(x, y)];
            if previous != Some(cell.fg) {
                if previous.is_some() {
                    html.push_str("</span>");
                }
                html.push_str(&format!("<span style='color:{}'>", color(cell.fg)));
                previous = Some(cell.fg);
            }
            html.push_str(&escape(cell.symbol()));
        }
        html.push_str("</span>\n");
    }
    html.push_str("</pre>");
    html
}

#[test]
#[ignore = "writes the feature showcase; run explicitly after changing thread feedback"]
fn export_thread_controls_showcase() {
    let mut html = String::from(
        r#"<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Seine thread controls</title>
<style>body{margin:16px;background:#101318;color:#b4bec8;font:14px system-ui}h1{font-size:22px}h2{font-size:16px}pre{background:#080b10;padding:8px;font:12px/1.25 monospace;width:max-content;max-width:100%;overflow:auto}section{margin:24px 0}.mobile{display:none}@media(max-width:700px){body{margin:12px}.desktop{display:none}.mobile{display:block}pre{font-size:12px}}</style>
<h1>Seine CPU thread controls</h1><p>Real terminal components, synthetic mining data. No miner or network connection is started.</p>"#,
    );
    let revision = std::process::Command::new("git")
        .args(["hash-object", "src/miner/tui.rs"])
        .output()
        .unwrap();
    html.push_str(&format!(
        "<p>Renderer source blob: <code>{}</code></p>",
        String::from_utf8_lossy(&revision.stdout).trim()
    ));
    for (name, mode) in [
        ("Ready", 0),
        ("Loading / restart requested", 1),
        ("Disabled / waiting for restart", 2),
        ("At the safe thread limit", 3),
        ("At the minimum thread count", 4),
        ("RAM fell before admission", 5),
        ("Control channel unavailable", 6),
        ("Empty / no CPU backend", 7),
        ("Success / replacement session", 8),
    ] {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let mut state = TuiStateInner::new();
        state.mode = "pool".into();
        state.state = "hashing".into();
        state.threads = 2;
        state.pool_worker = "demo-worker".into();
        state.api_url = "stratum://pool.example:3333".into();
        state.backends_desc = "cpu".into();
        state.round_hashrate = "2.500 H/s".into();
        state.avg_hashrate = "2.400 H/s".into();
        state.thread_control = Some(ThreadControl {
            live: None,
            run_id: 1,
            sender,
            max_threads: 4,
            current_threads: 2,
            memory: None,
            quit_requested: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        let key = |state: &mut TuiStateInner, ch| {
            state.handle_key(KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE));
        };
        match mode {
            1 => key(&mut state, '+'),
            2 => {
                key(&mut state, '+');
                key(&mut state, '+');
            }
            3 => {
                state.thread_control.as_mut().unwrap().max_threads = 2;
                key(&mut state, '+');
            }
            4 => {
                state.threads = 1;
                state.thread_control.as_mut().unwrap().current_threads = 1;
                key(&mut state, '-');
            }
            5 => {
                key(&mut state, '+');
                let request = receiver.try_recv().unwrap();
                let message = super::super::thread_control::validate_headroom(3, 2)
                    .unwrap_err()
                    .to_string();
                request.reply.unwrap().send(message).unwrap();
                state.poll_thread_reply();
            }
            6 => {
                drop(receiver);
                key(&mut state, '+');
            }
            7 => {
                state.threads = 0;
                state.thread_control = None;
                state.backends_desc.clear();
                key(&mut state, '+');
            }
            8 => {
                state.threads = 3;
                state.thread_control.as_mut().unwrap().current_threads = 3;
            }
            _ => {}
        }
        html.push_str(&format!("<section><h2>{name}</h2><div class='desktop'>{}</div><div class='mobile'>{}</div></section>", capture(&state, 120, 30), capture(&state, 40, 18)));
    }
    html.push_str("</html>");
    let path = std::path::Path::new("docs/showcases/thread-controls");
    std::fs::create_dir_all(path).unwrap();
    std::fs::write(path.join("index.html"), html).unwrap();
}

#[test]
#[ignore = "writes the tuning showcase using the real dashboard and fixture states"]
fn export_autotune_showcase() {
    use super::super::adaptive_cpu::status_text;
    use super::super::adaptive_search::{Comparing, Status};
    use blocknet_pow_kernel::CpuKernel;
    let mut html = String::from(
        r#"<!DOCTYPE html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Seine tuning in the dashboard</title>
<style>body{margin:16px;background:#101318;color:#b4bec8;font:14px system-ui}h1{font-size:22px}h2{font-size:17px;margin:28px 0 8px}pre{background:#0a0d12;padding:6px;margin:0 0 12px;font:12px/1.2 ui-monospace,Menlo,monospace;width:max-content}p{max-width:70ch}@media(max-width:700px){.desktop{display:none}}</style>
<h1>Seine tuning in the dashboard</h1><p>Real dashboard renderer with synthetic data, at 120 and 46 columns. Tuning has one row under the thread count, and the log only says what changed. Each comparison goes to plain logs and the control API, not the dashboard log. The [+] [-] keys show once the count is settled; while tuning picks it, pressing them explains why they wait.</p>"#,
    );
    let revision = std::process::Command::new("git")
        .args(["hash-object", "src/miner/adaptive_search.rs"])
        .output()
        .unwrap();
    html.push_str(&format!(
        "<p>Search source blob: <code>{}</code></p>",
        String::from_utf8_lossy(&revision.stdout).trim()
    ));
    let tuning = |left, testing_update, comparing| Status::Tuning {
        threads: 12,
        kernel: CpuKernel::Avx2Stream,
        done: 2,
        left,
        secs_left: left as u64 * 90,
        testing_update,
        comparing,
    };
    let settled = Status::Settled {
        threads: 12,
        kernel: CpuKernel::Avx2Stream,
        hps: 10.4,
    };
    let start_note = (
        LogLevel::Info,
        "CPU",
        "finding the best settings: the thread count will change for a few minutes",
        200.0,
    );
    let update_note = (
        LogLevel::Info,
        "CPU",
        "new seine version improves your CPU kernel: checking the thread count for a few minutes",
        120.0,
    );
    let tuned = (LogLevel::Success, "CPU", "tuned to 12 threads", 12.0);
    let lowered = (
        LogLevel::Info,
        "CPU",
        "lowered to 5 threads: RAM is low; going back when it is free",
        7200.0,
    );
    let cases: Vec<(&str, u32, Status, Vec<(LogLevel, &str, &str, f64)>)> = vec![
        (
            "First run: trying kernels",
            8,
            tuning(4, false, Comparing::Kernels),
            vec![start_note],
        ),
        (
            "First run: comparing thread counts (the count moves between the two)",
            16,
            tuning(3, false, Comparing::Threads(12, 16)),
            vec![start_note],
        ),
        ("Tuned", 12, settled.clone(), vec![start_note, tuned]),
        (
            "Tuned, then one more look at a close result",
            16,
            Status::Rechecking {
                current: 12,
                candidate: 16,
            },
            vec![start_note, tuned],
        ),
        (
            "Next start: back on the saved settings",
            12,
            Status::Checking,
            vec![],
        ),
        (
            "After an update that improved this CPU's kernel: checking the thread count",
            14,
            tuning(2, true, Comparing::Threads(12, 14)),
            vec![update_note],
        ),
        (
            "After an update: found something faster",
            14,
            settled.clone(),
            vec![
                update_note,
                (LogLevel::Success, "CPU", "tuned to 14 threads", 12.0),
            ],
        ),
        (
            "Low on RAM",
            5,
            Status::LimitedByRam {
                threads: 5,
                best: 12,
            },
            vec![lowered],
        ),
        (
            "RAM came back",
            12,
            settled,
            vec![
                lowered,
                (
                    LogLevel::Info,
                    "CPU",
                    "back to 12 threads: RAM is free",
                    8100.0,
                ),
            ],
        ),
        (
            "No room for even one thread",
            0,
            Status::WaitingForRam,
            vec![(
                LogLevel::Info,
                "CPU",
                "paused until enough RAM is free for one thread",
                1.0,
            )],
        ),
    ];
    let cases = {
        let mut cases = cases;
        cases.insert(
            2,
            (
                "First run: someone presses + or - (the keys wait and say why)",
                16,
                tuning(3, false, Comparing::Threads(12, 16)),
                vec![start_note],
            ),
        );
        cases
    };
    for (name, threads, status, log) in cases {
        let (sender, _requests) = crossbeam_channel::bounded(1);
        let mut state = mining_fixture("Working", &log);
        state.threads = threads as usize;
        state.tuning_status = status_text(&status);
        state.thread_control = Some(ThreadControl {
            live: None,
            run_id: 1,
            sender,
            max_threads: 16,
            current_threads: threads as usize,
            memory: None,
            quit_requested: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        state.threads_locked = super::super::adaptive_cpu::threads_locked_notice(&status);
        if name.contains("presses") {
            state.handle_key(KeyEvent::new(KeyCode::Char('+'), KeyModifiers::NONE));
        }
        html.push_str(&format!("<h2>{name}</h2><div class=desktop>"));
        html.push_str(&capture_full(&state, 120, 30));
        html.push_str("</div>");
        html.push_str(&capture_full(&state, 46, 36));
    }
    std::fs::write(
        std::env::var("SEINE_SHOWCASE_DIR")
            .map(|dir| std::path::PathBuf::from(dir).join("index.html"))
            .unwrap_or_else(|_| "docs/showcases/autotune-startup/index.html".into()),
        html,
    )
    .unwrap();
}

fn capture_log(state: &TuiStateInner, width: u16, height: u16) -> String {
    // Reserve enough rows for the actual log component's narrow-width wrapping.
    let rows: usize = state
        .log_entries
        .iter()
        .map(|entry| wrap_str(&entry.message, usize::from(width).saturating_sub(32).max(1)).len())
        .sum();
    let height = height.max(rows as u16 + 3);
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal
        .draw(|frame| draw_log(frame, frame.area(), state))
        .unwrap();
    let buffer = terminal.backend().buffer();
    let mut html = String::from("<pre>");
    for y in 0..height {
        let mut previous = None;
        for x in 0..width {
            let cell = &buffer[(x, y)];
            if previous != Some(cell.fg) {
                if previous.is_some() {
                    html.push_str("</span>");
                }
                html.push_str(&format!("<span style='color:{}'>", color(cell.fg)));
                previous = Some(cell.fg);
            }
            html.push_str(&escape(cell.symbol()));
        }
        html.push_str("</span>\n");
    }
    html.push_str("</pre>");
    html
}

#[test]
#[ignore = "writes exchange payout showcase from the real terminal renderer"]
fn export_exchange_payout_showcase() {
    let path = std::path::Path::new("docs/showcases/exchange-payouts");
    std::fs::create_dir_all(path).unwrap();
    let mut html = String::from(
        r#"<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Seine exchange payouts</title><style>body{margin:24px;background:#101318;color:#b4bec8;font:14px system-ui}h1{font-size:24px}h2{font-size:17px}pre{background:#080b10;padding:16px;font:13px/1.35 monospace;width:max-content;max-width:100%;overflow:auto;box-sizing:border-box}section{margin:28px 0}.mobile{display:none}.prompt{white-space:pre-wrap;max-width:100%;width:960px}code{overflow-wrap:anywhere}@media(max-width:700px){body{margin:12px}.desktop{display:none}.mobile{display:block}pre{font-size:12px;padding:8px}}</style><h1>Seine exchange payouts</h1><p>Actual Seine terminal components with demo data. The pool supplies the payout minimum.</p>"#,
    );
    for (slug, title, minimum, status) in [
        (
            "mining",
            "Mining / 10 BNT minimum",
            Some("10 BNT"),
            "hashing",
        ),
        (
            "loading",
            "Connecting / waiting for pool minimum",
            Some("awaiting pool"),
            "pool-connected",
        ),
        (
            "empty",
            "New account / no rewards yet",
            Some("10 BNT"),
            "hashing",
        ),
        (
            "reconnect",
            "Reconnecting / last confirmed minimum",
            Some("10 BNT"),
            "pool-disconnected",
        ),
        (
            "rejected",
            "Memo unsupported / mining stopped",
            Some("awaiting pool"),
            "pool-login-rejected",
        ),
        (
            "regular",
            "Personal wallet / pool minimum",
            Some("0.1 BNT"),
            "hashing",
        ),
        (
            "memo-no-minimum",
            "Memo accepted / pool does not report a minimum",
            None,
            "hashing",
        ),
        (
            "longhorn",
            "Longhorn / memo account without a reported minimum",
            None,
            "hashing",
        ),
        (
            "legacy",
            "Pool without a minimum in its reply",
            None,
            "hashing",
        ),
    ] {
        let mut state = TuiStateInner::new();
        state.mode = "pool".into();
        state.version = env!("CARGO_PKG_VERSION").into();
        state.state = status.into();
        state.threads = 2;
        state.pool_worker = "demo-worker".into();
        state.api_url = "stratum+tcp://pool.example:3333".into();
        state.backends_desc = "cpu".into();
        state.round_hashrate = "2.500 H/s".into();
        state.avg_hashrate = "2.400 H/s".into();
        state.pool_address = "demo-deposit-address".into();
        state.pool_pending = "2.50 BNT".into();
        state.pool_paid = "10.00 BNT".into();
        state.pool_payout_minimum = minimum.map(str::to_string);
        if slug == "longhorn" {
            state.api_url = "stratum+ssl://us.longhornpool.net:3334".into();
        }

        if slug == "empty" {
            state.pool_pending = "0.00 BNT".into();
            state.pool_paid = "0.00 BNT".into();
        }
        if matches!(slug, "loading" | "rejected") {
            state.pool_pending = "---".into();
            state.pool_paid = "---".into();
            state.round_hashrate = "0.000 H/s".into();
            state.avg_hashrate = "0.000 H/s".into();
        }
        if slug == "rejected" {
            state.push_log(LogEntry {
                elapsed_secs: 0.0,
                level: LogLevel::Error,
                tag: "AUTH".into(),
                message: "pool did not acknowledge the payout memo; no shares will be submitted"
                    .into(),
                count: 1,
            });
        }
        html.push_str(&format!("<section id='{slug}'><h2>{title}</h2><div class='desktop'>{}</div><div class='mobile'>{}</div></section>", capture(&state, 120, 30), capture(&state, 40, 18)));
    }
    let memo_error = crate::pool::payout_account_for_pool(
        "stratum+ssl://us.longhornpool.net:3334",
        "demo-address",
        Some("contains spaces"),
    )
    .unwrap_err();
    html.push_str(&format!("<section id='longhorn-invalid'><h2>Longhorn / unsupported memo characters</h2><pre class='prompt'>Error: {}</pre></section>", escape(&memo_error.to_string())));
    let prompts: Vec<(String, String)> =
        serde_json::from_str(&std::fs::read_to_string(path.join("prompts.json")).unwrap()).unwrap();
    for (index, (title, text)) in prompts.iter().enumerate() {
        html.push_str(&format!(
            "<section id='prompt-{index}'><h2>{}</h2><pre class='prompt'>{}</pre></section>",
            escape(title),
            escape(text)
        ));
    }
    html.push_str("<p>Captured from these source revisions:</p>");
    for source in [
        "src/miner/tui.rs",
        "src/config.rs",
        "src/pool.rs",
        "src/miner/mining_pool.rs",
    ] {
        let revision = std::process::Command::new("git")
            .args(["hash-object", source])
            .output()
            .unwrap();
        html.push_str(&format!(
            "<p>{source}: <code>{}</code></p>",
            String::from_utf8_lossy(&revision.stdout).trim()
        ));
    }
    html.push_str("</html>");
    std::fs::write(path.join("index.html"), html).unwrap();
}

fn capture_form(
    help: &str,
    label: &str,
    initial: &str,
    typed: &str,
    error: Option<&str>,
    width: u16,
    height: u16,
) -> String {
    let validate = |_: &str| Ok(());
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal
        .draw(|frame| {
            crate::miner::tui_prompt::draw_for_showcase(
                frame,
                crate::miner::tui_prompt::PromptSpec {
                    help,
                    label,
                    initial,
                    validate: &validate,
                },
                typed,
                error,
            )
        })
        .unwrap();
    html_from_buffer(terminal.backend().buffer(), width, height)
}

fn html_from_buffer(buffer: &ratatui::buffer::Buffer, width: u16, height: u16) -> String {
    let mut html = String::from("<pre>");
    for y in 0..height {
        let mut previous = None;
        for x in 0..width {
            let cell = &buffer[(x, y)];
            let style = (cell.fg, cell.bg);
            if previous != Some(style) {
                if previous.is_some() {
                    html.push_str("</span>");
                }
                let background = match cell.bg {
                    Color::Reset => String::new(),
                    bg => format!(";background:{}", color(bg)),
                };
                html.push_str(&format!(
                    "<span style='color:{}{background}'>",
                    color(cell.fg)
                ));
                previous = Some(style);
            }
            html.push_str(&escape(cell.symbol()));
        }
        html.push_str("</span>\n");
    }
    html.push_str("</pre>");
    html
}

fn capture_full(state: &TuiStateInner, width: u16, height: u16) -> String {
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal
        .draw(|frame| draw_dashboard(frame, frame.area(), state))
        .unwrap();
    html_from_buffer(terminal.backend().buffer(), width, height)
}

fn dashboard_fixture(state_label: &str, log: &[(LogLevel, &str, &str, f64)]) -> TuiStateInner {
    // Long enough ago that the oldest fixture line fits inside the uptime.
    let uptime = log.iter().map(|entry| entry.3).fold(0.0, f64::max) + 5.0;
    let mut state = TuiStateInner::new();
    state.started_at = Instant::now() - Duration::from_secs_f64(uptime);
    state.mode = "pool".into();
    state.version = "v0.3.1".into();
    state.state = state_label.into();
    state.api_url = "stratum+tcp://bntpool.com:3333".into();
    state.pool_worker = "seine-4821".into();
    state.pool_address = "Ab12Cd...Ef34Gh".into();
    state.pool_payout_minimum = Some("1 BNT".into());
    for (level, tag, message, age) in log {
        let entry = LogEntry::new(uptime - age, *level, tag, message);
        state.log_entries.push_back(entry);
    }
    state
}

fn mining_fixture(state_label: &str, log: &[(LogLevel, &str, &str, f64)]) -> TuiStateInner {
    let mut state = dashboard_fixture(state_label, log);
    state.backends_desc = "cpu#1".into();
    state.threads = 16;
    state.height = "71,309".into();
    state.network_hashrate = "53.92 KH/s".into();
    state.round_hashrate = "15.42 KH/s".into();
    state.avg_hashrate = "15.18 KH/s".into();
    state.total_hashes = 1_823_114;
    state.submitted = 212;
    state.share_difficulty = Some(90);
    state.accepted = 211;
    state.stale_shares = 1;
    state.pool_pending = "0.4182 BNT".into();
    state.pool_paid = "12.0000 BNT".into();
    state.device_hashrates = vec![DeviceHashrate {
        name: "cpu#1".into(),
        current: "15.42 KH/s".into(),
        average: "15.18 KH/s".into(),
    }];
    state
}

#[test]
#[ignore = "writes the startup dashboard showcase; pass SEINE_SHOWCASE_DIR to choose where"]
fn export_startup_dashboard_showcase() {
    use LogLevel::{Error, Info, Success, Warn};
    let tuning: &[(LogLevel, &str, &str, f64)] = &[
        (
            Info,
            "BACKEND",
            "detected 1 NVIDIA GPU(s): GeForce RTX 5090",
            3.0,
        ),
        (
            Info,
            "BACKEND",
            "cpu: starting 16 threads (~32.0 GiB RAM)",
            3.0,
        ),
        (
            Info,
            "CONFIG",
            "settings saved to C:\\Users\\miner\\seine\\seine.config.json",
            3.0,
        ),
        (Success, "CONN", "connected to pool", 2.0),
    ];
    let steady: &[(LogLevel, &str, &str, f64)] = &[
        (
            Info,
            "BACKEND",
            "cpu: starting 16 threads (~32.0 GiB RAM)",
            7_500.0,
        ),
        (Success, "CONN", "connected to pool", 7_500.0),
        (Success, "CPU", "tuned to 16 threads", 7_200.0),
        (Success, "SHARE", "share accepted", 2_400.0),
        (Warn, "SHARE", "rejected (stale share)", 2_380.0),
        (Success, "SHARE", "share accepted", 4.0),
    ];
    let outage: &[(LogLevel, &str, &str, f64)] = &[
        (Success, "CONN", "connected to pool", 3_600.0),
        (Success, "SHARE", "share accepted", 60.0),
        (
            Warn,
            "CONN",
            "pool connection lost: connection reset by peer",
            2.0,
        ),
    ];
    let rejected: &[(LogLevel, &str, &str, f64)] =
        &[(Error, "AUTH", "login rejected: unknown payout address", 1.0)];
    let notices: &[(LogLevel, &str, &str, f64)] = &[
        (Info, "BACKEND", "WSL2 gives this VM 12 GiB of RAM, enough for 5 of 32 CPU threads. For more, set memory=66GB under [wsl2] in %UserProfile%\\.wslconfig and run `wsl --shutdown`, or use the Windows build.", 8.0),
        (Info, "BACKEND", "memory runs at 4800 MT/s but is rated 6000 MT/s; enable EXPO/XMP in BIOS for more CPU hashrate", 8.0),
        (Warn, "NVIDIA", "GPU 0: NVRTC could not build the kernel; using the built-in kernel", 6.0),
        (Success, "CONN", "connected to pool", 5.0),
    ];

    let mut starting = dashboard_fixture("checking hardware", &[]);
    starting.pool_address = "---".into();
    starting.before_first_run = true;
    let mut gpu_pending = mining_fixture("working", tuning);
    gpu_pending.pending_gpus = vec!["nvidia"];
    gpu_pending.pending_gpus_since = Some(Instant::now());
    let connecting = mining_fixture("pool-connecting", tuning);
    let counted = |label: &str, log, counts: &[(usize, u32)]| {
        let mut state = mining_fixture(label, log);
        for (row, count) in counts {
            state.log_entries[*row].count = *count;
        }
        state
    };
    let steady_counts = [(3, 96), (5, 115)];
    let working = counted("working", steady, &steady_counts);
    // Long enough to overflow the log panel at every captured size. Only a
    // different line ends a folded row, so accepted runs alternate with rejections.
    let history: Vec<(LogLevel, &str, &str, f64)> = (0..40)
        .map(|row| {
            let age = (40 - row) as f64 * 240.0;
            if row % 2 == 0 {
                (Success, "SHARE", "share accepted", age)
            } else {
                (Warn, "SHARE", "rejected (stale share)", age)
            }
        })
        .collect();
    let history_counts: Vec<(usize, u32)> = (0..40)
        .step_by(2)
        .map(|row| (row, 20 + (row as u32 * 7) % 60))
        .collect();
    let mut scrolled = counted("working", history.as_slice(), &history_counts);
    scrolled.log_scroll = 10;
    let mut scrolled_to_oldest = counted("working", history.as_slice(), &history_counts);
    scrolled_to_oldest.log_scroll = usize::MAX;
    let reconnecting = counted("pool-disconnected", outage, &[(1, 88), (2, 14)]);
    let login_rejected = dashboard_fixture("pool-login-rejected", rejected);
    let advice = mining_fixture("working", notices);
    let mut stopping = counted("working", steady, &steady_counts);
    stopping.stopping = true;

    let mut body = String::new();
    let section = |body: &mut String, title: &str, note: &str, desktop: String, mobile: String| {
        body.push_str(&format!(
            "<section><h2>{title}</h2><p>{note}</p><div class='desktop'>{desktop}</div><div class='mobile'>{mobile}</div></section>"
        ));
    };
    let form = |typed: &str, error: Option<&str>, w, h| {
        capture_form(
            "Welcome to seine. Mining rewards are paid to your Blocknet wallet.",
            "Your address or shortname (@name / $name)",
            "",
            typed,
            error,
            w,
            h,
        )
    };
    section(
        &mut body,
        "First run: address",
        "Empty, before anything is typed.",
        form("", None, 120, 30),
        form("", None, 48, 20),
    );
    section(
        &mut body,
        "First run: invalid address",
        "A typo stays on the form instead of ending the program.",
        form(
            "notanaddress",
            Some("That is not a Blocknet address. Paste the whole address, or use @name."),
            120,
            30,
        ),
        form(
            "notanaddress",
            Some("That is not a Blocknet address. Paste the whole address, or use @name."),
            48,
            20,
        ),
    );
    let pool = |w, h| {
        capture_form(
            "Press Enter to mine on the default pool, or type another.",
            "Pool",
            "stratum+tcp://bntpool.com:3333",
            "",
            None,
            w,
            h,
        )
    };
    section(
        &mut body,
        "First run: pool",
        "The default is filled in, so Enter is all it takes.",
        pool(120, 30),
        pool(48, 20),
    );
    for (title, note, state) in [
        (
            "Starting",
            "The first frame, shown before hardware detection runs.",
            &starting,
        ),
        (
            "Connecting",
            "The startup lines a new miner sees in place of the old settings dump.",
            &connecting,
        ),
        (
            "GPU preparing",
            "CPU mining runs while the GPU gets ready in the devices panel.",
            &gpu_pending,
        ),
        (
            "Setup advice",
            "Advice that can raise hashrate stays, in one or two lines.",
            &advice,
        ),
        (
            "Mining",
            "Identical lines fold into one row: share accepted ×96 means 96 shares in a row were accepted.",
            &working,
        ),
        (
            "Scrolled back",
            "The view holds still while new lines arrive; End returns to live.",
            &scrolled,
        ),
        (
            "Scrolled to the oldest line",
            "The view stops with the oldest line on top and the panel still full.",
            &scrolled_to_oldest,
        ),
        (
            "Reconnecting",
            "Repeated errors fold the same way: ×14 means it happened 14 times in a row.",
            &reconnecting,
        ),
        (
            "Login rejected",
            "An error with no mining stats yet.",
            &login_rejected,
        ),
        (
            "Stopping",
            "Stays on the dashboard during cleanup, with the way out on screen.",
            &stopping,
        ),
    ] {
        section(
            &mut body,
            title,
            note,
            capture_full(state, 120, 30),
            capture_full(state, 48, 24),
        );
    }
    section(
        &mut body,
        "Common window sizes",
        "Mining at 80x24, the smallest common terminal, then 140x45, the size a double-clicked classic console opens at.",
        format!("{}{}", capture_full(&working, 80, 24), capture_full(&working, 140, 45)),
        capture_full(&working, 80, 24),
    );
    section(
        &mut body,
        "Short window",
        "Under the full layout's height the essentials and log remain.",
        capture_full(&working, 120, 16),
        capture_full(&working, 48, 16),
    );

    let revision = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .unwrap();
    let html = format!(
        r#"<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Seine startup dashboard</title>
<style>body{{margin:16px;background:#101318;color:#b4bec8;font:14px system-ui}}h1{{font-size:22px}}h2{{font-size:16px;margin-bottom:4px}}p{{margin:0 0 8px}}pre{{background:#080b10;padding:8px;font:12px/1.25 Consolas,Menlo,monospace;width:max-content;max-width:100%;overflow:auto}}section{{margin:24px 0}}.mobile{{display:none}}@media(max-width:900px){{body{{margin:12px}}.desktop{{display:none}}.mobile{{display:block}}}}</style>
<h1>Seine first-run and startup dashboard</h1><p>The real terminal renderer with fixture data; no miner or network connection runs. 120x30 on desktop widths, 48 columns on phones. Source base: <code>{}</code>.</p>{body}</html>"#,
        String::from_utf8_lossy(&revision.stdout).trim(),
    );
    let dir = std::env::var("SEINE_SHOWCASE_DIR")
        .unwrap_or_else(|_| "target/showcases/startup-dashboard".to_string());
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(std::path::Path::new(&dir).join("index.html"), html).unwrap();
}

#[test]
#[ignore = "exports pool balances using the real terminal renderer"]
fn export_pool_balance_showcase() {
    let mut html = String::from(
        r#"<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Seine pool balance</title>
<style>body{margin:16px;background:#101318;color:#b4bec8;font:14px system-ui}h1{font-size:22px}h2{font-size:16px}pre{background:#080b10;padding:8px;font:12px/1.25 monospace;width:max-content;max-width:100%;overflow:auto}section{margin:24px 0}.mobile{display:none}@media(max-width:700px){body{margin:12px}.desktop{display:none}.mobile{display:block}}</style>
<h1>Seine pool balance</h1><p>Pending includes estimated rewards from unmatured blocks plus confirmed rewards awaiting payout. Estimates can change before maturity. Paid shows completed payouts.</p>
<p>Real terminal renderer with synthetic balances, at desktop and mobile widths.</p>"#,
    );
    for source in [
        "src/miner/mining_pool.rs",
        "src/miner/tui.rs",
        "src/miner/thread_showcase.rs",
    ] {
        let revision = std::process::Command::new("git")
            .args(["hash-object", source])
            .output()
            .unwrap();
        html.push_str(&format!(
            "<p>{source}: <code>{}</code></p>",
            String::from_utf8_lossy(&revision.stdout).trim()
        ));
    }
    for (title, pending, paid, status) in [
        (
            "Loading / waiting for balances",
            "---",
            "---",
            "pool-connected",
        ),
        ("Empty / no rewards yet", "0 BNT", "0 BNT", "hashing"),
        (
            "Estimated rewards / none confirmed yet",
            "0.1096 BNT",
            "123,456.789 BNT",
            "hashing",
        ),
        (
            "Estimated plus confirmed rewards",
            "1.1096 BNT",
            "123,456.789 BNT",
            "hashing",
        ),
        (
            "Legacy pool / confirmed rewards only",
            "1 BNT",
            "2 BNT",
            "hashing",
        ),
        (
            "Error / telemetry unavailable at startup",
            "---",
            "---",
            "pool-connected",
        ),
        (
            "Reconnecting / balances cleared",
            "---",
            "---",
            "pool-disconnected",
        ),
        (
            "Mining disabled / login rejected",
            "---",
            "---",
            "pool-login-rejected",
        ),
    ] {
        let mut state = TuiStateInner::new();
        state.mode = "pool".into();
        state.state = status.into();
        state.pool_address = "demo-miner-address".into();
        state.pool_pending = pending.into();
        state.pool_paid = paid.into();
        let panel = |width| {
            let mut terminal = Terminal::new(TestBackend::new(width, 4)).unwrap();
            terminal
                .draw(|frame| draw_config_wallet_panel(frame, frame.area(), &state))
                .unwrap();
            let buffer = terminal.backend().buffer();
            let mut rendered = String::from("<pre>");
            for y in 0..4 {
                for x in 0..width {
                    let cell = &buffer[(x, y)];
                    rendered.push_str(&format!(
                        "<span style='color:{}'>{}</span>",
                        color(cell.fg),
                        escape(cell.symbol())
                    ));
                }
                rendered.push('\n');
            }
            rendered.push_str("</pre>");
            rendered
        };
        html.push_str(&format!("<section><h2>{title}</h2><div class='desktop'>{}</div><div class='mobile'>{}</div></section>", panel(80), panel(40)));
    }
    html.push_str("</html>");
    let path = std::path::Path::new("docs/showcases/pool-balance");
    std::fs::create_dir_all(path).unwrap();
    std::fs::write(path.join("index.html"), html).unwrap();
}

#[test]
#[ignore = "writes the auto-update showcase; pass SEINE_SHOWCASE_DIR to choose where"]
fn export_auto_update_showcase() {
    use crate::update_check::PendingInstall;
    use LogLevel::{Info, Success};
    let mining: &[(LogLevel, &str, &str, f64)] = &[
        (
            Info,
            "BACKEND",
            "cpu: starting 16 threads (~32.0 GiB RAM)",
            7_500.0,
        ),
        (Success, "CONN", "connected to pool", 7_500.0),
        (Success, "CPU", "tuned to 16 threads", 7_200.0),
        (Success, "SHARE", "share accepted", 4.0),
    ];
    let found = "seine v0.3.2 is available (running v0.3.1): https://github.com/zpalmtree/seine/releases/latest";
    let with = |extra: &[(LogLevel, &str, &str, f64)]| {
        let mut log = mining.to_vec();
        log.extend_from_slice(extra);
        let mut state = mining_fixture("working", &log);
        state.log_entries[3].count = 96;
        state
    };
    let countdown_log = [
        (Info, "UPDATE", found, 18.0),
        (
            Info,
            "UPDATE",
            "seine v0.3.2 installs in 60s: press u to update now or s to skip this version",
            18.0,
        ),
    ];
    let mut counting = with(&countdown_log);
    counting.update_available = Some("v0.3.2".into());
    counting.pending_install = Some(PendingInstall {
        latest: "v0.3.2".into(),
        deadline: Some(Instant::now() + Duration::from_millis(42_500)),
    });
    let mut installing = with(&countdown_log);
    installing.update_available = Some("v0.3.2".into());
    installing.pending_install = Some(PendingInstall {
        latest: "v0.3.2".into(),
        deadline: None,
    });
    let mut skipped = with(&[
        countdown_log[0],
        countdown_log[1],
        (
            Info,
            "UPDATE",
            "skipped seine v0.3.2; staying on v0.3.1 (seine --self-update installs it later)",
            9.0,
        ),
    ]);
    skipped.update_available = Some("v0.3.2".into());
    let mut announced = with(&[(Info, "UPDATE", found, 18.0)]);
    announced.update_available = Some("v0.3.2".into());
    let mut restarted = mining_fixture(
        "working",
        &[
            (
                Success,
                "UPDATE",
                "updated to seine v0.3.2 (was v0.3.1)",
                12.0,
            ),
            (
                Info,
                "BACKEND",
                "cpu: starting 16 threads (~32.0 GiB RAM)",
                12.0,
            ),
            (Success, "CONN", "connected to pool", 10.0),
            (Success, "SHARE", "share accepted", 2.0),
        ],
    );
    restarted.version = "v0.3.2".into();
    restarted.accepted = 3;

    let mut body = String::new();
    for (title, note, state) in [
        (
            "Countdown",
            "A newer release was found. Mining continues for 60 seconds; u installs now, s skips this version.",
            &counting,
        ),
        (
            "Installing",
            "After the countdown or u: the signed release downloads and is checked before Seine restarts.",
            &installing,
        ),
        (
            "Skipped",
            "s keeps this version. Seine remembers the skip and offers the next release as usual.",
            &skipped,
        ),
        (
            "Restarted",
            "The updated miner reports the update in its first log line.",
            &restarted,
        ),
        (
            "Notice only (--no-auto-update or a self-built binary)",
            "No countdown: the header and log only announce the release.",
            &announced,
        ),
    ] {
        body.push_str(&format!(
            "<section><h2>{title}</h2><p>{note}</p><div class='desktop'>{}{}</div><div class='mobile'>{}</div></section>",
            capture_full(state, 120, 30),
            capture_full(state, 80, 24),
            capture_full(state, 48, 24),
        ));
    }
    let revision = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .unwrap();
    let html = format!(
        r#"<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Seine auto-update</title>
<style>body{{margin:16px;background:#101318;color:#b4bec8;font:14px system-ui}}h1{{font-size:22px}}h2{{font-size:16px;margin-bottom:4px}}p{{margin:0 0 8px}}pre{{background:#080b10;padding:8px;font:12px/1.25 Consolas,Menlo,monospace;width:max-content;max-width:100%;overflow:auto}}section{{margin:24px 0}}.mobile{{display:none}}@media(max-width:900px){{body{{margin:12px}}.desktop{{display:none}}.mobile{{display:block}}}}</style>
<h1>Seine auto-update countdown</h1><p>The real terminal renderer with fixture data; no miner, network or update runs. 120x30 and 80x24 on desktop widths, 48 columns on phones. Source base: <code>{}</code>.</p>{body}</html>"#,
        String::from_utf8_lossy(&revision.stdout).trim(),
    );
    let dir = std::env::var("SEINE_SHOWCASE_DIR")
        .unwrap_or_else(|_| "target/showcases/auto-update".to_string());
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(std::path::Path::new(&dir).join("index.html"), html).unwrap();
}
