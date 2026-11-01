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
                request.rejection.unwrap().send(message).unwrap();
                state.poll_thread_rejection();
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
#[ignore = "writes the autotune showcase using the real log component and fixture messages"]
fn export_autotune_showcase() {
    let mut html = String::from(
        r#"<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Seine tuning startup</title>
<style>body{margin:16px;background:#101318;color:#b4bec8;font:14px system-ui}h1{font-size:22px}h2{font-size:16px}pre{background:#080b10;padding:8px;font:12px/1.25 monospace;width:max-content;max-width:100%;overflow:auto}section{margin:24px 0}.mobile{display:none}code{overflow-wrap:anywhere}@media(max-width:700px){body{margin:12px}.desktop{display:none}.mobile{display:block}}</style>
<h1>Seine tuning startup</h1><p>Actual Ratatui log component with fixture messages. CPU tunes while mining; NVIDIA tunes at startup. No mining or network connection is started.</p>"#,
    );
    for path in [
        "src/miner/mod.rs",
        "src/miner/adaptive_cpu.rs",
        "src/backend/nvidia.rs",
        "src/miner/tui.rs",
    ] {
        let revision = std::process::Command::new("git")
            .args(["hash-object", path])
            .output()
            .unwrap();
        html.push_str(&format!(
            "<p>{path}: <code>{}</code></p>",
            String::from_utf8_lossy(&revision.stdout).trim()
        ));
    }
    let cpu = include_str!("mod.rs");
    let live = include_str!("adaptive_cpu.rs");
    let nvidia = include_str!("../backend/nvidia.rs");
    for (title, tag, prefix, reason, action, warning) in [
        (
            "CPU / empty cache",
            "MINER",
            "cpu-autotune | ",
            "no saved tuning result",
            "; tuning while mining",
            false,
        ),
        (
            "CPU / build changed",
            "MINER",
            "cpu-autotune | ",
            "miner build changed",
            "; tuning while mining",
            false,
        ),
        (
            "CPU / hardware changed",
            "MINER",
            "cpu-autotune | ",
            "CPU or memory configuration changed",
            "; tuning while mining",
            false,
        ),
        (
            "CPU / runtime changed",
            "MINER",
            "cpu-autotune | ",
            "operating system or runtime changed",
            "; tuning while mining",
            false,
        ),
        (
            "CPU / topology changed",
            "MINER",
            "cpu-autotune | ",
            "CPU affinity or topology changed",
            "; tuning while mining",
            false,
        ),
        (
            "CPU / settings changed",
            "MINER",
            "cpu-autotune | ",
            "tuning settings changed",
            "; tuning while mining",
            false,
        ),
        (
            "CPU / invalid cache",
            "MINER",
            "cpu-autotune | ",
            "saved tuning result is invalid",
            "; tuning while mining",
            false,
        ),
        (
            "CPU / unreadable cache",
            "MINER",
            "cpu-autotune | ",
            "saved tuning result could not be read",
            "; tuning while mining",
            false,
        ),
        (
            "CPU / old cache format",
            "MINER",
            "cpu-autotune | ",
            "cache format changed",
            "; tuning while mining",
            false,
        ),
        (
            "CPU / manual or disabled",
            "MINER",
            "cpu-autotune | ",
            "disabled; using configured thread count",
            "",
            false,
        ),
        (
            "CPU benchmark / expired cache",
            "BENCH",
            "cpu-autotune | ",
            "cache expired",
            " (age=337h > 336h); re-running autotune",
            false,
        ),
        (
            "CPU benchmark / live result",
            "BENCH",
            "cpu-autotune | ",
            "cached result is from live mining",
            "; re-running autotune",
            false,
        ),
        (
            "NVIDIA / empty cache",
            "NVIDIA",
            "nvidia-autotune | GPU 0: ",
            "no saved tuning result",
            "; running autotune",
            false,
        ),
        (
            "NVIDIA / build changed",
            "NVIDIA",
            "nvidia-autotune | GPU 0: ",
            "miner build changed",
            "; running autotune",
            false,
        ),
        (
            "NVIDIA / another GPU",
            "NVIDIA",
            "nvidia-autotune | GPU 1: ",
            "no saved tuning result for this GPU",
            "; running autotune",
            false,
        ),
        (
            "NVIDIA / kernel changed",
            "NVIDIA",
            "nvidia-autotune | GPU 0: ",
            "CUDA kernel changed",
            "; running autotune",
            false,
        ),
        (
            "NVIDIA / compiler changed",
            "NVIDIA",
            "nvidia-autotune | GPU 0: ",
            "CUDA compiler changed",
            "; running autotune",
            false,
        ),
        (
            "NVIDIA / runtime changed",
            "NVIDIA",
            "nvidia-autotune | GPU 0: ",
            "operating system or runtime changed",
            "; running autotune",
            false,
        ),
        (
            "NVIDIA / settings changed",
            "NVIDIA",
            "nvidia-autotune | GPU 0: ",
            "mining or tuning settings changed",
            "; running autotune",
            false,
        ),
        (
            "NVIDIA / old cache format",
            "NVIDIA",
            "nvidia-autotune | GPU 0: ",
            "cache format changed",
            "; running autotune",
            false,
        ),
        (
            "NVIDIA / invalid cache",
            "NVIDIA",
            "nvidia-autotune | GPU 0: ",
            "saved tuning results are invalid",
            "; running autotune",
            false,
        ),
        (
            "NVIDIA / unreadable cache",
            "NVIDIA",
            "nvidia-autotune | GPU 0: ",
            "saved tuning results could not be read",
            "; running autotune",
            false,
        ),
        (
            "NVIDIA / success and reuse",
            "NVIDIA",
            "nvidia-autotune | GPU 0: ",
            "using cached tuning",
            "",
            false,
        ),
        (
            "NVIDIA / explicit override",
            "NVIDIA",
            "nvidia-autotune | GPU 0: ",
            "disabled by explicit register cap",
            "",
            false,
        ),
        (
            "NVIDIA / tuning failure",
            "NVIDIA",
            "nvidia-autotune | GPU 0: ",
            "tuning failed; using default tuning",
            "",
            true,
        ),
    ] {
        assert!(
            cpu.contains(reason) || live.contains(reason) || nvidia.contains(reason),
            "stale fixture: {reason}"
        );
        let mut state = TuiStateInner::new();
        state.push_log(LogEntry {
            elapsed_secs: 0.0,
            level: if warning {
                LogLevel::Warn
            } else {
                LogLevel::Info
            },
            tag: tag.into(),
            message: format!("{prefix}{reason}{action}"),
        });
        html.push_str(&format!("<section><h2>{title}</h2><div class='desktop'>{}</div><div class='mobile'>{}</div></section>", capture_log(&state, 120, 5), capture_log(&state, 48, 10)));
    }
    for reason in [
        "cached starting point",
        "waiting for RAM",
        "initial estimate",
    ] {
        assert!(live.contains(reason));
        let mut state = TuiStateInner::new();
        let threads = if reason == "waiting for RAM" { 0 } else { 2 };
        state.push_log(LogEntry { elapsed_secs: 0.0, level: LogLevel::Info, tag: "MINER".into(), message: format!("cpu-autotune | starting with {threads} threads ({reason}); tuning while mining, capacity=4, worker slots=4, currently available={threads}") });
        html.push_str(&format!("<section><h2>CPU / {reason}</h2><div class='desktop'>{}</div><div class='mobile'>{}</div></section>", capture_log(&state, 120, 5), capture_log(&state, 48, 12)));
    }
    html.push_str("</html>");
    let path = std::path::Path::new("docs/showcases/autotune-startup");
    std::fs::create_dir_all(path).unwrap();
    std::fs::write(path.join("index.html"), html).unwrap();
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
            });
        }
        html.push_str(&format!("<section id='{slug}'><h2>{title}</h2><div class='desktop'>{}</div><div class='mobile'>{}</div></section>", capture(&state, 120, 30), capture(&state, 40, 18)));
    }
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
