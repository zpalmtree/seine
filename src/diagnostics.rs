//! Bounded failure counters. Free-form messages never enter the upload state.
use crate::config::{BackendKind, Config, CpuAffinityMode, MiningMode};
use seine_telemetry::{
    DiagnosticCode, DiagnosticEvent, DiagnosticKind, DiagnosticOperation, Diagnostics,
    FailureDetail, FailureReason, PreviousRun, Report, RunPhase, MAX_BODY_BYTES,
};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    panic::Location,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc, Mutex, Once, Weak,
    },
    time::{Duration, Instant, SystemTime},
};

static ACTIVE: Mutex<Option<Weak<Recorder>>> = Mutex::new(None);
static PANIC_HOOK: Once = Once::new();
#[cfg(test)]
pub(crate) static TEST_LOCK: Mutex<()> = Mutex::new(());
type Key = (
    DiagnosticCode,
    DiagnosticKind,
    String,
    u32,
    Option<String>,
    Option<FailureDetail>,
    RunPhase,
);

pub(crate) struct Recorder {
    started: Instant,
    context: Diagnostics,
    events: Mutex<BTreeMap<Key, DiagnosticEvent>>,
    recent: Mutex<(Vec<DiagnosticEvent>, bool)>,
    dropped: AtomicU64,
    failed: AtomicBool,
    previous_unclean: AtomicBool,
    phase: Mutex<RunPhase>,
    shutdown_request: Mutex<Option<seine_telemetry::ShutdownRequest>>,
    cleanup_stage: Mutex<Option<seine_telemetry::CleanupStage>>,
    nvidia_software: Mutex<Option<seine_telemetry::NvidiaSoftware>>,
    checkpoint_store: Mutex<Option<(Weak<Store>, String)>>,
    previous_run: Mutex<Option<PreviousRun>>,
    first_hashes: Mutex<BTreeMap<u64, (String, u64)>>,
}

/// Session-owned origin survives a stop arriving before the recorder is installed.
#[derive(Debug, Default)]
pub(crate) struct StopOrigin(Mutex<(Option<seine_telemetry::ShutdownRequest>, Weak<Recorder>)>);
impl StopOrigin {
    pub(crate) fn request(&self, reason: seine_telemetry::ShutdownRequest) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let reason = *state.0.get_or_insert(reason);
        if let Some(recorder) = state.1.upgrade() {
            recorder.stop_requested(reason);
        }
    }

    pub(crate) fn attach(&self, recorder: &Arc<Recorder>) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        state.1 = Arc::downgrade(recorder);
        if let Some(reason) = state.0 {
            recorder.stop_requested(reason);
        }
    }
}

pub(crate) struct Guard(pub Arc<Recorder>);
impl Drop for Guard {
    fn drop(&mut self) {
        if let Ok(mut active) = ACTIVE.lock() {
            if active
                .as_ref()
                .and_then(Weak::upgrade)
                .is_some_and(|r| Arc::ptr_eq(&r, &self.0))
            {
                *active = None;
            }
        }
    }
}

pub(crate) fn context(cfg: &Config) -> Diagnostics {
    let mut backends: Vec<_> = cfg
        .backend_specs
        .iter()
        .map(|b| backend_name(b.kind).to_owned())
        .collect();
    backends.sort();
    backends.dedup();
    Diagnostics {
        shutdown_request: None,
        cleanup_stage: None,
        nvidia_software: None,
        phase: None,
        previous_run: None,
        build_commit: fingerprint(option_env!("SEINE_GIT_COMMIT").unwrap_or("unknown")),
        source_fingerprint: fingerprint(crate::runtime_identity::source_fingerprint()),
        mode: match cfg.mode {
            MiningMode::Pool => "pool",
            MiningMode::Daemon => "daemon",
        }
        .into(),
        requested_backends: backends,
        requested_cpu_threads: cfg.threads.min(65536) as u32,
        cpu_page_mode: cfg.cpu_page_mode.as_str().into(),
        cpu_affinity: match cfg.cpu_affinity {
            CpuAffinityMode::Auto => "auto",
            CpuAffinityMode::Off => "off",
            CpuAffinityMode::Os => "os",
            CpuAffinityMode::PcoreOnly => "pcore-only",
        }
        .into(),
        cpu_autotune: cfg.cpu_autotune_threads,
        uptime_secs: 0,
        reason: "periodic".into(),
        previous_run_unclean: false,
        dropped_events: 0,
        dropped_backend_samples: 0,
        events: Vec::new(),
        update: None,
        pool: (cfg.mode == MiningMode::Pool).then(|| pool_setup(cfg)),
        amd_detection: None,
        unsupported_work_paused_secs: None,
        backend_startup: Some(expected_backends(cfg)),
    }
}

fn expected_backends(cfg: &Config) -> seine_telemetry::BackendStartup {
    let count = |kind: BackendKind| {
        cfg.backend_specs
            .iter()
            .filter(|spec| spec.kind == kind)
            .count()
            .min(1024) as u32
    };
    seine_telemetry::BackendStartup {
        expected_cpu: count(BackendKind::Cpu),
        expected_nvidia: count(BackendKind::Nvidia),
        expected_amd: count(BackendKind::Amd),
        expected_metal: count(BackendKind::Metal),
        first_hashes: Vec::new(),
    }
}

/// A backend instance reported its first hash of this run.
pub(crate) fn backend_first_hash(backend: &str, instance: u64) {
    if let Some(recorder) = active_recorder() {
        let elapsed = recorder.started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        recorder
            .first_hashes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(instance)
            .or_insert_with(|| (backend.to_owned(), elapsed));
    }
}

/// Pool settings as booleans and a count; never hosts, addresses or memos.
fn pool_setup(cfg: &Config) -> seine_telemetry::PoolStatus {
    let urls: Vec<&str> = cfg
        .pool_url
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .collect();
    let primary = urls.first().map(|url| url.to_ascii_lowercase());
    seine_telemetry::PoolStatus {
        tls: primary.as_deref().is_some_and(|url| {
            url.starts_with("stratum+ssl://") || url.starts_with("stratum+tls://")
        }),
        socks: cfg.pool_proxy.is_some(),
        payout_memo: cfg.payout_memo.is_some(),
        backup_pools: urls.len().saturating_sub(1).min(16) as u32,
        ..Default::default()
    }
}

// Process-wide schema-14 counters. The update checker and pool sessions run
// outside any one recorder, so each report takes the counts for its window.

static UPDATE_ACTIVE: AtomicBool = AtomicBool::new(false);
static UPDATE_AUTO_INSTALL: AtomicBool = AtomicBool::new(false);
static UPDATE_CHECKS_API: AtomicU32 = AtomicU32::new(0);
static UPDATE_CHECKS_WEB: AtomicU32 = AtomicU32::new(0);
static UPDATE_CHECKS_FAILED: AtomicU32 = AtomicU32::new(0);
static UPDATE_INSTALLS: AtomicU32 = AtomicU32::new(0);
static UPDATE_INSTALL_FAILURES: AtomicU32 = AtomicU32::new(0);
static UPDATE_LAST_FAILURE: Mutex<Option<seine_telemetry::UpdateFailure>> = Mutex::new(None);

/// How a release lookup was answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UpdateCheck {
    Api,
    Web,
    Failed,
}

/// The background release checker started; `auto_install` when it installs.
pub(crate) fn update_checker_started(auto_install: bool) {
    UPDATE_AUTO_INSTALL.store(auto_install, Ordering::Relaxed);
    UPDATE_ACTIVE.store(true, Ordering::Relaxed);
}

pub(crate) fn update_checked(check: UpdateCheck) {
    match check {
        UpdateCheck::Api => &UPDATE_CHECKS_API,
        UpdateCheck::Web => &UPDATE_CHECKS_WEB,
        UpdateCheck::Failed => &UPDATE_CHECKS_FAILED,
    }
    .fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn update_installed() {
    UPDATE_INSTALLS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn update_install_failed(stage: seine_telemetry::UpdateFailure) {
    UPDATE_INSTALL_FAILURES.fetch_add(1, Ordering::Relaxed);
    *UPDATE_LAST_FAILURE
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(stage);
}

fn take_update_status() -> Option<seine_telemetry::UpdateStatus> {
    if !UPDATE_ACTIVE.load(Ordering::Relaxed) {
        return None;
    }
    let take = |counter: &AtomicU32| counter.swap(0, Ordering::Relaxed).min(100_000);
    let install_failures = take(&UPDATE_INSTALL_FAILURES);
    let last = UPDATE_LAST_FAILURE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take();
    Some(seine_telemetry::UpdateStatus {
        auto_install: UPDATE_AUTO_INSTALL.load(Ordering::Relaxed),
        checks_api: take(&UPDATE_CHECKS_API),
        checks_web: take(&UPDATE_CHECKS_WEB),
        checks_failed: take(&UPDATE_CHECKS_FAILED),
        installs: take(&UPDATE_INSTALLS),
        install_failures,
        last_install_failure: last.filter(|_| install_failures > 0),
    })
}

static POOL_FAILOVERS: AtomicU32 = AtomicU32::new(0);
static POOL_RECONNECTS: AtomicU32 = AtomicU32::new(0);
static POOL_REFILLS: AtomicU32 = AtomicU32::new(0);

/// Sessions currently on a backup pool, when that started, and backup time
/// accumulated in this window.
struct BackupClock {
    sessions: u32,
    since: Option<Instant>,
    accumulated: Duration,
}

static POOL_BACKUP: Mutex<BackupClock> = Mutex::new(BackupClock {
    sessions: 0,
    since: None,
    accumulated: Duration::ZERO,
});

pub(crate) fn pool_failover() {
    POOL_FAILOVERS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn pool_reconnect() {
    POOL_RECONNECTS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn pool_refill() {
    POOL_REFILLS.fetch_add(1, Ordering::Relaxed);
}

/// A pool session moved onto (`true`) or off (`false`) a backup pool.
pub(crate) fn pool_on_backup(on_backup: bool) {
    let mut clock = POOL_BACKUP.lock().unwrap_or_else(|e| e.into_inner());
    if on_backup {
        if clock.sessions == 0 {
            clock.since = Some(Instant::now());
        }
        clock.sessions += 1;
    } else if clock.sessions > 0 {
        clock.sessions -= 1;
        if clock.sessions == 0 {
            if let Some(since) = clock.since.take() {
                clock.accumulated += since.elapsed();
            }
        }
    }
}

fn take_pool_counts(status: &mut seine_telemetry::PoolStatus, uptime_secs: u64) {
    status.failovers = POOL_FAILOVERS.swap(0, Ordering::Relaxed).min(1_000_000);
    status.reconnects = POOL_RECONNECTS.swap(0, Ordering::Relaxed).min(1_000_000);
    status.refills = POOL_REFILLS.swap(0, Ordering::Relaxed).min(100_000_000);
    let mut clock = POOL_BACKUP.lock().unwrap_or_else(|e| e.into_inner());
    let mut on_backup = std::mem::take(&mut clock.accumulated);
    if let Some(since) = clock.since.as_mut() {
        on_backup += since.elapsed();
        *since = Instant::now();
    }
    status.secs_on_backup = on_backup.as_secs().min(uptime_secs.max(1)) as u32;
}

/// When mining paused on unsupported work, and pause time in this window.
static UNSUPPORTED_PAUSE: Mutex<(Option<Instant>, Duration, bool)> =
    Mutex::new((None, Duration::ZERO, false));

/// Mining paused on (`true`) or resumed after (`false`) work this build
/// cannot mine. The first pause also records a typed diagnostic.
#[track_caller]
pub(crate) fn unsupported_work(paused: bool) {
    let first_pause = {
        let mut pause = UNSUPPORTED_PAUSE.lock().unwrap_or_else(|e| e.into_inner());
        match (paused, pause.0) {
            (true, None) => {
                pause.0 = Some(Instant::now());
                pause.2 = true;
                true
            }
            (false, Some(since)) => {
                pause.1 += since.elapsed();
                pause.0 = None;
                false
            }
            _ => false,
        }
    };
    if first_pause {
        record_failure(
            DiagnosticCode::Warning,
            DiagnosticKind::General,
            None,
            FailureDetail::new(
                FailureReason::UnsupportedWork,
                DiagnosticOperation::ReceiveWork,
            ),
        );
    }
}

fn take_unsupported_pause(uptime_secs: u64) -> Option<u32> {
    let mut pause = UNSUPPORTED_PAUSE.lock().unwrap_or_else(|e| e.into_inner());
    if !pause.2 {
        return None;
    }
    let mut paused = std::mem::take(&mut pause.1);
    match pause.0.as_mut() {
        Some(since) => {
            paused += since.elapsed();
            *since = Instant::now();
        }
        // Report once more after the pause ended, then stop sending the field.
        None => pause.2 = false,
    }
    Some(paused.as_secs().min(uptime_secs.max(1)) as u32)
}

static AMD_DETECTION: Mutex<Option<seine_telemetry::AmdDetection>> = Mutex::new(None);

/// AMD devices found at startup, reported with every window of the run.
pub(crate) fn set_amd_detection(detection: seine_telemetry::AmdDetection) {
    *AMD_DETECTION.lock().unwrap_or_else(|e| e.into_inner()) = Some(detection);
}

pub(crate) fn install(context: Diagnostics) -> Guard {
    let recorder = Arc::new(Recorder {
        started: Instant::now(),
        context,
        events: Mutex::new(BTreeMap::new()),
        recent: Mutex::new((Vec::new(), false)),
        dropped: AtomicU64::new(0),
        failed: AtomicBool::new(false),
        previous_unclean: AtomicBool::new(false),
        phase: Mutex::new(RunPhase::Starting),
        shutdown_request: Mutex::new(None),
        cleanup_stage: Mutex::new(None),
        nvidia_software: Mutex::new(None),
        checkpoint_store: Mutex::new(None),
        previous_run: Mutex::new(None),
        first_hashes: Mutex::new(BTreeMap::new()),
    });
    if let Ok(mut active) = ACTIVE.lock() {
        *active = Some(Arc::downgrade(&recorder));
    }
    PANIC_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            // Never inspect or serialize the panic payload (it can contain secrets).
            if let Some(location) = info.location() {
                record_at(
                    DiagnosticCode::RustPanic,
                    DiagnosticKind::General,
                    None,
                    location,
                );
            }
            previous(info);
        }));
    });
    Guard(recorder)
}

fn fingerprint(value: &str) -> String {
    if !value.is_empty() && value.len() <= 64 && value.bytes().all(|b| b.is_ascii_hexdigit()) {
        value.into()
    } else {
        "unknown".into()
    }
}

fn normalize_source(source: &str) -> String {
    let normalized = source.replace('\\', "/");
    if seine_telemetry::valid_source(&normalized) {
        return normalized;
    }
    // Match complete, fixed dependency suffixes. Never upload a Cargo home,
    // username, registry directory, arbitrary filename, or panic payload.
    for &allowed in seine_telemetry::DEPENDENCY_SOURCES {
        let suffix = allowed.strip_prefix("deps/").unwrap();
        if normalized == suffix || normalized.ends_with(&format!("/{suffix}")) {
            return allowed.into();
        }
    }
    "unknown".into()
}

fn event_priority(event: &DiagnosticEvent) -> u8 {
    match event.code {
        DiagnosticCode::RustPanic => 3,
        DiagnosticCode::Warning | DiagnosticCode::BackendWarning => {
            u8::from(event.detail.is_some())
        }
        _ => 2,
    }
}

pub(crate) fn backend_name(kind: BackendKind) -> &'static str {
    match kind {
        BackendKind::Cpu => "cpu",
        BackendKind::Nvidia => "nvidia",
        BackendKind::Amd => "amd",
        BackendKind::Metal => "metal",
    }
}

/// A hint only: source location + build identify the exact path for investigation.
/// The input is inspected locally; no message, substring or message hash is retained.
pub(crate) fn classify(message: &str) -> DiagnosticKind {
    let text = message
        .chars()
        .take(2048)
        .collect::<String>()
        .to_ascii_lowercase();
    if text.contains("out of memory")
        || text.contains("allocation failed")
        || text.contains("insufficient memory")
    {
        DiagnosticKind::Memory
    } else if text.contains("timed out") || text.contains("timeout") {
        DiagnosticKind::Timeout
    } else if text.contains("unauthorized") || text.contains("authentication") {
        DiagnosticKind::Authentication
    } else if text.contains("driver") {
        DiagnosticKind::Driver
    } else if text.contains("compilation")
        || text.contains("compile failed")
        || text.contains("nvrtc")
        || text.contains("hiprtc")
    {
        DiagnosticKind::Compilation
    } else if text.contains("unsupported") || text.contains("not supported") {
        DiagnosticKind::UnsupportedHardware
    } else if text.contains("share") && text.contains("reject") {
        DiagnosticKind::RejectedShare
    } else if text.contains("connect") || text.contains("socket") || text.contains("broken pipe") {
        DiagnosticKind::Connection
    } else if text.contains("tuning") || text.contains("autotune") {
        DiagnosticKind::Tuning
    } else {
        DiagnosticKind::General
    }
}

#[track_caller]
pub(crate) fn record(code: DiagnosticCode, kind: DiagnosticKind, backend: Option<&str>) {
    record_at(code, kind, backend, Location::caller());
}

fn record_at(
    code: DiagnosticCode,
    kind: DiagnosticKind,
    backend: Option<&str>,
    location: &Location<'_>,
) {
    let Ok(active) = ACTIVE.try_lock() else {
        return;
    };
    let Some(recorder) = active.as_ref().and_then(Weak::upgrade) else {
        return;
    };
    drop(active);
    recorder.record_detail(code, kind, backend, location.file(), location.line(), None);
}

/// Exact, typed context captured before errors are converted to display strings.
#[track_caller]
pub(crate) fn record_failure(
    code: DiagnosticCode,
    kind: DiagnosticKind,
    backend: Option<&str>,
    detail: FailureDetail,
) {
    let Ok(active) = ACTIVE.try_lock() else {
        return;
    };
    let Some(recorder) = active.as_ref().and_then(Weak::upgrade) else {
        return;
    };
    drop(active);
    let location = Location::caller();
    recorder.record_detail(
        code,
        kind,
        backend,
        location.file(),
        location.line(),
        Some(detail),
    );
}

pub(crate) fn set_phase(phase: RunPhase) {
    let Ok(active) = ACTIVE.try_lock() else {
        return;
    };
    if let Some(recorder) = active.as_ref().and_then(Weak::upgrade) {
        *recorder.phase.lock().unwrap_or_else(|e| e.into_inner()) = phase;
    }
}

// Take an owned reference before doing disk IO; never hold ACTIVE while writing.
fn active_recorder() -> Option<Arc<Recorder>> {
    ACTIVE.lock().ok()?.as_ref()?.upgrade()
}

pub(crate) fn stop_requested(reason: seine_telemetry::ShutdownRequest) {
    if let Some(recorder) = active_recorder() {
        recorder.stop_requested(reason);
    }
}

pub(crate) fn cleanup_stage(stage: seine_telemetry::CleanupStage) {
    if let Some(recorder) = active_recorder() {
        *recorder
            .cleanup_stage
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(stage);
        *recorder.phase.lock().unwrap_or_else(|e| e.into_inner()) = RunPhase::ShuttingDown;
        recorder.persist_checkpoint();
    }
}

#[cfg(any(feature = "nvidia", test))]
pub(crate) fn nvidia_software(software: seine_telemetry::NvidiaSoftware) {
    if !software.validate() {
        return;
    }
    if let Some(recorder) = active_recorder() {
        *recorder
            .nvidia_software
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(software);
        recorder.persist_checkpoint();
    }
}

#[track_caller]
pub(crate) fn allocation_failure(large: bool, bytes: usize, os_error: Option<i64>) {
    record_failure(
        DiagnosticCode::Warning,
        DiagnosticKind::Memory,
        Some("cpu"),
        FailureDetail {
            reason: if large {
                FailureReason::HugePagesUnavailable
            } else {
                FailureReason::AllocationFailed
            },
            operation: DiagnosticOperation::CpuAllocation,
            os_error,
            requested_bytes: Some(bytes as u64),
            ready_workers: None,
            expected_workers: None,
        },
    );
}

pub(crate) fn io_detail(operation: DiagnosticOperation, error: &std::io::Error) -> FailureDetail {
    use std::io::ErrorKind;
    let reason = if operation == DiagnosticOperation::PoolResolve {
        FailureReason::DnsFailed
    } else {
        match error.kind() {
            ErrorKind::ConnectionRefused => FailureReason::ConnectionRefused,
            ErrorKind::TimedOut => FailureReason::ConnectionTimedOut,
            ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted | ErrorKind::BrokenPipe => {
                FailureReason::ConnectionReset
            }
            _ => FailureReason::IoFailed,
        }
    };
    FailureDetail {
        reason,
        operation,
        os_error: error.raw_os_error().map(i64::from),
        requested_bytes: None,
        ready_workers: None,
        expected_workers: None,
    }
}

#[track_caller]
pub(crate) fn record_io(operation: DiagnosticOperation, error: &std::io::Error) {
    record_failure(
        DiagnosticCode::Warning,
        DiagnosticKind::Connection,
        None,
        io_detail(operation, error),
    );
}

impl Recorder {
    #[cfg(test)]
    fn record(
        &self,
        code: DiagnosticCode,
        kind: DiagnosticKind,
        backend: Option<&str>,
        source: &str,
        line: u32,
    ) {
        self.record_detail(code, kind, backend, source, line, None);
    }

    fn record_detail(
        &self,
        code: DiagnosticCode,
        kind: DiagnosticKind,
        backend: Option<&str>,
        source: &str,
        line: u32,
        detail: Option<FailureDetail>,
    ) {
        if matches!(code, DiagnosticCode::RunFailed | DiagnosticCode::RustPanic) {
            self.failed.store(true, Ordering::Relaxed);
        }
        let Ok(mut events) = self.events.try_lock() else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let normalized = normalize_source(source);
        let source = normalized.as_str();
        let phase = *self.phase.lock().unwrap_or_else(|e| e.into_inner());
        let backend = backend
            .filter(|b| matches!(*b, "cpu" | "nvidia" | "amd" | "metal"))
            .map(str::to_owned);
        let key = (
            code,
            kind,
            source.to_owned(),
            line.min(1_000_000),
            backend.clone(),
            detail.clone(),
            phase,
        );
        let now = self.started.elapsed().as_secs();
        self.remember(DiagnosticEvent {
            code,
            kind,
            detail: detail.clone(),
            phase: Some(phase),
            source: source.into(),
            line: line.min(1_000_000),
            backend: backend.clone(),
            count: 1,
            first_seen_secs: now,
            last_seen_secs: now,
        });
        if events.len() >= 32 && !events.contains_key(&key) {
            let is_error = !matches!(
                code,
                DiagnosticCode::Warning | DiagnosticCode::BackendWarning
            );
            if is_error || detail.is_some() {
                // Keep specific causes when generic startup warnings fill the window.
                let is_warning = |k: &&Key| {
                    matches!(
                        k.0,
                        DiagnosticCode::Warning | DiagnosticCode::BackendWarning
                    )
                };
                let victim = events
                    .keys()
                    .filter(is_warning)
                    .find(|k| k.5.is_none())
                    .or_else(|| {
                        if is_error {
                            events.keys().find(is_warning)
                        } else {
                            None
                        }
                    })
                    .cloned();
                if let Some(victim) = victim {
                    if let Some(evicted) = events.remove(&victim) {
                        self.dropped.fetch_add(evicted.count, Ordering::Relaxed);
                    }
                }
            }
            if events.len() >= 32 {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        let event = events.entry(key).or_insert_with(|| DiagnosticEvent {
            code,
            kind,
            detail,
            phase: Some(phase),
            source: source.into(),
            line: line.min(1_000_000),
            backend,
            count: 0,
            first_seen_secs: now,
            last_seen_secs: now,
        });
        event.count = event.count.saturating_add(1);
        event.last_seen_secs = now;
    }

    fn remember(&self, mut event: DiagnosticEvent) {
        let mut recent = self.recent.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(index) = recent.0.iter().position(|e| {
            e.code == event.code
                && e.kind == event.kind
                && e.source == event.source
                && e.line == event.line
                && e.backend == event.backend
                && e.detail == event.detail
                && e.phase == event.phase
        }) {
            let old = recent.0.remove(index);
            event.count = old.count.saturating_add(1);
            event.first_seen_secs = old.first_seen_secs;
        } else if recent.0.len() == 8 {
            recent.1 = true;
            // Oldest group of the lowest priority goes first. Routine reconnect
            // warnings must not erase a panic or a precise allocation failure.
            let index = recent
                .0
                .iter()
                .enumerate()
                .min_by_key(|(_, e)| event_priority(e))
                .map(|(i, _)| i)
                .unwrap();
            if event_priority(&event) < event_priority(&recent.0[index]) {
                return;
            }
            recent.0.remove(index);
        }
        recent.0.push(event);
    }

    pub(crate) fn attach_checkpoint_store(&self, store: &Arc<Store>, installation_id: &str) {
        *self
            .checkpoint_store
            .lock()
            .unwrap_or_else(|e| e.into_inner()) =
            Some((Arc::downgrade(store), installation_id.to_owned()));
        self.persist_checkpoint();
    }

    pub(crate) fn persist_checkpoint(&self) {
        // Serialize transition snapshots so a delayed writer cannot overwrite a
        // newer stage. Weak ownership preserves opt-out and stale-owner safety.
        let destination = self
            .checkpoint_store
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some((store, id)) = destination.as_ref() {
            if let Some(store) = store.upgrade() {
                store.checkpoint(id, self.checkpoint());
            }
        }
    }

    fn stop_requested(&self, reason: seine_telemetry::ShutdownRequest) {
        self.shutdown_request
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert_with(|| crate::shutdown::requested().unwrap_or(reason));
        self.persist_checkpoint();
    }

    pub(crate) fn checkpoint(&self) -> PreviousRun {
        let recent = self.recent.lock().unwrap_or_else(|e| e.into_inner());
        PreviousRun {
            shutdown_request: self
                .shutdown_request
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .or_else(crate::shutdown::requested),
            cleanup_stage: *self.cleanup_stage.lock().unwrap_or_else(|e| e.into_inner()),
            nvidia_software: self
                .nvidia_software
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            phase: *self.phase.lock().unwrap_or_else(|e| e.into_inner()),
            uptime_secs: self.started.elapsed().as_secs(),
            build_commit: self.context.build_commit.clone(),
            miner_version: Some(crate::version::RELEASE_VERSION.into()),
            recent_events: recent.0.clone(),
            events_truncated: recent.1,
        }
    }

    pub(crate) fn set_previous_run(&self, previous: Option<PreviousRun>) {
        *self.previous_run.lock().unwrap_or_else(|e| e.into_inner()) = previous;
    }

    pub(crate) fn set_previous_unclean(&self, value: bool) {
        self.previous_unclean.store(value, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self, final_report: bool) -> Diagnostics {
        let mut result = self.context.clone();
        result.reason = if !final_report {
            "periodic"
        } else if self.failed.load(Ordering::Relaxed) {
            "failure"
        } else {
            "shutdown"
        }
        .into();
        let checkpoint = self.checkpoint();
        result.phase = Some(checkpoint.phase);
        result.shutdown_request = checkpoint.shutdown_request;
        result.cleanup_stage = checkpoint.cleanup_stage;
        result.nvidia_software = checkpoint.nvidia_software;
        result.previous_run_unclean = self.previous_unclean.swap(false, Ordering::Relaxed);
        result.previous_run = self
            .previous_run
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        result.dropped_events = self.dropped.swap(0, Ordering::Relaxed);
        if let Ok(mut events) = self.events.lock() {
            result.events = std::mem::take(&mut *events).into_values().collect();
        }
        result.uptime_secs = self.started.elapsed().as_secs();
        result.update = take_update_status();
        if let Some(pool) = result.pool.as_mut() {
            take_pool_counts(pool, result.uptime_secs);
        }
        result.amd_detection = AMD_DETECTION
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .filter(seine_telemetry::AmdDetection::is_valid);
        result.unsupported_work_paused_secs = take_unsupported_pause(result.uptime_secs);
        if let Some(startup) = result.backend_startup.as_mut() {
            // Whole-run context: every report repeats it, so a lost report loses nothing.
            let latest = result.uptime_secs.saturating_mul(1000).saturating_add(999);
            startup.first_hashes = self
                .first_hashes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .filter(|(instance, _)| **instance <= 65536)
                .take(128)
                .map(
                    |(instance, (backend, ms))| seine_telemetry::BackendFirstHash {
                        backend: backend.clone(),
                        instance: *instance,
                        first_hash_ms: (*ms).min(latest),
                    },
                )
                .collect();
        }
        result
    }
}

/// Only the holder of an OS file lock owns the marker/spool. Concurrent processes
/// sharing a data directory must not label each other as crashed or delete state.
pub(crate) struct Store {
    _lock: File,
    finished: Mutex<bool>,
    marker: PathBuf,
    pending: PathBuf,
    attempt: PathBuf,
    checkpoint_path: PathBuf,
    pub(crate) previous_run: Option<PreviousRun>,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    installation_id: String,
    marker_modified_ns: u64,
    run: PreviousRun,
}

fn marker_stamp(path: &Path) -> Option<u64> {
    fs::metadata(path)
        .ok()?
        .modified()
        .ok()?
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()?
        .as_nanos()
        .try_into()
        .ok()
}

impl Store {
    pub(crate) fn open(settings: &Path, installation_id: &str) -> Option<(Self, bool)> {
        let dir = settings.parent()?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("seine.diagnostics.lock"))
            .ok()?;
        lock.try_lock().ok()?;
        let marker = dir.join("seine.diagnostics.running");
        let previous_unclean =
            fresh(&marker) && fs::read_to_string(&marker).ok().as_deref() == Some(installation_id);
        let checkpoint_path = dir.join("seine.diagnostics.phase.json");
        let previous_run = if previous_unclean && fresh(&checkpoint_path) {
            fs::metadata(&checkpoint_path)
                .ok()
                .filter(|m| m.len() <= 8192)
                .and_then(|_| fs::read(&checkpoint_path).ok())
                .and_then(|bytes| serde_json::from_slice::<Checkpoint>(&bytes).ok())
                .filter(|c| {
                    c.installation_id == installation_id
                        && Some(c.marker_modified_ns) == marker_stamp(&marker)
                        && c.run.validate()
                })
                .map(|c| c.run)
        } else {
            None
        };
        let _ = fs::remove_file(&checkpoint_path);
        fs::write(&marker, installation_id).ok()?;
        let _ = fs::remove_file(dir.join("seine.diagnostics.pending.tmp"));
        Some((
            Self {
                _lock: lock,
                finished: Mutex::new(false),
                marker,
                checkpoint_path,
                previous_run,
                pending: dir.join("seine.diagnostics.pending.json"),
                attempt: dir.join("seine.diagnostics.attempt"),
            },
            previous_unclean,
        ))
    }

    pub(crate) fn pending(&self, installation_id: &str) -> Option<Report> {
        let report = if fresh(&self.pending) {
            fs::metadata(&self.pending)
                .ok()
                .filter(|m| m.len() <= MAX_BODY_BYTES as u64)
                .and_then(|_| fs::read(&self.pending).ok())
                .and_then(|bytes| serde_json::from_slice::<Report>(&bytes).ok())
                .filter(|r| r.installation_id == installation_id && r.validate())
        } else {
            None
        };
        if report.is_none() {
            self.clear_pending();
        }
        report
    }

    pub(crate) fn save(&self, report: &Report) {
        if !report.validate() {
            return;
        }
        if let Ok(bytes) = serde_json::to_vec(report) {
            if bytes.len() <= MAX_BODY_BYTES {
                let temporary = self.pending.with_extension("tmp");
                if fs::write(&temporary, bytes).is_ok() {
                    let _ = fs::rename(temporary, &self.pending);
                }
            }
        }
    }
    pub(crate) fn clear_pending(&self) {
        let _ = fs::remove_file(&self.pending);
    }
    pub(crate) fn admit_upload(&self, installation_id: &str) -> bool {
        let age = fs::metadata(&self.attempt)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|time| SystemTime::now().duration_since(time).ok());
        if age.is_some_and(|age| age < Duration::from_secs(300))
            && fs::read_to_string(&self.attempt).ok().as_deref() == Some(installation_id)
        {
            return false;
        }
        fs::write(&self.attempt, installation_id).is_ok()
    }
    // Production callers hold `finished` across the complete checkpoint write.
    fn heartbeat(&self, installation_id: &str) {
        let _ = fs::write(&self.marker, installation_id);
    }
    pub(crate) fn checkpoint(&self, installation_id: &str, run: PreviousRun) {
        let finished = self.finished.lock().unwrap_or_else(|e| e.into_inner());
        if *finished {
            return;
        }
        if !run.validate() {
            return;
        }
        // Tie the sidecar to this exact marker write. A legacy miner can update
        // the plain marker without updating the sidecar; never attribute that
        // older checkpoint to its later run.
        self.heartbeat(installation_id);
        let Some(marker_modified_ns) = marker_stamp(&self.marker) else {
            return;
        };
        let checkpoint = Checkpoint {
            installation_id: installation_id.into(),
            marker_modified_ns,
            run,
        };
        let temp = self.checkpoint_path.with_extension("tmp");
        if let Ok(bytes) = serde_json::to_vec(&checkpoint) {
            if bytes.len() <= 8192 && fs::write(&temp, bytes).is_ok() {
                let _ = fs::rename(&temp, &self.checkpoint_path);
            }
        }
    }
    pub(crate) fn finish(&self) {
        // Completion may arrive from the mining thread while the uploader is
        // checkpointing. No later checkpoint may recreate this run's marker.
        let mut finished = self.finished.lock().unwrap_or_else(|e| e.into_inner());
        *finished = true;
        let _ = fs::remove_file(&self.checkpoint_path);
        let _ = fs::remove_file(self.checkpoint_path.with_extension("tmp"));
        let _ = fs::remove_file(&self.marker);
    }
}

fn fresh(path: &Path) -> bool {
    fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_some_and(|age| age <= Duration::from_secs(86400))
}

pub(crate) fn clear_disabled(settings: &Path) {
    let Some(dir) = settings.parent() else {
        return;
    };
    // Do not interfere with another live writer; it rechecks opt-out before upload.
    let Ok(lock) = OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join("seine.diagnostics.lock"))
    else {
        return;
    };
    if lock.try_lock().is_ok() {
        let _ = fs::remove_file(dir.join("seine.diagnostics.running"));
        let _ = fs::remove_file(dir.join("seine.diagnostics.phase.json"));
        let _ = fs::remove_file(dir.join("seine.diagnostics.phase.tmp"));
        let _ = fs::remove_file(dir.join("seine.diagnostics.pending.json"));
        let _ = fs::remove_file(dir.join("seine.diagnostics.pending.tmp"));
        let _ = fs::remove_file(dir.join("seine.diagnostics.attempt"));
    }
}

#[cfg(test)]
pub(crate) fn test_context() -> Diagnostics {
    Diagnostics {
        shutdown_request: None,
        cleanup_stage: None,
        nvidia_software: None,
        phase: None,
        previous_run: None,
        build_commit: "abcdef123456".into(),
        source_fingerprint: "1234abcd".into(),
        mode: "pool".into(),
        requested_backends: vec!["cpu".into()],
        requested_cpu_threads: 2,
        cpu_page_mode: "auto".into(),
        cpu_affinity: "auto".into(),
        cpu_autotune: true,
        uptime_secs: 0,
        reason: "periodic".into(),
        previous_run_unclean: false,
        dropped_events: 0,
        dropped_backend_samples: 0,
        events: vec![],
        update: None,
        pool: None,
        amd_detection: None,
        unsupported_work_paused_secs: None,
        backend_startup: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_hash_keeps_the_earliest_time_per_instance_and_leaves_gaps_visible() {
        let _lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mut context = test_context();
        context.backend_startup = Some(seine_telemetry::BackendStartup {
            expected_cpu: 1,
            expected_amd: 2,
            ..Default::default()
        });
        let guard = install(context);
        guard
            .0
            .first_hashes
            .lock()
            .unwrap()
            .insert(1, ("cpu".into(), 250));
        backend_first_hash("cpu", 1);
        backend_first_hash("amd", 2);
        backend_first_hash("amd", 2);
        let startup = guard.0.snapshot(false).backend_startup.unwrap();
        // The second AMD card never hashed, so only the expected count shows it.
        assert_eq!(startup.expected_amd, 2);
        assert_eq!(startup.first_hashes.len(), 2);
        assert_eq!(startup.first_hashes[0].backend, "cpu");
        assert_eq!(startup.first_hashes[0].first_hash_ms, 250);
        assert_eq!(startup.first_hashes[1].backend, "amd");
        // Reports repeat the record for the whole run.
        assert_eq!(guard.0.snapshot(false).backend_startup.unwrap(), startup);
    }

    #[test]
    fn schema14_counters_cover_one_window_and_never_invalidate_a_report() {
        let _lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mut context = test_context();
        context.pool = Some(seine_telemetry::PoolStatus {
            tls: true,
            backup_pools: 1,
            ..Default::default()
        });
        let recorder = Recorder {
            context,
            ..recorder()
        };
        // Clear what other tests left in the process-wide counters.
        let _ = recorder.snapshot(false);

        update_checker_started(true);
        update_checked(UpdateCheck::Api);
        update_checked(UpdateCheck::Web);
        update_checked(UpdateCheck::Failed);
        update_install_failed(seine_telemetry::UpdateFailure::Signature);
        update_installed();
        pool_failover();
        pool_reconnect();
        pool_reconnect();
        pool_refill();
        pool_on_backup(true);
        unsupported_work(true);

        let first = recorder.snapshot(false);
        let update = first.update.as_ref().unwrap();
        assert!(update.auto_install);
        assert_eq!(
            (update.checks_api, update.checks_web, update.checks_failed),
            (1, 1, 1)
        );
        assert_eq!((update.installs, update.install_failures), (1, 1));
        assert_eq!(
            update.last_install_failure,
            Some(seine_telemetry::UpdateFailure::Signature)
        );
        let pool = first.pool.as_ref().unwrap();
        assert!(pool.tls);
        assert_eq!(pool.backup_pools, 1);
        assert_eq!((pool.failovers, pool.reconnects, pool.refills), (1, 2, 1));
        assert!(first.unsupported_work_paused_secs.is_some());

        // Counters reset each window, while a pause in progress keeps reporting.
        let second = recorder.snapshot(false);
        let update = second.update.as_ref().unwrap();
        assert_eq!((update.checks_api, update.install_failures), (0, 0));
        assert_eq!(update.last_install_failure, None);
        assert_eq!(second.pool.as_ref().unwrap().reconnects, 0);
        assert!(second.unsupported_work_paused_secs.is_some());

        // An ended pause reports once more, then the field goes away.
        pool_on_backup(false);
        unsupported_work(false);
        assert!(recorder
            .snapshot(false)
            .unsupported_work_paused_secs
            .is_some());
        assert!(recorder
            .snapshot(false)
            .unsupported_work_paused_secs
            .is_none());
        UPDATE_ACTIVE.store(false, Ordering::Relaxed);
    }

    #[test]
    fn invalid_amd_detection_is_dropped_instead_of_failing_the_report() {
        let _lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let recorder = recorder();
        set_amd_detection(seine_telemetry::AmdDetection {
            found: 1,
            selected: 2,
            ..Default::default()
        });
        assert!(recorder.snapshot(false).amd_detection.is_none());
        let valid = seine_telemetry::AmdDetection {
            hip_unavailable: true,
            pci_devices: Some(1),
            ..Default::default()
        };
        set_amd_detection(valid.clone());
        assert_eq!(recorder.snapshot(false).amd_detection, Some(valid));
        *AMD_DETECTION.lock().unwrap() = None;
    }

    fn recorder() -> Recorder {
        Recorder {
            started: Instant::now(),
            context: test_context(),
            events: Mutex::new(BTreeMap::new()),
            recent: Mutex::new((Vec::new(), false)),
            dropped: AtomicU64::new(0),
            failed: AtomicBool::new(false),
            previous_unclean: AtomicBool::new(false),
            phase: Mutex::new(RunPhase::Starting),
            shutdown_request: Mutex::new(None),
            cleanup_stage: Mutex::new(None),
            nvidia_software: Mutex::new(None),
            checkpoint_store: Mutex::new(None),
            previous_run: Mutex::new(None),
            first_hashes: Mutex::new(BTreeMap::new()),
        }
    }

    #[test]
    fn stop_origins_survive_early_requests_and_do_not_leak_between_sessions() {
        use seine_telemetry::ShutdownRequest::*;
        let first = Arc::new(recorder());
        let origin = StopOrigin::default();
        origin.request(ApiRestart); // API stop arrived before worker startup.
        origin.attach(&first);
        origin.request(ApiStop); // Preserve the first request.
        assert_eq!(first.checkpoint().shutdown_request, Some(ApiRestart));
        assert_eq!(first.snapshot(true).shutdown_request, Some(ApiRestart));
        let second = Arc::new(recorder());
        let next = StopOrigin::default();
        next.attach(&second);
        assert_eq!(second.checkpoint().shutdown_request, None);
        next.request(TuiThreadChange);
        assert_eq!(second.checkpoint().shutdown_request, Some(TuiThreadChange));
        origin.request(ApiStop);
        assert_eq!(second.checkpoint().shutdown_request, Some(TuiThreadChange));
    }

    #[test]
    fn notify_panic_source_keeps_only_the_allowlisted_dependency_suffix() {
        assert_eq!(
            normalize_source(
                "/private/home/.cargo/registry/src/index/tokio-1.49.0/src/sync/notify.rs"
            ),
            "deps/tokio-1.49.0/src/sync/notify.rs"
        );
        assert_eq!(
            normalize_source(r"C:\private\cargo\tokio-1.49.0\src\sync\notify.rs"),
            "deps/tokio-1.49.0/src/sync/notify.rs"
        );
        for source in [
            "/private/tokio-1.49.0/src/sync/private.rs",
            "/private/tokio-1.49.1/src/sync/notify.rs",
            "/private/my-tokio-1.49.0/src/sync/notify.rs",
        ] {
            assert_eq!(normalize_source(source), "unknown");
        }
    }

    #[test]
    fn dependency_sources_strip_only_fixed_allowlisted_locations() {
        for source in [
            "/home/private/.cargo/registry/src/index.example/cudarc-0.19.2/src/lib.rs",
            r"C:\Users\private\.cargo\registry\src\index.example\cudarc-0.19.2\src\lib.rs",
        ] {
            assert_eq!(normalize_source(source), "deps/cudarc-0.19.2/src/lib.rs");
        }
        for source in [
            "/home/private/src/lib.rs",
            "private-cudarc-0.19.2/src/lib.rs",
            "/home/private/cudarc-0.19.2/src/private.rs",
            "deps/cudarc-0.19.3/src/lib.rs",
        ] {
            assert_eq!(normalize_source(source), "unknown");
        }
    }

    #[test]
    fn checkpoints_keep_failures_across_uploads_and_warning_floods() {
        let recorder = recorder();
        recorder.record(
            DiagnosticCode::RustPanic,
            DiagnosticKind::General,
            None,
            "/home/private/.cargo/registry/src/index/cudarc-0.19.2/src/lib.rs",
            181,
        );
        assert_eq!(recorder.snapshot(false).events.len(), 1);
        assert!(recorder.snapshot(false).events.is_empty());
        for line in 0..40 {
            recorder.record(
                DiagnosticCode::Warning,
                DiagnosticKind::Connection,
                None,
                "src/pool.rs",
                line,
            );
        }
        let previous = recorder.checkpoint();
        assert!(previous.validate());
        assert_eq!(previous.recent_events.len(), 8);
        assert!(previous.events_truncated);
        assert!(previous
            .recent_events
            .iter()
            .any(|e| e.code == DiagnosticCode::RustPanic));
        assert!(!serde_json::to_string(&previous)
            .unwrap()
            .contains("private"));
        recorder.record(
            DiagnosticCode::RustPanic,
            DiagnosticKind::General,
            None,
            "/home/private/.cargo/registry/src/index/cudarc-0.19.2/src/lib.rs",
            181,
        );
        assert_eq!(
            recorder
                .checkpoint()
                .recent_events
                .iter()
                .find(|e| e.code == DiagnosticCode::RustPanic)
                .unwrap()
                .count,
            2
        );
    }

    #[test]
    fn interrupted_run_recovers_diagnostics_and_clean_shutdown_clears_them() {
        let dir = std::env::temp_dir().join(format!(
            "seine-recovered-diagnostics-{}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        let settings = dir.join("settings.json");
        let id = "11111111111111111111111111111111";
        let recorder = recorder();
        recorder.record(
            DiagnosticCode::RustPanic,
            DiagnosticKind::General,
            None,
            "deps/cudarc-0.19.2/src/lib.rs",
            181,
        );
        let (store, _) = Store::open(&settings, id).unwrap();
        let mut checkpoint = recorder.checkpoint();
        checkpoint.shutdown_request = Some(seine_telemetry::ShutdownRequest::ConsoleClose);
        checkpoint.cleanup_stage = Some(seine_telemetry::CleanupStage::StoppingBackends);
        checkpoint.nvidia_software = Some(seine_telemetry::NvidiaSoftware {
            driver_cuda_version: Some(12080),
            nvrtc_version: Some(12090),
        });
        store.checkpoint(id, checkpoint);
        drop(store);
        let (store, unclean) = Store::open(&settings, id).unwrap();
        assert!(unclean);
        let previous = store.previous_run.as_ref().unwrap();
        assert_eq!(
            previous.shutdown_request,
            Some(seine_telemetry::ShutdownRequest::ConsoleClose)
        );
        assert_eq!(
            previous.miner_version.as_deref(),
            Some(crate::version::RELEASE_VERSION)
        );
        assert_eq!(previous.recent_events[0].code, DiagnosticCode::RustPanic);
        assert_eq!(
            previous.cleanup_stage,
            Some(seine_telemetry::CleanupStage::StoppingBackends)
        );
        assert_eq!(
            previous
                .nvidia_software
                .as_ref()
                .unwrap()
                .driver_cuda_version,
            Some(12080)
        );
        store.finish();
        drop(store);
        let (store, unclean) = Store::open(&settings, id).unwrap();
        assert!(!unclean);
        assert!(store.previous_run.is_none());
        store.finish();
        drop(store);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn structured_causes_separate_errors_at_one_site_and_normalize_windows_sources() {
        let recorder = recorder();
        *recorder.phase.lock().unwrap() = RunPhase::InitializingBackends;
        for errno in [12, 13, 12] {
            recorder.record_detail(
                DiagnosticCode::Warning,
                DiagnosticKind::Memory,
                Some("cpu"),
                r"src\backend\cpu\kernel.rs",
                10,
                Some(FailureDetail {
                    reason: FailureReason::HugePagesUnavailable,
                    operation: DiagnosticOperation::CpuAllocation,
                    requested_bytes: Some(2147483648),
                    os_error: Some(errno),
                    ready_workers: None,
                    expected_workers: None,
                }),
            );
        }
        let snapshot = recorder.snapshot(false);
        assert_eq!(snapshot.events.len(), 2);
        assert_eq!(snapshot.events.iter().map(|e| e.count).sum::<u64>(), 3);
        assert!(snapshot
            .events
            .iter()
            .all(|e| e.source == "src/backend/cpu/kernel.rs"
                && e.phase == Some(RunPhase::InitializingBackends)));
        let mut report: Report =
            serde_json::from_str(include_str!("../telemetry/fixture.json")).unwrap();
        report.schema_version = seine_telemetry::SCHEMA_VERSION;
        report.diagnostics = Some(snapshot);
        assert!(report.validate());
        let json = serde_json::to_vec(&report).unwrap();
        assert!(serde_json::from_slice::<Report>(&json).unwrap().validate());
        recorder.record(
            DiagnosticCode::Warning,
            DiagnosticKind::General,
            None,
            r"C:\Users\alice\src\private.rs",
            1,
        );
        assert_eq!(recorder.snapshot(false).events[0].source, "unknown");
    }

    #[test]
    fn specific_cause_displaces_generic_warning_at_capacity() {
        let recorder = recorder();
        for line in 0..32 {
            recorder.record(
                DiagnosticCode::Warning,
                DiagnosticKind::General,
                None,
                "src/miner/mod.rs",
                line,
            );
        }
        recorder.record_detail(
            DiagnosticCode::Warning,
            DiagnosticKind::Memory,
            Some("cpu"),
            "src/backend/cpu/kernel.rs",
            100,
            Some(FailureDetail::new(
                FailureReason::HugePagesUnavailable,
                DiagnosticOperation::CpuAllocation,
            )),
        );
        let snapshot = recorder.snapshot(false);
        assert_eq!(snapshot.events.len(), 32);
        assert_eq!(snapshot.dropped_events, 1);
        assert!(snapshot.events.iter().any(|e| e.detail.is_some()));
    }

    #[test]
    fn io_details_preserve_codes_without_error_text() {
        for (kind, reason) in [
            (
                std::io::ErrorKind::ConnectionRefused,
                FailureReason::ConnectionRefused,
            ),
            (
                std::io::ErrorKind::TimedOut,
                FailureReason::ConnectionTimedOut,
            ),
            (
                std::io::ErrorKind::BrokenPipe,
                FailureReason::ConnectionReset,
            ),
        ] {
            let error = std::io::Error::new(kind, "PRIVATE https://user:password@host/wallet");
            let detail = io_detail(DiagnosticOperation::PoolConnect, &error);
            assert_eq!(detail.reason, reason);
            assert!(!serde_json::to_string(&detail).unwrap().contains("PRIVATE"));
        }
        let detail = io_detail(
            DiagnosticOperation::PoolResolve,
            &std::io::Error::from_raw_os_error(13),
        );
        assert_eq!(detail.reason, FailureReason::DnsFailed);
        assert_eq!(detail.os_error, Some(13));
    }

    #[test]
    fn bounded_counters_deduplicate_and_never_retain_messages_or_local_paths() {
        let recorder = recorder();
        let secret = "driver failure for wallet SECRET at /home/alice/private/config";
        for _ in 0..100 {
            recorder.record(
                DiagnosticCode::Warning,
                classify(secret),
                Some("nvidia"),
                "src/miner/mod.rs",
                10,
            );
        }
        recorder.record(
            DiagnosticCode::Error,
            classify(secret),
            Some(secret),
            "/home/alice/private.rs",
            20,
        );
        let result = recorder.snapshot(false);
        assert_eq!(result.events.len(), 2);
        assert_eq!(result.events[0].count, 100);
        let json = serde_json::to_string(&result).unwrap();
        assert!(!json.contains("SECRET") && !json.contains("alice") && !json.contains("private"));
        assert!(result
            .events
            .iter()
            .all(|e| e.kind == DiagnosticKind::Driver));
        assert!(recorder.snapshot(false).events.is_empty());
        for line in 1..=40 {
            recorder.record(
                DiagnosticCode::Warning,
                DiagnosticKind::General,
                None,
                "src/miner/mod.rs",
                line,
            );
        }
        let result = recorder.snapshot(false);
        assert_eq!(result.events.len(), 32);
        assert_eq!(result.dropped_events, 8);
        for line in 1..=32 {
            recorder.record(
                DiagnosticCode::Warning,
                DiagnosticKind::General,
                None,
                "src/miner/mod.rs",
                line,
            );
        }
        recorder.record(
            DiagnosticCode::RunFailed,
            DiagnosticKind::General,
            None,
            "src/miner/mod.rs",
            99,
        );
        let result = recorder.snapshot(true);
        assert_eq!(result.events.len(), 32);
        assert!(result
            .events
            .iter()
            .any(|e| e.code == DiagnosticCode::RunFailed));
        assert_eq!(result.dropped_events, 1);
    }

    #[test]
    fn failures_and_unclean_markers_survive_short_sessions() {
        let recorder = recorder();
        recorder.set_previous_unclean(true);
        recorder.record(
            DiagnosticCode::RunFailed,
            DiagnosticKind::Connection,
            None,
            "src/miner/mod.rs",
            1,
        );
        let snapshot = recorder.snapshot(true);
        assert_eq!(snapshot.reason, "failure");
        assert!(snapshot.previous_run_unclean);
        assert!(!recorder.snapshot(true).previous_run_unclean);
        let mut report: Report =
            serde_json::from_str(include_str!("../telemetry/fixture.json")).unwrap();
        report.schema_version = seine_telemetry::SCHEMA_VERSION;
        report.window_secs = 1;
        report.backends.clear();
        report.diagnostics = Some(snapshot);
        assert!(report.validate());
    }

    #[test]
    fn persistence_is_exclusive_identity_bound_and_replays_exact_report_id() {
        struct Temp(PathBuf);
        impl Drop for Temp {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let dir = Temp(std::env::temp_dir().join(format!(
                "seine-diagnostics-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )));
        fs::create_dir(&dir.0).unwrap();
        let settings = dir.0.join("seine.telemetry.json");
        let report: Report =
            serde_json::from_str(include_str!("../telemetry/fixture.json")).unwrap();
        let (store, unclean) = Store::open(&settings, &report.installation_id).unwrap();
        assert!(!unclean);
        assert!(Store::open(&settings, &report.installation_id).is_none());
        assert!(store.admit_upload(&report.installation_id));
        assert!(!store.admit_upload(&report.installation_id));
        let marker = File::options().write(true).open(&store.marker).unwrap();
        marker
            .set_modified(SystemTime::now() - Duration::from_secs(86401))
            .unwrap();
        assert!(!fresh(&store.marker));
        store.heartbeat(&report.installation_id);
        assert!(fresh(&store.marker));
        store.save(&report);
        assert_eq!(
            store.pending(&report.installation_id).unwrap().report_id,
            report.report_id
        );
        store.checkpoint(
            &report.installation_id,
            PreviousRun {
                shutdown_request: None,
                cleanup_stage: None,
                nvidia_software: None,
                phase: RunPhase::Tuning,
                uptime_secs: 42,
                build_commit: "abcdef".into(),
                miner_version: None,
                recent_events: Vec::new(),
                events_truncated: false,
            },
        );
        drop(store); // Simulated abnormal exit: no finish().
        let (store, unclean) = Store::open(&settings, &report.installation_id).unwrap();
        assert!(unclean);
        let previous = store.previous_run.as_ref().unwrap();
        assert_eq!(previous.phase, RunPhase::Tuning);
        assert_eq!(previous.uptime_secs, 42);
        assert_eq!(previous.build_commit, "abcdef");
        assert!(store.pending("changed-destination-id").is_none());
        assert!(!store.pending.exists());
        store.finish();
        drop(store);
        let (store, unclean) = Store::open(&settings, &report.installation_id).unwrap();
        assert!(!unclean);
        assert!(store.previous_run.is_none());
        store.save(&report);
        let file = File::options().write(true).open(&store.pending).unwrap();
        file.set_modified(SystemTime::now() - Duration::from_secs(86401))
            .unwrap();
        assert!(store.pending(&report.installation_id).is_none());
        store.finish();
    }
    #[test]
    fn checkpoint_requires_matching_fresh_marker_and_bounded_valid_context() {
        let dir = std::env::temp_dir().join(format!("seine-checkpoint-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let settings = dir.join("settings.json");
        let id = "11111111111111111111111111111111";
        for content in [
            r#"{"installation_id":"other","run":{"phase":"mining","uptime_secs":5,"build_commit":"abcdef"}}"#.to_string(),
            "PRIVATE".repeat(300),
            r#"{"installation_id":"11111111111111111111111111111111","run":{"phase":"mining","uptime_secs":5,"build_commit":"PRIVATE"}}"#.to_string(),
        ] {
            fs::write(dir.join("seine.diagnostics.running"), id).unwrap();
            fs::write(dir.join("seine.diagnostics.phase.json"), content).unwrap();
            let (store, unclean) = Store::open(&settings, id).unwrap();
            assert!(unclean);
            assert!(store.previous_run.is_none());
            store.finish();
        }
        let (store, _) = Store::open(&settings, id).unwrap();
        store.checkpoint(
            id,
            PreviousRun {
                shutdown_request: None,
                cleanup_stage: None,
                nvidia_software: None,
                phase: RunPhase::Mining,
                uptime_secs: 90,
                build_commit: "abcdef".into(),
                miner_version: None,
                recent_events: Vec::new(),
                events_truncated: false,
            },
        );
        drop(store);
        // Simulate a legacy writer starting another run with no phase sidecar.
        let file = File::options()
            .write(true)
            .open(dir.join("seine.diagnostics.running"))
            .unwrap();
        file.set_modified(SystemTime::now() - Duration::from_secs(1))
            .unwrap();
        let (store, unclean) = Store::open(&settings, id).unwrap();
        assert!(unclean);
        assert!(store.previous_run.is_none());
        store.checkpoint(
            id,
            PreviousRun {
                shutdown_request: None,
                cleanup_stage: None,
                nvidia_software: None,
                phase: RunPhase::Mining,
                uptime_secs: 90,
                build_commit: "abcdef".into(),
                miner_version: None,
                recent_events: Vec::new(),
                events_truncated: false,
            },
        );
        drop(store);
        let file = File::options()
            .write(true)
            .open(dir.join("seine.diagnostics.running"))
            .unwrap();
        file.set_modified(SystemTime::now() - Duration::from_secs(86401))
            .unwrap();
        let (store, unclean) = Store::open(&settings, id).unwrap();
        assert!(!unclean);
        assert!(store.previous_run.is_none());
        store.finish();
        drop(store);
        fs::remove_dir_all(dir).unwrap();
    }
}
