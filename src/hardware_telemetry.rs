//! Optional, best-effort hardware reporting. No wallet/config/log serialization.
use anyhow::{bail, Context, Result};
use crossbeam_channel::{bounded, Sender};
use seine_telemetry::tuning::{
    RecordedTrial, TrialOutcome, TuningReport, MAX_TRIALS, MAX_TUNING_BYTES,
};
use seine_telemetry::{
    BackendSample, BackendSettings, Hardware, Report, Resources, SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

const INTERVAL: Duration = Duration::from_secs(3600);
const SETTINGS_FILE: &str = "seine.telemetry.json";

#[derive(Clone, Debug)]
pub(crate) struct Options {
    pub endpoint: String,
    pub installation_id: String,
    pub state_path: PathBuf,
    /// Explicit `--threads` value (schema 9); filled in by `start` from the config.
    pub requested_threads: Option<u32>,
    /// Whether automatic worker-count tuning is active (schema 9).
    pub cpu_autotune_threads: Option<bool>,
}

#[derive(Serialize, Deserialize)]
#[serde(default)]
struct Preferences {
    enabled: bool,
    endpoint: Option<String>,
    installation_id: Option<String>,
}

impl Default for Preferences {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: None,
            installation_id: None,
        }
    }
}

fn random_id() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes).map_err(|e| anyhow::anyhow!("random ID: {e}"))?;
    Ok(hex::encode(bytes))
}

pub(crate) fn resolve(
    data_dir: &Path,
    choice: Option<bool>,
    endpoint: Option<&str>,
) -> Result<Option<Options>> {
    resolve_with_default(
        data_dir,
        choice,
        endpoint,
        option_env!("SEINE_TELEMETRY_URL"),
    )
}

fn resolve_with_default(
    data_dir: &Path,
    choice: Option<bool>,
    endpoint: Option<&str>,
    default_endpoint: Option<&str>,
) -> Result<Option<Options>> {
    let path = data_dir.join(SETTINGS_FILE);
    let mut preferences: Preferences = match fs::read(&path) {
        Ok(bytes) => match serde_json::from_slice(&bytes) {
            Ok(c) => c,
            Err(_) if choice == Some(false) => Preferences::default(),
            Err(e) => return Err(e).context("invalid telemetry settings"),
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => Preferences::default(),
        Err(e) => return Err(e.into()),
    };
    let mut changed = false;
    let endpoint = endpoint.or_else(|| {
        if preferences.endpoint.is_none() {
            default_endpoint.filter(|v| !v.is_empty())
        } else {
            None
        }
    });
    if let Some(enabled) = choice {
        preferences.enabled = enabled;
        changed = true;
    }
    if let Some(endpoint) = endpoint {
        validate_endpoint(endpoint)?;
        if preferences.endpoint.as_deref() != Some(endpoint) {
            preferences.installation_id = None;
        }
        preferences.endpoint = Some(endpoint.to_owned());
        changed = true;
    }
    if choice == Some(true) && preferences.endpoint.is_none() {
        bail!("--telemetry on requires --telemetry-url https://your-collector/v1/reports on first use");
    }
    if preferences.enabled
        && preferences.endpoint.is_some()
        && preferences
            .installation_id
            .as_deref()
            .is_none_or(|id| !seine_telemetry::valid_id(id))
    {
        preferences.installation_id = Some(random_id()?);
        changed = true;
    }
    if changed {
        fs::create_dir_all(data_dir)?;
        let temporary = path.with_extension("json.tmp");
        fs::write(&temporary, serde_json::to_vec_pretty(&preferences)?)?;
        fs::rename(temporary, &path)?;
    }
    if !preferences.enabled || preferences.endpoint.is_none() {
        if !preferences.enabled {
            crate::diagnostics::clear_disabled(&path);
        }
        return Ok(None);
    }
    let endpoint = preferences.endpoint.unwrap();
    validate_endpoint(&endpoint)?;
    Ok(Some(Options {
        endpoint,
        installation_id: preferences.installation_id.unwrap(),
        state_path: path,
        requested_threads: None,
        cpu_autotune_threads: None,
    }))
}

fn validate_endpoint(value: &str) -> Result<()> {
    let url = reqwest::Url::parse(value)?;
    let loopback = matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"));
    if (url.scheme() != "https" && !(loopback && url.scheme() == "http"))
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("telemetry URL must use HTTPS without credentials, query or fragment (HTTP permitted only on loopback)");
    }
    Ok(())
}

fn still_enabled(options: &Options) -> bool {
    fs::read(&options.state_path)
        .ok()
        .and_then(|v| serde_json::from_slice::<Preferences>(&v).ok())
        .is_some_and(|c| {
            c.enabled
                && c.endpoint.as_deref() == Some(&options.endpoint)
                && c.installation_id.as_deref() == Some(&options.installation_id)
        })
}

#[derive(Clone)]
pub(crate) struct Recorder(
    Arc<Mutex<BTreeMap<u64, Accumulated>>>,
    Arc<AtomicBool>,
    Arc<TrialRecorder>,
);

struct TrialRecorder {
    active: AtomicBool,
    run_id: String,
    started: Instant,
    queue: Mutex<(u64, Vec<RecordedTrial>)>,
    dropped: AtomicU64,
}

impl TrialRecorder {
    fn new(run_id: String) -> Self {
        Self {
            active: AtomicBool::new(true),
            run_id,
            started: Instant::now(),
            queue: Mutex::new((0, Vec::new())),
            dropped: AtomicU64::new(0),
        }
    }
}

struct TrialWorkerLifetime(Arc<TrialRecorder>);

impl Drop for TrialWorkerLifetime {
    fn drop(&mut self) {
        self.0.active.store(false, Ordering::Release);
        self.0
            .queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .1
            .clear();
        self.0.dropped.store(0, Ordering::Relaxed);
    }
}

struct Accumulated {
    settings: BackendSettings,
    hashes: u64,
    first_hash_noted: bool,
    first: Instant,
    last: Instant,
    changed: bool,
}

impl Recorder {
    pub(crate) fn trials_enabled(&self) -> bool {
        self.2.active.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn test_disable_trials(&self) {
        self.2.active.store(false, Ordering::Release);
    }

    /// Common run-relative clock for the staged runtime tuning hooks.
    #[allow(dead_code)]
    pub(crate) fn trial_elapsed_ms(&self) -> u64 {
        self.2.started.elapsed().as_millis().min(u64::MAX as u128) as u64
    }

    /// Emit once per completed/aborted trial. Never blocks or serializes on the
    /// mining thread; FIFO overflow loses telemetry, never productive mining.
    #[allow(dead_code)]
    pub(crate) fn record_trial(&self, outcome: TrialOutcome) {
        if !self.trials_enabled() {
            return;
        }
        if !outcome.validate() {
            self.2.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let Ok(mut queue) = self.2.queue.try_lock() else {
            self.2.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if !self.trials_enabled() {
            return;
        }
        if queue.1.len() == MAX_TRIALS || queue.0 == i64::MAX as u64 {
            self.2.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        queue.0 += 1;
        let trial_id = queue.0;
        queue.1.push(RecordedTrial { trial_id, outcome });
    }

    fn drain_tuning(&self) -> Option<TuningReport> {
        let mut queue = self.2.queue.lock().unwrap_or_else(|e| e.into_inner());
        let mut report = TuningReport {
            run_id: self.2.run_id.clone(),
            trials: std::mem::take(&mut queue.1),
            dropped_trials: self.2.dropped.swap(0, Ordering::Relaxed),
        };
        drop(queue);
        // Only the reporting thread performs serialization/budget work. Keep
        // oldest records regardless of outcome, without cherry-picking wins.
        while serde_json::to_vec(&report).is_ok_and(|b| b.len() > MAX_TUNING_BYTES)
            && !report.trials.is_empty()
        {
            report.trials.pop();
            report.dropped_trials += 1;
        }
        (!report.trials.is_empty() || report.dropped_trials > 0).then_some(report)
    }

    #[cfg(test)]
    pub(crate) fn test_tuning_report(&self) -> Option<TuningReport> {
        self.drain_tuning()
    }

    pub(crate) fn record(&self, instance: u64, settings: BackendSettings, hashes: u64) {
        let backend = (hashes > 0).then(|| settings.backend.clone());
        // Noted outside the sample lock; a retry after contention finds it unset.
        let first_hash = self.record_sample(instance, settings, hashes);
        if let (true, Some(backend)) = (first_hash, backend) {
            crate::diagnostics::backend_first_hash(&backend, instance);
        }
    }

    /// Returns true when this is the instance's first sample with hashes.
    fn record_sample(&self, instance: u64, settings: BackendSettings, hashes: u64) -> bool {
        // The miner never waits for a report to finish; contention loses telemetry only.
        let Ok(mut samples) = self.0.try_lock() else {
            self.1.store(true, Ordering::Relaxed);
            return false;
        };
        if samples.len() >= 128 && !samples.contains_key(&instance) {
            self.1.store(true, Ordering::Relaxed);
            return false;
        }
        let now = Instant::now();
        let first_sample = !samples.contains_key(&instance);
        let sample = samples.entry(instance).or_insert_with(|| Accumulated {
            settings: settings.clone(),
            hashes: 0,
            first_hash_noted: false,
            first: now,
            last: now,
            changed: false,
        });
        sample.changed |= sample.settings != settings;
        sample.settings = settings;
        sample.last = now;
        // The first counter predates our observation window; do not count it.
        if !first_sample {
            sample.hashes = sample.hashes.saturating_add(hashes);
        }
        let first_hash = hashes > 0 && !sample.first_hash_noted;
        sample.first_hash_noted |= hashes > 0;
        first_hash
    }

    #[cfg(test)]
    pub(crate) fn test_recorder() -> Self {
        Self(
            Arc::new(Mutex::new(BTreeMap::new())),
            Arc::new(AtomicBool::new(false)),
            Arc::new(TrialRecorder::new(
                "11111111111111111111111111111111".into(),
            )),
        )
    }

    #[cfg(test)]
    pub(crate) fn test_advance_window(&self, duration: Duration) {
        for sample in self.0.lock().unwrap().values_mut() {
            sample.first -= duration;
        }
    }

    pub(crate) fn drain(&self) -> Vec<BackendSample> {
        let Ok(mut samples) = self.0.lock() else {
            return Vec::new();
        };
        let now = Instant::now();
        let missed = self.1.swap(false, Ordering::Relaxed);
        let result = samples
            .iter()
            .filter_map(|(id, s)| {
                let observed_secs = s.last.duration_since(s.first).as_secs_f64();
                (observed_secs >= 60.0).then(|| BackendSample {
                    instance: *id,
                    settings: s.settings.clone(),
                    hashes: s.hashes,
                    observed_secs,
                    settings_changed: s.changed || missed,
                })
            })
            .collect();
        samples.retain(|_, s| now.duration_since(s.last) < Duration::from_secs(60));
        for s in samples.values_mut() {
            s.hashes = 0;
            // Counters arriving next include work since the last poll.
            s.first = s.last;
            s.changed = false;
        }
        result
    }
}

#[derive(Default)]
enum RunCompletion {
    #[default]
    Starting,
    Running(std::sync::Weak<crate::diagnostics::Store>),
    Finished,
}

impl RunCompletion {
    fn attach(&mut self, store: &Arc<crate::diagnostics::Store>) {
        if matches!(self, Self::Finished) {
            // Mining can end before the background worker opens its store.
            store.finish();
        } else {
            // An uploader that exits after opt-out must release its file lock
            // even while the mining-side Worker remains alive.
            *self = Self::Running(Arc::downgrade(store));
        }
    }

    fn finish(&mut self) {
        if let Self::Running(store) = self {
            if let Some(store) = store.upgrade() {
                store.finish();
            }
        }
        *self = Self::Finished;
    }
}

pub(crate) struct Worker {
    stop: Sender<()>,
    thread: Option<thread::JoinHandle<()>>,
    completion: Arc<Mutex<RunCompletion>>,
    _diagnostics: crate::diagnostics::Guard,
}

impl Worker {
    pub(crate) fn attach_stop_origin(&self, origin: &crate::diagnostics::StopOrigin) {
        origin.attach(&self._diagnostics.0);
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Mining is finished even if a previous HTTP request is still blocked.
        // Keep spool/file-lock ownership on the uploader until delivery ends.
        self.completion
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .finish();
        let _ = self.stop.try_send(());
        // HTTP has a strict total timeout, so shutdown remains bounded.
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn hardware(options: &Options) -> Hardware {
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    sys.refresh_cpu_all();
    let identity = crate::runtime_identity::cpu_identity();
    let (effective_memory_gib, available_memory_gib) = crate::config::telemetry_memory_budget_gib();
    Hardware {
        resources: Some(Resources {
            allowed_logical_cpus: core_affinity::get_core_ids().map(|ids| ids.len() as u32),
            available_parallelism: thread::available_parallelism().ok().map(|v| v.get() as u32),
            effective_memory_gib,
            available_memory_gib,
            requested_threads: options.requested_threads,
            cpu_autotune_threads: options.cpu_autotune_threads,
        }),
        os: std::env::consts::OS.into(),
        os_version: sysinfo::System::os_version(),
        runtime_environment: crate::runtime_identity::runtime_environment(),
        cpu_model: sys
            .cpus()
            .first()
            .map(|c| c.brand().to_owned())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "unknown".into()),
        cpu_vendor: identity.vendor,
        cpu_family: identity.family,
        cpu_model_id: identity.model,
        logical_cpus: sys.cpus().len() as u32,
        physical_cores: sys.physical_core_count().map(|v| v as u32),
        numa_nodes: numa_node_count(),
        memory_gib: sys.total_memory().div_ceil(1024 * 1024 * 1024) as u32,
        memory_setup: crate::memory_modules::host_memory_setup(),
        supported_cpu_kernels: blocknet_pow_kernel::CpuKernel::ALL
            .into_iter()
            .filter(|k| k.is_supported())
            .map(|k| k.to_string())
            .collect(),
    }
}

fn http_client() -> reqwest::Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(3))
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

fn upload(client: &reqwest::blocking::Client, options: &Options, report: &Report) -> bool {
    report.validate()
        && still_enabled(options)
        && serde_json::to_vec(report)
            .is_ok_and(|body| body.len() <= seine_telemetry::MAX_BODY_BYTES)
        && client
            .post(&options.endpoint)
            .json(report)
            .send()
            .is_ok_and(|r| r.status() == reqwest::StatusCode::NO_CONTENT)
}

fn send_report(
    client: &reqwest::blocking::Client,
    options: &Options,
    report: &Report,
    store: Option<&crate::diagnostics::Store>,
) -> bool {
    still_enabled(options)
        && store.is_none_or(|s| s.admit_upload(&options.installation_id))
        && upload(client, options, report)
}

pub(crate) fn start(options: &Options, cfg: &crate::config::Config) -> Result<(Recorder, Worker)> {
    let options = Options {
        requested_threads: cfg
            .cpu_threads_explicit
            .map(|threads| u32::try_from(threads).unwrap_or(u32::MAX)),
        cpu_autotune_threads: Some(cfg.cpu_autotune_threads),
        ..options.clone()
    };
    start_with_context(&options, crate::diagnostics::context(cfg))
}

fn start_with_context(
    options: &Options,
    context: seine_telemetry::Diagnostics,
) -> Result<(Recorder, Worker)> {
    let guard = crate::diagnostics::install(context);
    let diagnostics = Arc::clone(&guard.0);
    let recorder = Recorder(
        Arc::new(Mutex::new(BTreeMap::new())),
        Arc::new(AtomicBool::new(false)),
        Arc::new(TrialRecorder::new(random_id()?)),
    );
    let samples = recorder.clone();
    let options = options.clone();
    let (stop, stopped) = bounded(1);
    let completion = Arc::new(Mutex::new(RunCompletion::default()));
    let worker_completion = Arc::clone(&completion);
    let thread = thread::Builder::new()
        .name("hardware-telemetry".into())
        .spawn(move || {
            let _trial_lifetime = TrialWorkerLifetime(Arc::clone(&samples.2));
            let Ok(client) = http_client() else {
                return;
            };
            if !still_enabled(&options) {
                return;
            }
            let store =
                crate::diagnostics::Store::open(&options.state_path, &options.installation_id).map(
                    |(store, unclean)| {
                        let store = Arc::new(store);
                        worker_completion
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .attach(&store);
                        diagnostics.set_previous_unclean(unclean);
                        diagnostics.set_previous_run(store.previous_run.clone());
                        diagnostics.attach_checkpoint_store(&store, &options.installation_id);
                        store
                    },
                );
            // One bounded replay on the next launch, using the same report ID for deduplication.
            if let Some(store) = &store {
                if let Some(pending) = store.pending(&options.installation_id) {
                    if send_report(&client, &options, &pending, Some(store)) {
                        store.clear_pending();
                    }
                }
            }
            let mut since = Instant::now();
            // Regular reports stay hourly; shutdown/failure wakes the worker immediately.
            let mut next_report = Instant::now() + Duration::from_secs(300);
            let mut next_checkpoint = Instant::now();
            let mut saved_shutdown = None;
            loop {
                let final_report = !matches!(
                    stopped.recv_timeout(
                        next_report
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_secs(1))
                    ),
                    Err(crossbeam_channel::RecvTimeoutError::Timeout)
                );
                if !still_enabled(&options) {
                    if let Some(store) = &store {
                        store.clear_pending();
                        store.finish();
                    }
                    break;
                }
                // Observe console close within one second while keeping ordinary
                // checkpoint disk writes at five seconds. No IO in the OS handler.
                if let Some(store) = &store {
                    if final_report {
                        // Mining has already released this worker. Do not leave a
                        // false unfinished marker while final HTTP delivery waits.
                        store.finish();
                    } else if Instant::now() >= next_checkpoint
                        || saved_shutdown != crate::shutdown::requested()
                    {
                        saved_shutdown = crate::shutdown::requested();
                        diagnostics.persist_checkpoint();
                        next_checkpoint = Instant::now() + Duration::from_secs(5);
                    }
                }
                if !final_report && Instant::now() < next_report {
                    continue;
                }
                if let Some(store) = store.as_ref().filter(|_| !final_report) {
                    if let Some(pending) = store.pending(&options.installation_id) {
                        if send_report(&client, &options, &pending, Some(store)) {
                            store.clear_pending();
                        }
                    }
                }
                let tuning = samples.drain_tuning();
                let diagnostic_snapshot = diagnostics.snapshot(final_report);
                let has_stop_context = diagnostic_snapshot.shutdown_request.is_some()
                    || diagnostic_snapshot.cleanup_stage.is_some();
                let has_diagnostics = !diagnostic_snapshot.events.is_empty()
                    || diagnostic_snapshot.previous_run_unclean
                    || diagnostic_snapshot.dropped_events > 0;
                let has_evidence = has_diagnostics || tuning.is_some() || has_stop_context;
                // Routine stop context must not evict an undelivered report from
                // the bounded spool. A replay may still be rate-limited or fail.
                let preserve_pending = final_report
                    && !has_diagnostics
                    && tuning.is_none()
                    && store
                        .as_ref()
                        .is_some_and(|s| s.pending(&options.installation_id).is_some());
                if final_report && (!has_evidence || preserve_pending) {
                    if let Some(store) = &store {
                        store.finish();
                    }
                    break;
                }
                let backends = samples.drain();
                let window_secs = backends
                    .iter()
                    .fold(since.elapsed().as_secs().max(1), |secs, sample| {
                        secs.max(sample.observed_secs.ceil() as u64)
                    });
                let mut report = Report {
                    schema_version: SCHEMA_VERSION,
                    installation_id: options.installation_id.clone(),
                    report_id: match random_id() {
                        Ok(id) => id,
                        Err(_) => break,
                    },
                    miner_version: crate::version::RELEASE_VERSION.into(),
                    build_target: crate::runtime_identity::build_target().into(),
                    build_features: {
                        let features = crate::runtime_identity::build_features();
                        if features.is_empty() {
                            "cpu-only".into()
                        } else {
                            features.into()
                        }
                    },
                    debug_build: cfg!(debug_assertions),
                    hardware: hardware(&options),
                    window_secs,
                    backends,
                    diagnostics: Some(diagnostic_snapshot),
                    tuning,
                };
                since = Instant::now();
                // Keep uploads within the collector limit even on unusually large rigs.
                while serde_json::to_vec(&report)
                    .is_ok_and(|b| b.len() > seine_telemetry::MAX_BODY_BYTES)
                    && !report.backends.is_empty()
                {
                    report.backends.pop();
                    if let Some(d) = &mut report.diagnostics {
                        d.dropped_backend_samples = d.dropped_backend_samples.saturating_add(1);
                    }
                }
                while serde_json::to_vec(&report)
                    .is_ok_and(|b| b.len() > seine_telemetry::MAX_BODY_BYTES)
                    && report.tuning.as_ref().is_some_and(|t| !t.trials.is_empty())
                {
                    let tuning = report.tuning.as_mut().unwrap();
                    tuning.trials.pop();
                    tuning.dropped_trials += 1;
                }
                if has_evidence {
                    if let Some(store) = &store {
                        store.save(&report);
                    }
                }
                if send_report(&client, &options, &report, store.as_deref()) && has_evidence {
                    if let Some(store) = &store {
                        store.clear_pending();
                    }
                }
                if !still_enabled(&options) {
                    if let Some(store) = &store {
                        store.clear_pending();
                    }
                }
                if final_report {
                    if let Some(store) = &store {
                        store.finish();
                    }
                    break;
                }
                next_report = Instant::now() + INTERVAL;
            }
        })?;
    Ok((
        recorder,
        Worker {
            stop,
            thread: Some(thread),
            completion,
            _diagnostics: guard,
        },
    ))
}

/// NUMA nodes on the host, independent of this process's cpuset.
#[cfg(target_os = "linux")]
fn numa_node_count() -> Option<u32> {
    count_id_list(&std::fs::read_to_string("/sys/devices/system/node/online").ok()?)
}

#[cfg(target_os = "windows")]
fn numa_node_count() -> Option<u32> {
    let mut highest = 0u32;
    // SAFETY: the call only writes its out-parameter.
    let ok =
        unsafe { windows_sys::Win32::System::Threading::GetNumaHighestNodeNumber(&mut highest) };
    (ok != 0).then(|| highest + 1)
}

#[cfg(target_os = "macos")]
fn numa_node_count() -> Option<u32> {
    Some(1)
}

#[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
fn numa_node_count() -> Option<u32> {
    None
}

/// Number of ids in a sysfs list such as `0-1,3`.
#[cfg(any(target_os = "linux", test))]
fn count_id_list(raw: &str) -> Option<u32> {
    let mut total = 0u32;
    for part in raw.trim().split(',').filter(|p| !p.is_empty()) {
        total += match part.split_once('-') {
            Some((lo, hi)) => {
                hi.parse::<u32>()
                    .ok()?
                    .checked_sub(lo.parse::<u32>().ok()?)?
                    + 1
            }
            None => {
                part.parse::<u32>().ok()?;
                1
            }
        };
    }
    (total > 0).then_some(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sysfs_id_lists_count_ranges() {
        assert_eq!(count_id_list("0\n"), Some(1));
        assert_eq!(count_id_list("0-1"), Some(2));
        assert_eq!(count_id_list("0-1,4-5,7"), Some(5));
        assert_eq!(count_id_list(""), None);
        assert_eq!(count_id_list("3-1"), None);
        assert_eq!(count_id_list("x"), None);
    }
    use crate::diagnostics::TEST_LOCK as WORKER_TEST;

    fn trial_fixture() -> TrialOutcome {
        let report: Report =
            serde_json::from_str(include_str!("../telemetry/tuning-fixture.json")).unwrap();
        report.tuning.unwrap().trials.remove(0).outcome
    }

    #[test]
    fn trial_queue_is_bounded_fifo_nonblocking_and_monotonic_across_drains() {
        let recorder = Recorder::test_recorder();
        let mut outcome = trial_fixture();
        outcome.samples.clear();
        outcome.decision = seine_telemetry::tuning::TrialDecision::Aborted;
        outcome.reason = seine_telemetry::tuning::TrialReason::ExecutionFailure;
        for _ in 0..MAX_TRIALS + 1 {
            recorder.record_trial(outcome.clone());
        }
        let first = recorder.drain_tuning().unwrap();
        assert_eq!(first.trials.len(), MAX_TRIALS);
        assert_eq!(first.dropped_trials, 1);
        assert_eq!(first.trials[0].trial_id, 1);
        assert!(first.validate());
        assert!(recorder.drain_tuning().is_none());
        let lock = recorder.2.queue.lock().unwrap();
        recorder.record_trial(outcome.clone()); // Would deadlock if it used lock().
        drop(lock);
        recorder.record_trial(outcome);
        let second = recorder.drain_tuning().unwrap();
        assert_eq!(second.run_id, first.run_id);
        assert_eq!(second.trials[0].trial_id, MAX_TRIALS as u64 + 1);
        assert_eq!(second.dropped_trials, 1);
    }

    #[test]
    fn worker_exit_disables_trial_collection_and_discards_queued_evidence() {
        let recorder = Recorder::test_recorder();
        let lifetime = TrialWorkerLifetime(Arc::clone(&recorder.2));
        recorder.record_trial(trial_fixture());
        drop(lifetime);
        assert!(!recorder.trials_enabled());
        assert!(recorder.drain_tuning().is_none());
        recorder.record_trial(trial_fixture());
        assert!(recorder.drain_tuning().is_none());
    }

    #[test]
    fn trial_payload_budget_preserves_earliest_outcomes_and_reports_omissions() {
        let recorder = Recorder::test_recorder();
        let outcome = trial_fixture();
        for _ in 0..MAX_TRIALS {
            recorder.record_trial(outcome.clone());
        }
        let report = recorder.drain_tuning().unwrap();
        assert!(serde_json::to_vec(&report).unwrap().len() <= MAX_TUNING_BYTES);
        assert_eq!(
            report.trials.len() as u64 + report.dropped_trials,
            MAX_TRIALS as u64
        );
        assert_eq!(report.trials[0].trial_id, 1);
        let mut invalid = outcome;
        invalid.samples[0].elapsed_ms = 0;
        recorder.record_trial(invalid);
        let dropped = recorder.drain_tuning().unwrap();
        assert!(dropped.trials.is_empty());
        assert_eq!(dropped.dropped_trials, 1);
    }

    #[test]
    fn trial_only_final_report_is_spooled_and_opt_out_clears_it() {
        let _lock = WORKER_TEST.lock().unwrap();
        let dir = Temp::new();
        let server = httpmock::MockServer::start();
        let failed = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/report")
                .body_contains("\"tuning\"");
            then.status(503);
        });
        let options = resolve(&dir.0, Some(true), Some(&server.url("/report")))
            .unwrap()
            .unwrap();
        let (recorder, worker) =
            start_with_context(&options, crate::diagnostics::test_context()).unwrap();
        let mut outcome = trial_fixture();
        outcome.started_ms = 0;
        outcome.finished_ms = recorder.trial_elapsed_ms();
        outcome.samples.clear();
        outcome.cost.switching_ms = None;
        outcome.cost.idle_ms = None;
        outcome.decision = seine_telemetry::tuning::TrialDecision::Aborted;
        outcome.reason = seine_telemetry::tuning::TrialReason::UserInterrupted;
        recorder.record_trial(outcome);
        let run_id = recorder.2.run_id.clone();
        drop(worker);
        failed.assert_hits(1);
        let saved: Report = serde_json::from_slice(
            &fs::read(dir.0.join("seine.diagnostics.pending.json")).unwrap(),
        )
        .unwrap();
        assert!(saved.validate());
        let tuning = saved.tuning.unwrap();
        assert_eq!(tuning.run_id, run_id);
        assert_eq!(tuning.trials[0].trial_id, 1);
        resolve(&dir.0, Some(false), None).unwrap();
        assert!(!dir.0.join("seine.diagnostics.pending.json").exists());
    }

    #[test]
    fn completed_run_marker_is_cleared_before_slow_final_upload() {
        let _guard = WORKER_TEST.lock().unwrap();
        let dir = Temp::new();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}/reports", listener.local_addr().unwrap());
        let options = resolve(&dir.0, Some(true), Some(&endpoint))
            .unwrap()
            .unwrap();
        let (_, worker) = start_with_context(&options, crate::diagnostics::test_context()).unwrap();
        crate::diagnostics::allocation_failure(true, 2147483648, Some(12));
        let finished = std::thread::spawn(move || drop(worker));
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "final upload never arrived");
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(e) => panic!("{e}"),
            }
        };
        assert!(!finished.is_finished());
        assert!(!dir.0.join("seine.diagnostics.running").exists());
        assert!(!dir.0.join("seine.diagnostics.phase.json").exists());
        use std::io::Write;
        socket
            .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
            .unwrap();
        drop(socket);
        finished.join().unwrap();
    }

    #[test]
    fn clean_stop_context_is_spooled_without_leaving_an_unclean_marker() {
        let _guard = WORKER_TEST.lock().unwrap();
        let dir = Temp::new();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/reports", listener.local_addr().unwrap());
        drop(listener); // An immediate connection refusal, no external service.
        let options = resolve(&dir.0, Some(true), Some(&endpoint))
            .unwrap()
            .unwrap();
        let (_, worker) = start_with_context(&options, crate::diagnostics::test_context()).unwrap();
        let origin = crate::diagnostics::StopOrigin::default();
        worker.attach_stop_origin(&origin);
        origin.request(seine_telemetry::ShutdownRequest::ApiStop);
        crate::diagnostics::cleanup_stage(seine_telemetry::CleanupStage::FinalizingTelemetry);
        drop(worker);
        let (store, unclean) =
            crate::diagnostics::Store::open(&options.state_path, &options.installation_id).unwrap();
        assert!(!unclean);
        let report = store.pending(&options.installation_id).unwrap();
        assert!(report.validate());
        let diagnostic = report.diagnostics.unwrap();
        assert_eq!(
            diagnostic.shutdown_request,
            Some(seine_telemetry::ShutdownRequest::ApiStop)
        );
        assert_eq!(
            diagnostic.cleanup_stage,
            Some(seine_telemetry::CleanupStage::FinalizingTelemetry)
        );
        assert_eq!(diagnostic.reason, "shutdown");
        store.finish();
    }

    #[test]
    fn completed_run_marker_is_cleared_during_slow_pending_replay() {
        let _guard = WORKER_TEST.lock().unwrap();
        let dir = Temp::new();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}/reports", listener.local_addr().unwrap());
        let options = resolve(&dir.0, Some(true), Some(&endpoint))
            .unwrap()
            .unwrap();
        let mut pending: Report =
            serde_json::from_str(include_str!("../telemetry/fixture.json")).unwrap();
        pending.installation_id = options.installation_id.clone();
        let (store, _) =
            crate::diagnostics::Store::open(&options.state_path, &options.installation_id).unwrap();
        store.save(&pending);
        store.finish();
        drop(store);
        let (_, worker) = start_with_context(&options, crate::diagnostics::test_context()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "pending replay never arrived");
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(e) => panic!("{e}"),
            }
        };
        let marker = dir.0.join("seine.diagnostics.running");
        assert!(marker.exists(), "live mining must retain its marker");
        // The uploader is blocked in HTTP. Transitions must persist immediately,
        // without waiting for its one-second polling loop or periodic checkpoint.
        let origin = crate::diagnostics::StopOrigin::default();
        worker.attach_stop_origin(&origin);
        origin.request(seine_telemetry::ShutdownRequest::ApiStop);
        crate::diagnostics::cleanup_stage(seine_telemetry::CleanupStage::StoppingBackends);
        crate::diagnostics::nvidia_software(seine_telemetry::NvidiaSoftware {
            driver_cuda_version: Some(12080),
            nvrtc_version: Some(12090),
        });
        let persisted: serde_json::Value = serde_json::from_slice(
            &std::fs::read(dir.0.join("seine.diagnostics.phase.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(persisted["run"]["shutdown_request"], "api_stop");
        assert_eq!(persisted["run"]["cleanup_stage"], "stopping_backends");
        assert_eq!(persisted["run"]["nvidia_software"]["nvrtc_version"], 12090);
        let finished = std::thread::spawn(move || drop(worker));
        let deadline = Instant::now() + Duration::from_secs(1);
        while marker.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let cleared_during_upload =
            !marker.exists() && !dir.0.join("seine.diagnostics.phase.json").exists();
        let still_uploading = !finished.is_finished();
        let still_exclusive =
            crate::diagnostics::Store::open(&options.state_path, &options.installation_id)
                .is_none();
        use std::io::Write;
        socket
            .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
        drop(socket);
        finished.join().unwrap();
        let (store, unclean) =
            crate::diagnostics::Store::open(&options.state_path, &options.installation_id).unwrap();
        assert!(!unclean);
        assert_eq!(
            store.pending(&options.installation_id).unwrap().report_id,
            pending.report_id
        );
        store.finish();
        assert!(still_uploading, "test must observe the in-flight upload");
        assert!(
            still_exclusive,
            "spool ownership must survive until the upload ends"
        );
        assert!(
            cleared_during_upload,
            "completed mining must not wait for HTTP to clear its marker"
        );
    }

    #[test]
    fn completed_uploader_releases_ownership_before_mining_worker_exits() {
        let dir = Temp::new();
        let options = resolve(
            &dir.0,
            Some(true),
            Some("https://collector.example/reports"),
        )
        .unwrap()
        .unwrap();
        let (store, _) =
            crate::diagnostics::Store::open(&options.state_path, &options.installation_id).unwrap();
        let store = Arc::new(store);
        let mut completion = RunCompletion::default();
        completion.attach(&store);
        store.finish(); // The uploader observes opt-out and exits before mining does.
        drop(store);
        let (next_store, unclean) =
            crate::diagnostics::Store::open(&options.state_path, &options.installation_id)
                .expect("the departed uploader must release its OS lock");
        assert!(!unclean);
        completion.finish(); // An old Worker must not clear the new run's marker.
        assert!(dir.0.join("seine.diagnostics.running").exists());
        next_store.finish();
    }

    #[test]
    fn completed_run_rejects_late_store_attachment_and_checkpoints() {
        let _lock = WORKER_TEST.lock().unwrap();
        let dir = Temp::new();
        let options = resolve(
            &dir.0,
            Some(true),
            Some("https://collector.example/reports"),
        )
        .unwrap()
        .unwrap();
        let diagnostics = crate::diagnostics::install(crate::diagnostics::test_context());
        let mut completion = RunCompletion::default();
        completion.finish();
        let (store, _) =
            crate::diagnostics::Store::open(&options.state_path, &options.installation_id).unwrap();
        let store = Arc::new(store);
        completion.attach(&store);
        store.checkpoint(&options.installation_id, diagnostics.0.checkpoint());
        assert!(!dir.0.join("seine.diagnostics.running").exists());
        assert!(!dir.0.join("seine.diagnostics.phase.json").exists());
        drop(store);
        let (store, unclean) =
            crate::diagnostics::Store::open(&options.state_path, &options.installation_id).unwrap();
        assert!(!unclean);
        store.finish();
    }

    #[test]
    fn completed_run_cannot_be_recreated_by_concurrent_checkpoints() {
        let _lock = WORKER_TEST.lock().unwrap();
        let dir = Temp::new();
        let options = resolve(
            &dir.0,
            Some(true),
            Some("https://collector.example/reports"),
        )
        .unwrap()
        .unwrap();
        let diagnostics = crate::diagnostics::install(crate::diagnostics::test_context());
        let (store, _) =
            crate::diagnostics::Store::open(&options.state_path, &options.installation_id).unwrap();
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                barrier.wait();
                for _ in 0..32 {
                    store.checkpoint(&options.installation_id, diagnostics.0.checkpoint());
                }
            });
            barrier.wait();
            store.finish();
        });
        store.checkpoint(&options.installation_id, diagnostics.0.checkpoint());
        assert!(!dir.0.join("seine.diagnostics.running").exists());
        assert!(!dir.0.join("seine.diagnostics.phase.json").exists());
        assert!(!dir.0.join("seine.diagnostics.phase.tmp").exists());
    }

    #[test]
    fn early_failure_is_reported_and_failed_delivery_is_retained_without_restart_storms() {
        let _guard = WORKER_TEST.lock().unwrap();
        let dir = Temp::new();
        let server = httpmock::MockServer::start();
        let failed = server.mock(|when, then| {
            when.method(httpmock::Method::POST).path("/reports");
            then.status(503);
        });
        let options = resolve(&dir.0, None, Some(&server.url("/reports")))
            .unwrap()
            .unwrap();
        let (_, worker) = start_with_context(&options, crate::diagnostics::test_context()).unwrap();
        crate::diagnostics::set_phase(seine_telemetry::RunPhase::InitializingBackends);
        crate::diagnostics::allocation_failure(true, 2147483648, Some(12));
        crate::miner::ui::warn(
            "PRIVATE TAG",
            "driver failed at /home/alice with wallet PRIVATE WALLET",
        );
        let _ = std::panic::catch_unwind(|| panic!("PRIVATE PANIC CONTENT"));
        crate::diagnostics::record(
            seine_telemetry::DiagnosticCode::RunFailed,
            seine_telemetry::DiagnosticKind::Driver,
            Some("nvidia"),
        );
        drop(worker);
        failed.assert_hits(1);
        let saved = fs::read_to_string(dir.0.join("seine.diagnostics.pending.json")).unwrap();
        assert!(!saved.contains("PRIVATE") && !saved.contains("alice"));
        let report: Report = serde_json::from_str(&saved).unwrap();
        assert!(report.validate());
        let diagnostics = report.diagnostics.as_ref().unwrap();
        assert_eq!(diagnostics.reason, "failure");
        assert!(diagnostics
            .events
            .iter()
            .any(|e| e.detail.as_ref().is_some_and(|d| d.reason
                == seine_telemetry::FailureReason::HugePagesUnavailable
                && d.os_error == Some(12)
                && d.requested_bytes == Some(2147483648))));
        assert!(diagnostics
            .events
            .iter()
            .any(|e| e.code == seine_telemetry::DiagnosticCode::Warning
                && e.source == "src/hardware_telemetry.rs"));
        assert!(diagnostics
            .events
            .iter()
            .any(|e| e.code == seine_telemetry::DiagnosticCode::RunFailed));
        assert!(diagnostics
            .events
            .iter()
            .any(|e| e.code == seine_telemetry::DiagnosticCode::RustPanic));
        assert!(!dir.0.join("seine.diagnostics.running").exists());
        let (_, worker) = start_with_context(&options, crate::diagnostics::test_context()).unwrap();
        drop(worker);
        failed.assert_hits(1); // A supervisor restart must not bypass the five-minute cooldown.
        resolve(&dir.0, Some(false), None).unwrap();
        assert!(!dir.0.join("seine.diagnostics.pending.json").exists());
        let (_, worker) = start_with_context(&options, crate::diagnostics::test_context()).unwrap();
        crate::diagnostics::record(
            seine_telemetry::DiagnosticCode::RunFailed,
            seine_telemetry::DiagnosticKind::General,
            None,
        );
        drop(worker);
        failed.assert_hits(1);
    }

    fn resolve(
        data_dir: &Path,
        choice: Option<bool>,
        endpoint: Option<&str>,
    ) -> Result<Option<Options>> {
        resolve_with_default(data_dir, choice, endpoint, None)
    }

    #[test]
    fn release_destination_enables_reporting_without_interaction_and_honors_opt_out() {
        let dir = Temp::new();
        let url = Some("https://collector.example/v1/reports");
        assert!(resolve_with_default(&dir.0, None, None, url)
            .unwrap()
            .is_some());
        assert!(resolve_with_default(&dir.0, Some(false), None, url)
            .unwrap()
            .is_none());
        assert!(resolve_with_default(&dir.0, None, None, url)
            .unwrap()
            .is_none());
    }

    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            Self(
                std::env::temp_dir().join(format!("seine-telemetry-test-{}", random_id().unwrap())),
            )
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn unconfigured_build_is_inactive_and_configured_build_defaults_on() {
        let dir = Temp::new();
        assert!(resolve(&dir.0, None, None).unwrap().is_none());
        assert!(!dir.0.exists());
        assert!(
            resolve(&dir.0, None, Some("https://collector.example/v1/reports"),)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn opt_out_persists_across_destination_changes() {
        let dir = Temp::new();
        let first = resolve(
            &dir.0,
            Some(true),
            Some("https://collector.example/v1/reports"),
        )
        .unwrap()
        .unwrap();
        assert!(still_enabled(&first));
        let remembered = resolve(&dir.0, None, None).unwrap().unwrap();
        assert_eq!(first.installation_id, remembered.installation_id);
        assert!(resolve(&dir.0, Some(false), None).unwrap().is_none());
        assert!(
            resolve(&dir.0, None, Some("https://other.example/v1/reports"))
                .unwrap()
                .is_none()
        );
        assert!(!still_enabled(&first));
        let other = resolve(&dir.0, Some(true), None).unwrap().unwrap();
        assert_ne!(first.installation_id, other.installation_id);
        assert!(resolve(&dir.0, Some(false), None).unwrap().is_none());
        assert!(!still_enabled(&other));
        fs::write(&first.state_path, "broken").unwrap();
        assert!(resolve(&dir.0, Some(false), None).unwrap().is_none());
    }

    #[test]
    fn changing_settings_marks_sample_unsuitable_for_performance_comparison() {
        let r = Recorder::test_recorder();
        let fixture: Report =
            serde_json::from_str(include_str!("../telemetry/fixture.json")).unwrap();
        let mut settings = fixture.backends[0].settings.clone();
        r.record(1, settings.clone(), 100);
        {
            let mut samples = r.0.lock().unwrap();
            samples.get_mut(&1).unwrap().first = Instant::now() - Duration::from_secs(120);
        }
        settings.lanes = 16;
        r.record(1, settings, 200);
        let report = r.drain();
        assert_eq!(report.len(), 1);
        assert!(report[0].settings_changed);
        assert_eq!(report[0].hashes, 200);
        assert!(r.drain().is_empty());
    }

    #[test]
    fn upload_obeys_opt_out_and_does_not_follow_redirects_or_retry_errors() {
        let _guard = WORKER_TEST.lock().unwrap();
        let dir = Temp::new();
        let server = httpmock::MockServer::start();
        let redirect = server.mock(|when, then| {
            when.method(httpmock::Method::POST).path("/report");
            then.status(302).header("Location", server.url("/trap"));
        });
        let trap = server.mock(|when, then| {
            when.path("/trap");
            then.status(204);
        });
        let options = resolve(&dir.0, None, Some(&server.url("/report")))
            .unwrap()
            .unwrap();
        let report: Report =
            serde_json::from_str(include_str!("../telemetry/fixture.json")).unwrap();
        let client = http_client().unwrap();
        upload(&client, &options, &report);
        redirect.assert_hits(1);
        trap.assert_hits(0);
        resolve(&dir.0, Some(false), None).unwrap();
        upload(&client, &options, &report);
        redirect.assert_hits(1);
        let failed = server.mock(|when, then| {
            when.method(httpmock::Method::POST).path("/failed");
            then.status(503);
        });
        let options = resolve(&dir.0, Some(true), Some(&server.url("/failed")))
            .unwrap()
            .unwrap();
        upload(&client, &options, &report);
        failed.assert_hits(1);
        let (_, worker) = start_with_context(&options, crate::diagnostics::test_context()).unwrap();
        drop(worker); // Wakes immediately rather than waiting for the reporting interval.
        failed.assert_hits(1);
    }

    #[test]
    fn endpoint_cannot_redirect_to_cleartext_or_embed_secrets() {
        assert!(validate_endpoint("https://telemetry.example/v1/reports").is_ok());
        assert!(validate_endpoint("http://127.0.0.1:9080/v1/reports").is_ok());
        for bad in [
            "http://telemetry.example/v1/reports",
            "https://token@telemetry.example/",
            "https://telemetry.example/?token=secret",
            "file:///tmp/report",
        ] {
            assert!(validate_endpoint(bad).is_err(), "{bad}");
        }
    }
}
