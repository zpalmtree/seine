use std::collections::{HashMap, HashSet};
#[cfg(test)]
use std::io::Write;
#[cfg(test)]
use std::net::TcpStream;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender, TryRecvError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::net::TcpStream as TokioTcpStream;
use tokio::sync::mpsc;

mod socks;
mod submit_retry;

#[cfg(test)]
mod tls_tests;

pub use socks::SocksProxy;

use submit_retry::{
    is_retryable_submit_reject, AbandonedSubmit, ScheduleOutcome, SubmitRetryQueue,
};

const LOGIN_REQUEST_ID: u64 = 1;
const RECONNECT_DELAY: Duration = Duration::from_secs(2);
/// Longest reconnect delay a pool's `connection_limit` notice can ask for.
const MAX_CONNECTION_LIMIT_DELAY: Duration = Duration::from_secs(120);
/// Longest wait between reconnects after repeated quick failures.
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(60);
/// A session that stays logged in this long resets the backoff.
const HEALTHY_SESSION: Duration = Duration::from_secs(60);
/// Delay used when a `connection_limit` notice gives none.
const DEFAULT_CONNECTION_LIMIT_DELAY: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// Connection attempts without a login before moving to the next pool.
const FAILOVER_AFTER_FAILED_ATTEMPTS: u32 = 2;
/// How often a backup session checks whether the primary pool is back.
const PRIMARY_RECHECK_INTERVAL: Duration = Duration::from_secs(5 * 60);
const MAX_PRIMARY_RECHECK_INTERVAL: Duration = Duration::from_secs(60 * 60);
const CHANNEL_CAPACITY: usize = 4096;
const MAX_SUBMITS_PER_IO_CYCLE: usize = 32;
const MAX_RETRY_SUBMITS_PER_IO_CYCLE: usize = 4;
const SUBMIT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const LOCAL_POOL_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const MAX_POOL_MESSAGE_BYTES: usize = 8 * 1024;
const STRATUM_PROTOCOL_VERSION: u32 = 2;
const STRATUM_CAPABILITY_LOGIN_NEGOTIATION: &str = "login_negotiation";
const STRATUM_CAPABILITY_VALIDATION_STATUS: &str = "share_validation_status";
const STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH: &str = "submit_claimed_hash";
const STRATUM_CAPABILITY_DIFFICULTY_HINT: &str = "difficulty_hint";
const STRATUM_CAPABILITY_SAME_TEMPLATE_REBIND_V1: &str = "same_template_rebind_v1";
/// The pool hands out an extra nonce range for the current job on `refill`.
pub(crate) const STRATUM_CAPABILITY_NONCE_REFILL_V1: &str = "nonce_refill_v1";
const STRATUM_METHOD_REFILL: &str = "refill";
/// A refill unanswered this long falls back to a fresh session.
const REFILL_REPLY_TIMEOUT: Duration = Duration::from_secs(10);
/// Wait before asking again after a `refill rate limited` reply.
const REFILL_RATE_LIMIT_RETRY: Duration = Duration::from_secs(1);
/// The pool credits each share to the worker named on its submit.
pub(crate) const STRATUM_CAPABILITY_PROXY_WORKERS_V1: &str = "proxy_workers_v1";
/// The pool accepts miner-side timing fields on submits.
const STRATUM_CAPABILITY_SUBMIT_TIMING_V1: &str = "submit_timing_v1";
/// This miner hashes Faraday BLAKE2b work. The pool refuses fork-height work to
/// sessions without it, so older miners get an upgrade message instead of
/// mining the old hash and having every share rejected.
const STRATUM_CAPABILITY_POW_BLAKE2B_V1: &str = "pow_blake2b_v1";
const STRATUM_METHOD_NOTIFICATION: &str = "notification";
const SUBMIT_REJECT_REASON_DISCONNECTED: &str = "pool disconnected";
const SUBMIT_REJECT_REASON_LOGIN_REJECTED: &str = "pool login rejected";
const SUBMIT_REJECT_REASON_SEND_FAILED: &str = "submit interrupted during reconnect";
const SUBMIT_REJECT_REASON_RESYNC: &str = "submit interrupted during pool resync";

pub const POOL_NOTIFICATION_POOL_BLOCK_SOLVED: &str = "pool_block_solved";
pub const POOL_NOTIFICATION_MINER_BLOCK_FOUND: &str = "miner_block_found";
/// The pool refused this session at its connection limit and is closing it.
pub const POOL_NOTIFICATION_CONNECTION_LIMIT: &str = "connection_limit";
/// Seine moved between the configured pools.
pub const POOL_NOTIFICATION_FAILOVER: &str = "pool_failover";

const CLIENT_CAPABILITIES: &[&str] = &[
    STRATUM_CAPABILITY_LOGIN_NEGOTIATION,
    STRATUM_CAPABILITY_VALIDATION_STATUS,
    STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH,
    STRATUM_CAPABILITY_DIFFICULTY_HINT,
    STRATUM_CAPABILITY_SAME_TEMPLATE_REBIND_V1,
    STRATUM_CAPABILITY_SUBMIT_TIMING_V1,
    STRATUM_CAPABILITY_NONCE_REFILL_V1,
    STRATUM_CAPABILITY_PROXY_WORKERS_V1,
];

const CLIENT_CAPABILITIES_WITH_BLAKE2B: &[&str] = &[
    STRATUM_CAPABILITY_LOGIN_NEGOTIATION,
    STRATUM_CAPABILITY_VALIDATION_STATUS,
    STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH,
    STRATUM_CAPABILITY_DIFFICULTY_HINT,
    STRATUM_CAPABILITY_SAME_TEMPLATE_REBIND_V1,
    STRATUM_CAPABILITY_SUBMIT_TIMING_V1,
    STRATUM_CAPABILITY_NONCE_REFILL_V1,
    STRATUM_CAPABILITY_PROXY_WORKERS_V1,
    STRATUM_CAPABILITY_POW_BLAKE2B_V1,
];

/// Advertises BLAKE2b only when this build will mine fork-height work: final
/// parameters, or the provisional opt-in for Faraday testnet testing.
fn client_capabilities() -> &'static [&'static str] {
    let mines_blake2b = blocknet_pow_spec::ensure_supported_pow(
        blocknet_pow_spec::FARADAY_ACTIVATION_HEIGHT,
        crate::miner::provisional_faraday_pow_allowed(),
    )
    .is_ok();
    client_capabilities_for(mines_blake2b)
}

fn client_capabilities_for(mines_blake2b: bool) -> &'static [&'static str] {
    if mines_blake2b {
        CLIENT_CAPABILITIES_WITH_BLAKE2B
    } else {
        CLIENT_CAPABILITIES
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PoolJob {
    pub job_id: String,
    #[serde(default)]
    pub template_id: Option<String>,
    pub header_base: String,
    pub target: String,
    #[serde(default)]
    pub difficulty: Option<u64>,
    #[serde(default)]
    pub network_target: Option<String>,
    pub height: u64,
    pub nonce_start: u64,
    pub nonce_end: u64,
    /// When the pool client decoded this job line; starts the job-switch
    /// latency.
    #[serde(skip)]
    pub received_at: Option<Instant>,
}

impl PoolJob {
    pub fn nonce_count(&self) -> u64 {
        if self.nonce_end < self.nonce_start {
            return 0;
        }
        self.nonce_end
            .saturating_sub(self.nonce_start)
            .saturating_add(1)
    }
}

#[derive(Debug, Clone)]
pub struct PoolSubmitAck {
    pub job_id: String,
    pub nonce: u64,
    pub accepted: bool,
    pub difficulty: Option<u64>,
    pub error: Option<String>,
    /// How many times this share was resubmitted after retryable pool
    /// rejects before this final outcome (0 = first submit was final).
    pub retries: u32,
}

/// A share the pool rejected as overloaded and that will be resubmitted
/// unchanged after `delay`. The final outcome arrives later as `SubmitAck`.
#[derive(Debug, Clone)]
pub struct PoolSubmitRetry {
    pub nonce: u64,
    pub reason: String,
    pub retry_number: u32,
    pub max_retries: u32,
    pub delay: Duration,
    pub block_candidate: bool,
}

#[derive(Debug, Clone)]
pub struct PoolLoginAck {
    pub protocol_version: u32,
    pub capabilities: Vec<String>,
    pub required_capabilities: Vec<String>,
}

impl PoolLoginAck {
    pub fn supports(&self, capability: &str) -> bool {
        let probe = capability.trim().to_ascii_lowercase();
        self.capabilities.iter().any(|entry| entry == &probe)
    }
}

#[derive(Debug, Clone)]
pub struct PoolNotification {
    pub kind: String,
    pub message: String,
    /// How long the pool asks the miner to wait before reconnecting.
    pub retry_after: Option<Duration>,
}

#[derive(Debug, Clone)]
pub enum PoolEvent {
    Connected,
    Disconnected(String),
    LoginAccepted(PoolLoginAck),
    LoginRejected(String),
    Job(PoolJob),
    SubmitAck(PoolSubmitAck),
    SubmitRetry(PoolSubmitRetry),
    Notification(PoolNotification),
}

#[derive(Debug, Clone)]
struct PoolSubmit {
    job_id: String,
    nonce: u64,
    claimed_hash: Option<[u8; 32]>,
    backend: Option<String>,
    /// Resubmits already sent for this share after retryable rejects.
    retries: u32,
    timing: Option<SubmitTiming>,
    /// The worker to credit, from a farm proxy naming its rig.
    worker: Option<String>,
}

/// Miner-side timings reported on a share's first submit when the pool
/// advertises `submit_timing_v1`.
#[derive(Debug, Clone, Copy)]
pub struct SubmitTiming {
    /// When the backend found the nonce.
    pub found_at: Instant,
    /// Job received → backends hashing it, sent once per job.
    pub job_switch: Option<Duration>,
}

#[derive(Debug)]
pub struct PoolClient {
    submit_tx: Sender<PoolSubmit>,
    event_rx: Receiver<PoolEvent>,
    reconnect_requested: Arc<AtomicBool>,
    refill_requested: Arc<Mutex<Option<String>>>,
    shutdown: Arc<AtomicBool>,
}

impl PoolClient {
    #[cfg(test)]
    pub fn connect(
        pool_url: &str,
        address: String,
        worker: String,
        process_shutdown: Arc<AtomicBool>,
    ) -> Result<Self> {
        Self::connect_via(pool_url, None, address, worker, process_shutdown)
    }

    /// Like `connect`, reaching every pool in the list through `proxy`.
    pub fn connect_via(
        pool_url: &str,
        proxy: Option<&SocksProxy>,
        address: String,
        worker: String,
        process_shutdown: Arc<AtomicBool>,
    ) -> Result<Self> {
        let mut endpoints = parse_pool_endpoints(pool_url)?;
        for endpoint in &mut endpoints {
            endpoint.proxy = proxy.cloned();
        }
        let (submit_tx, submit_rx) = bounded::<PoolSubmit>(CHANNEL_CAPACITY);
        let (event_tx, event_rx) = bounded::<PoolEvent>(CHANNEL_CAPACITY);
        let reconnect_requested = Arc::new(AtomicBool::new(false));
        let reconnect_requested_for_thread = Arc::clone(&reconnect_requested);
        let refill_requested = Arc::new(Mutex::new(None));
        let refill_requested_for_thread = Arc::clone(&refill_requested);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_for_thread = Arc::clone(&shutdown);

        std::thread::Builder::new()
            .name("pool-stratum".to_string())
            .spawn(move || {
                run_pool_client_thread(
                    endpoints,
                    address,
                    worker,
                    submit_rx,
                    event_tx,
                    process_shutdown,
                    reconnect_requested_for_thread,
                    refill_requested_for_thread,
                    shutdown_for_thread,
                )
            })
            .context("failed to spawn pool client thread")?;

        Ok(Self {
            submit_tx,
            event_rx,
            reconnect_requested,
            refill_requested,
            shutdown,
        })
    }

    #[cfg(test)]
    pub fn submit_share(
        &self,
        job_id: String,
        nonce: u64,
        claimed_hash: Option<[u8; 32]>,
    ) -> Result<()> {
        self.submit_share_with_backend(job_id, nonce, claimed_hash, None)
    }

    #[cfg(test)]
    pub fn submit_share_with_backend(
        &self,
        job_id: String,
        nonce: u64,
        claimed_hash: Option<[u8; 32]>,
        backend: Option<&str>,
    ) -> Result<()> {
        self.submit_share_with_timing(job_id, nonce, claimed_hash, backend, None)
    }

    pub fn submit_share_with_timing(
        &self,
        job_id: String,
        nonce: u64,
        claimed_hash: Option<[u8; 32]>,
        backend: Option<&str>,
        timing: Option<SubmitTiming>,
    ) -> Result<()> {
        self.submit_tx
            .send(PoolSubmit {
                job_id,
                nonce,
                claimed_hash,
                backend: backend.map(str::to_string),
                retries: 0,
                timing,
                worker: None,
            })
            .map_err(|_| anyhow!("pool submit channel closed"))
    }

    /// Submits a share credited to `worker`, for pools that accept
    /// `proxy_workers_v1`; other pools ignore the name.
    pub fn submit_share_for_worker(
        &self,
        job_id: String,
        nonce: u64,
        claimed_hash: Option<[u8; 32]>,
        backend: Option<&str>,
        worker: &str,
    ) -> Result<()> {
        self.submit_tx
            .send(PoolSubmit {
                job_id,
                nonce,
                claimed_hash,
                backend: backend.map(str::to_string),
                retries: 0,
                timing: None,
                worker: Some(worker.to_string()),
            })
            .map_err(|_| anyhow!("pool submit channel closed"))
    }

    pub fn recv_event_timeout(&self, timeout: Duration) -> Option<PoolEvent> {
        match self.event_rx.recv_timeout(timeout) {
            Ok(event) => Some(event),
            Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => None,
        }
    }

    pub fn drain_events(&self) -> Vec<PoolEvent> {
        let mut out = Vec::new();
        loop {
            match self.event_rx.try_recv() {
                Ok(event) => out.push(event),
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }
        out
    }

    /// Obtain a fresh range through a normal login, after all submissions have
    /// reached a terminal response. This must not discard queued/in-flight shares.
    pub fn request_nonce_refill(&self, job_id: &str) {
        *self.refill_requested.lock().expect("refill lock") = Some(job_id.to_string());
    }

    pub fn request_reconnect(&self) {
        self.reconnect_requested.store(true, Ordering::Relaxed);
    }
}

impl Drop for PoolClient {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

#[derive(Debug, Serialize)]
struct StratumRequest<'a, T: Serialize> {
    id: u64,
    method: &'a str,
    params: T,
}

#[derive(Debug, Serialize)]
struct LoginParams<'a> {
    address: &'a str,
    worker: &'a str,
    protocol_version: u32,
    capabilities: &'static [&'static str],
    #[serde(skip_serializing_if = "Option::is_none")]
    difficulty_hint: Option<u64>,
}

#[derive(Debug, Serialize)]
struct SubmitParams<'a> {
    job_id: &'a str,
    nonce: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    claimed_hash: Option<&'a str>,
    miner_version: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    backend: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    found_to_send_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    job_switch_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    worker: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
struct StratumMessage {
    #[serde(default)]
    id: Option<u64>,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    params: Option<Value>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    result: Option<Value>,
}

#[derive(Debug, Deserialize, Default)]
struct LoginResult {
    #[serde(default)]
    protocol_version: Option<u32>,
    #[serde(default)]
    capabilities: Vec<String>,
    #[serde(default)]
    required_capabilities: Vec<String>,
}

#[derive(Debug, Clone)]
struct PoolEndpoint {
    authority: String,
    host: String,
    port: u16,
    tls: bool,
    url: String,
    proxy: Option<SocksProxy>,
}

impl PoolEndpoint {
    fn display(&self) -> &str {
        &self.url
    }

    fn is_local(&self) -> bool {
        self.authority.starts_with("127.0.0.1:")
            || self.authority.starts_with("localhost:")
            || self.authority.starts_with("[::1]:")
    }
}

#[derive(Debug)]
enum ClientOutbound {
    Text(String),
    Keepalive,
}

#[derive(Debug)]
enum InboundFrame {
    Text(String),
    ReadError(String),
}

struct PoolConnection {
    outbound_tx: mpsc::UnboundedSender<ClientOutbound>,
    inbound_rx: mpsc::UnboundedReceiver<InboundFrame>,
    reader_task: tokio::task::JoinHandle<()>,
    writer_task: tokio::task::JoinHandle<()>,
}

impl PoolConnection {
    fn abort(self) {
        drop(self.outbound_tx);
        self.reader_task.abort();
        self.writer_task.abort();
    }
}

fn take_ready_nonce_refill(
    requested: &Mutex<Option<String>>,
    current_job_id: Option<&str>,
    submissions_drained: bool,
) -> bool {
    let mut request = requested.lock().expect("refill lock");
    let Some(job_id) = request.as_deref() else {
        return false;
    };
    let Some(current) = current_job_id else {
        return false;
    };
    // A newer assignment makes an old refill request unnecessary. Bind the
    // request to the job so delayed ACKs cannot interrupt new mining work.
    if job_id != current {
        *request = None;
        return false;
    }
    if !submissions_drained {
        return false;
    }
    *request = None;
    true
}

fn run_pool_client_thread(
    endpoints: Vec<PoolEndpoint>,
    address: String,
    worker: String,
    submit_rx: Receiver<PoolSubmit>,
    event_tx: Sender<PoolEvent>,
    process_shutdown: Arc<AtomicBool>,
    reconnect_requested: Arc<AtomicBool>,
    refill_requested: Arc<Mutex<Option<String>>>,
    shutdown: Arc<AtomicBool>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            let _ = event_tx.try_send(PoolEvent::Disconnected(format!(
                "pool client runtime failed for {}: {err}",
                endpoints[0].display()
            )));
            return;
        }
    };

    runtime.block_on(run_pool_client_loop(
        endpoints,
        address,
        worker,
        submit_rx,
        event_tx,
        process_shutdown,
        reconnect_requested,
        refill_requested,
        shutdown,
    ));
}

async fn run_pool_client_loop(
    endpoints: Vec<PoolEndpoint>,
    address: String,
    worker: String,
    submit_rx: Receiver<PoolSubmit>,
    event_tx: Sender<PoolEvent>,
    process_shutdown: Arc<AtomicBool>,
    reconnect_requested: Arc<AtomicBool>,
    refill_requested: Arc<Mutex<Option<String>>>,
    shutdown: Arc<AtomicBool>,
) {
    let mut next_request_id = LOGIN_REQUEST_ID + 1;
    let mut allow_login_difficulty_hint = false;
    let mut login_difficulty_hint: Option<u64> = None;
    let mut failover = PoolFailover::new(endpoints.len());
    let mut backoff = ReconnectBackoff::default();
    while !should_shutdown(process_shutdown.as_ref(), shutdown.as_ref()) {
        let endpoint = endpoints[failover.active].clone();
        let mut session_logged_in = false;
        let mut switch_to_primary = false;
        match connect_pool_transport(&endpoint).await {
            Ok(mut connection) => {
                reject_queued_submits(&event_tx, &submit_rx, SUBMIT_REJECT_REASON_DISCONNECTED);
                let _ = event_tx.try_send(PoolEvent::Connected);
                let login_hint = if allow_login_difficulty_hint {
                    login_difficulty_hint
                } else {
                    None
                };
                let login_payload = match login_payload(&address, &worker, login_hint) {
                    Ok(payload) => payload,
                    Err(_) => {
                        reject_queued_submits(
                            &event_tx,
                            &submit_rx,
                            SUBMIT_REJECT_REASON_DISCONNECTED,
                        );
                        let _ = event_tx.try_send(PoolEvent::Disconnected(
                            friendly_pool_disconnect_message(&endpoint),
                        ));
                        connection.abort();
                        sleep_with_shutdown(
                            process_shutdown.as_ref(),
                            shutdown.as_ref(),
                            RECONNECT_DELAY,
                        );
                        continue;
                    }
                };
                if connection
                    .outbound_tx
                    .send(ClientOutbound::Text(login_payload))
                    .is_err()
                {
                    reject_queued_submits(&event_tx, &submit_rx, SUBMIT_REJECT_REASON_DISCONNECTED);
                    let _ = event_tx.try_send(PoolEvent::Disconnected(
                        friendly_pool_disconnect_message(&endpoint),
                    ));
                    connection.abort();
                    sleep_with_shutdown(
                        process_shutdown.as_ref(),
                        shutdown.as_ref(),
                        RECONNECT_DELAY,
                    );
                    continue;
                }

                let mut pending_submits = HashMap::<u64, PoolSubmit>::new();
                let mut retry_queue = SubmitRetryQueue::new();
                let mut login_confirmed = false;
                let mut limit_delay = None::<Duration>;
                let session_started = Instant::now();
                let mut current_job_id = None::<String>;
                let mut submit_claimed_hash_enabled = true;
                let mut submit_timing_enabled = false;
                let mut nonce_refill_enabled = false;
                let mut pending_refill = None::<(u64, Instant)>;
                let mut refill_retry = None::<(Instant, String)>;
                let mut last_outbound = Instant::now();
                let mut submit_tick = tokio::time::interval(SUBMIT_POLL_INTERVAL);
                submit_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut next_primary_probe = Instant::now() + failover.primary_recheck;
                let mut primary_probe: Option<tokio::task::JoinHandle<bool>> = None;
                let mut primary_back = false;

                loop {
                    if should_shutdown(process_shutdown.as_ref(), shutdown.as_ref()) {
                        connection.abort();
                        return;
                    }
                    // A refill keeps the current range valid, so it need not
                    // wait for in-flight submits the way a resync does.
                    let mut refill_ready = take_ready_nonce_refill(
                        &refill_requested,
                        current_job_id.as_deref(),
                        login_confirmed
                            && (nonce_refill_enabled
                                || (pending_submits.is_empty() && submit_rx.is_empty())),
                    );
                    if refill_retry
                        .as_ref()
                        .is_some_and(|(at, _)| Instant::now() >= *at)
                    {
                        let (_, job_id) = refill_retry.take().expect("retry checked");
                        if current_job_id.as_deref() == Some(job_id.as_str()) {
                            *refill_requested.lock().expect("refill lock") = Some(job_id);
                        }
                    }
                    if pending_refill
                        .is_some_and(|(_, sent)| sent.elapsed() >= REFILL_REPLY_TIMEOUT)
                    {
                        pending_refill = None;
                        nonce_refill_enabled = false;
                        *refill_requested.lock().expect("refill lock") = current_job_id.clone();
                    }
                    if refill_ready && nonce_refill_enabled {
                        // A refill already in flight answers this request too;
                        // a failed send surfaces as a disconnect.
                        refill_ready = false;
                        if let (None, Some(job_id)) = (pending_refill, current_job_id.as_deref()) {
                            let request_id = next_request_id;
                            next_request_id =
                                next_request_id.wrapping_add(1).max(LOGIN_REQUEST_ID + 1);
                            if let Ok(payload) = refill_payload(request_id, job_id) {
                                if connection
                                    .outbound_tx
                                    .send(ClientOutbound::Text(payload))
                                    .is_ok()
                                {
                                    last_outbound = Instant::now();
                                    pending_refill = Some((request_id, Instant::now()));
                                }
                            }
                        }
                    }
                    let idle = pending_submits.is_empty() && submit_rx.is_empty();
                    if primary_back && idle {
                        let _ = event_tx.try_send(PoolEvent::Notification(PoolNotification {
                            kind: POOL_NOTIFICATION_FAILOVER.to_string(),
                            message: format!(
                                "primary pool {} is reachable again; switching back",
                                endpoints[0].display()
                            ),
                            retry_after: None,
                        }));
                        let _ = event_tx.try_send(PoolEvent::Disconnected(format!(
                            "leaving backup pool {}",
                            endpoint.display()
                        )));
                        switch_to_primary = true;
                        connection.abort();
                        break;
                    }
                    if failover.active != 0
                        && login_confirmed
                        && primary_probe.is_none()
                        && Instant::now() >= next_primary_probe
                    {
                        let primary = endpoints[0].clone();
                        primary_probe = Some(tokio::spawn(async move {
                            connect_pool_transport(&primary)
                                .await
                                .map(PoolConnection::abort)
                                .is_ok()
                        }));
                    }
                    if reconnect_requested.swap(false, Ordering::Relaxed) || refill_ready {
                        *refill_requested.lock().expect("refill lock") = None;
                        reject_pending_submits(
                            &event_tx,
                            &mut pending_submits,
                            &mut retry_queue,
                            SUBMIT_REJECT_REASON_RESYNC,
                        );
                        reject_queued_submits(&event_tx, &submit_rx, SUBMIT_REJECT_REASON_RESYNC);
                        let _ = event_tx.try_send(PoolEvent::Disconnected(format!(
                            "pool resync requested at {}",
                            endpoint.display()
                        )));
                        connection.abort();
                        break;
                    }

                    tokio::select! {
                        probe = async { primary_probe.as_mut().expect("probe guarded").await },
                            if primary_probe.is_some() =>
                        {
                            primary_probe = None;
                            next_primary_probe = Instant::now() + failover.primary_recheck;
                            primary_back = matches!(probe, Ok(true));
                        }
                        _ = submit_tick.tick() => {
                            let mut reconnect_now = false;
                            if login_confirmed {
                                if pending_submits.is_empty()
                                    && last_outbound.elapsed() >= LOCAL_POOL_KEEPALIVE_INTERVAL
                                {
                                    if connection.outbound_tx.send(ClientOutbound::Keepalive).is_err() {
                                        reject_pending_submits(
                                            &event_tx,
                                            &mut pending_submits,
                                            &mut retry_queue,
                                            SUBMIT_REJECT_REASON_SEND_FAILED,
                                        );
                                        reject_queued_submits(
                                            &event_tx,
                                            &submit_rx,
                                            SUBMIT_REJECT_REASON_SEND_FAILED,
                                        );
                                        let _ = event_tx.try_send(PoolEvent::Disconnected(
                                            friendly_pool_disconnect_message(&endpoint),
                                        ));
                                        reconnect_now = true;
                                    } else {
                                        last_outbound = Instant::now();
                                    }
                                }
                                if reconnect_now {
                                    connection.abort();
                                    sleep_with_shutdown(
                                        process_shutdown.as_ref(),
                                        shutdown.as_ref(),
                                        RECONNECT_DELAY,
                                    );
                                    break;
                                }

                                // Due busy-retries go first but are capped per
                                // cycle (and the retry queue is bounded), so fresh
                                // shares are never starved.
                                let (due_retries, abandoned) = retry_queue
                                    .take_due(Instant::now(), MAX_RETRY_SUBMITS_PER_IO_CYCLE);
                                reject_abandoned_submits(&event_tx, abandoned);
                                let mut due_retries = due_retries.into_iter();
                                let mut fresh_sent = 0usize;
                                loop {
                                    let submit = match due_retries.next() {
                                        Some(submit) => submit,
                                        None => {
                                            if fresh_sent >= MAX_SUBMITS_PER_IO_CYCLE {
                                                break;
                                            }
                                            let Ok(submit) = submit_rx.try_recv() else {
                                                break;
                                            };
                                            fresh_sent += 1;
                                            submit
                                        }
                                    };
                                    let request_id = next_request_id;
                                    next_request_id =
                                        next_request_id.wrapping_add(1).max(LOGIN_REQUEST_ID + 1);
                                    let payload = match submit_payload(
                                        request_id,
                                        &submit,
                                        submit_claimed_hash_enabled,
                                        submit_timing_enabled,
                                    ) {
                                        Ok(payload) => payload,
                                        Err(_) => {
                                            reject_submit(&event_tx, submit, SUBMIT_REJECT_REASON_SEND_FAILED);
                                            continue;
                                        }
                                    };
                                    if connection
                                        .outbound_tx
                                        .send(ClientOutbound::Text(payload))
                                        .is_err()
                                    {
                                        reject_submit(&event_tx, submit, SUBMIT_REJECT_REASON_SEND_FAILED);
                                        for submit in due_retries.by_ref() {
                                            reject_submit(
                                                &event_tx,
                                                submit,
                                                SUBMIT_REJECT_REASON_SEND_FAILED,
                                            );
                                        }
                                        reject_pending_submits(
                                            &event_tx,
                                            &mut pending_submits,
                                            &mut retry_queue,
                                            SUBMIT_REJECT_REASON_SEND_FAILED,
                                        );
                                        reject_queued_submits(
                                            &event_tx,
                                            &submit_rx,
                                            SUBMIT_REJECT_REASON_SEND_FAILED,
                                        );
                                        let _ = event_tx.try_send(PoolEvent::Disconnected(
                                            friendly_pool_disconnect_message(&endpoint),
                                        ));
                                        reconnect_now = true;
                                        break;
                                    }
                                    last_outbound = Instant::now();
                                    pending_submits.insert(request_id, submit);
                                }
                            }

                            if reconnect_now {
                                connection.abort();
                                sleep_with_shutdown(
                                    process_shutdown.as_ref(),
                                    shutdown.as_ref(),
                                    RECONNECT_DELAY,
                                );
                                break;
                            }
                        }
                        maybe_inbound = connection.inbound_rx.recv() => {
                            let inbound = match maybe_inbound {
                                Some(frame) => frame,
                                None => {
                                    reject_pending_submits(
                                        &event_tx,
                                        &mut pending_submits,
                                        &mut retry_queue,
                                        SUBMIT_REJECT_REASON_DISCONNECTED,
                                    );
                                    reject_queued_submits(
                                        &event_tx,
                                        &submit_rx,
                                        SUBMIT_REJECT_REASON_DISCONNECTED,
                                    );
                                    let _ = event_tx.try_send(PoolEvent::Disconnected(
                                        friendly_pool_disconnect_message(&endpoint),
                                    ));
                                    connection.abort();
                                    sleep_with_shutdown(
                                        process_shutdown.as_ref(),
                                        shutdown.as_ref(),
                                        backoff.after_close(
                                            limit_delay,
                                            login_confirmed
                                                && session_started.elapsed() >= HEALTHY_SESSION,
                                        ),
                                    );
                                    break;
                                }
                            };

                            let line = match inbound {
                                InboundFrame::Text(line) => line,
                                InboundFrame::ReadError(err) => {
                                    reject_pending_submits(
                                        &event_tx,
                                        &mut pending_submits,
                                        &mut retry_queue,
                                        SUBMIT_REJECT_REASON_DISCONNECTED,
                                    );
                                    reject_queued_submits(
                                        &event_tx,
                                        &submit_rx,
                                        SUBMIT_REJECT_REASON_DISCONNECTED,
                                    );
                                    let detail = err.trim();
                                    let message = if detail.is_empty() {
                                        friendly_pool_disconnect_message(&endpoint)
                                    } else {
                                        format!(
                                            "{} ({detail})",
                                            friendly_pool_disconnect_message(&endpoint)
                                        )
                                    };
                                    let _ = event_tx.try_send(PoolEvent::Disconnected(message));
                                    connection.abort();
                                    sleep_with_shutdown(
                                        process_shutdown.as_ref(),
                                        shutdown.as_ref(),
                                        backoff.after_close(
                                            limit_delay,
                                            login_confirmed
                                                && session_started.elapsed() >= HEALTHY_SESSION,
                                        ),
                                    );
                                    break;
                                }
                            };

                            let trimmed = line.trim();
                            if trimmed.is_empty() {
                                continue;
                            }
                            let decoded = match pending_refill
                                .and_then(|(id, _)| decode_refill_response(trimmed, id))
                            {
                                Some(outcome) => {
                                    pending_refill = None;
                                    match outcome {
                                        RefillOutcome::Job(job) => Some((PoolEvent::Job(job), None)),
                                        RefillOutcome::Stale => None,
                                        RefillOutcome::RateLimited => {
                                            refill_retry = current_job_id.clone().map(|job_id| {
                                                (Instant::now() + REFILL_RATE_LIMIT_RETRY, job_id)
                                            });
                                            None
                                        }
                                        RefillOutcome::Refused => {
                                            // Fall back to a fresh session once
                                            // in-flight submits settle.
                                            nonce_refill_enabled = false;
                                            *refill_requested.lock().expect("refill lock") =
                                                current_job_id.clone();
                                            None
                                        }
                                    }
                                }
                                None => decode_pool_message_with_submit(
                                    trimmed,
                                    &mut login_confirmed,
                                    &mut pending_submits,
                                ),
                            };
                            if let Some((mut event, acked_submit)) = decoded {
                                let decoded_at = Instant::now();
                                if let (PoolEvent::SubmitAck(ack), Some(submit)) =
                                    (&event, acked_submit)
                                {
                                    event = route_submit_ack(
                                        &event_tx,
                                        &mut retry_queue,
                                        ack.clone(),
                                        submit,
                                        decoded_at,
                                    );
                                }
                                if let PoolEvent::Job(job) = &event {
                                    let abandoned = retry_queue.observe_job(job, decoded_at);
                                    reject_abandoned_submits(&event_tx, abandoned);
                                }
                                if let PoolEvent::LoginAccepted(ack) = &event {
                                    match evaluate_login_ack(ack) {
                                        Ok(include_claimed_hash) => {
                                            submit_claimed_hash_enabled = include_claimed_hash;
                                            submit_timing_enabled = ack
                                                .supports(STRATUM_CAPABILITY_SUBMIT_TIMING_V1);
                                            nonce_refill_enabled = ack
                                                .supports(STRATUM_CAPABILITY_NONCE_REFILL_V1);
                                            allow_login_difficulty_hint = ack
                                                .supports(STRATUM_CAPABILITY_DIFFICULTY_HINT)
                                                || ack.required_capabilities.iter().any(
                                                    |capability| {
                                                        capability
                                                            == STRATUM_CAPABILITY_DIFFICULTY_HINT
                                                    },
                                                );
                                        }
                                        Err(message) => {
                                            login_confirmed = false;
                                            allow_login_difficulty_hint = false;
                                            event = PoolEvent::LoginRejected(message);
                                        }
                                    }
                                }
                                match &event {
                                    PoolEvent::Job(job) => {
                                        current_job_id = Some(job.job_id.clone());
                                        if let Some(difficulty) = job.difficulty {
                                            login_difficulty_hint = Some(difficulty.max(1));
                                        }
                                    }
                                    PoolEvent::SubmitAck(ack) => {
                                        if let Some(difficulty) = ack.difficulty {
                                            login_difficulty_hint = Some(difficulty.max(1));
                                        }
                                    }
                                    PoolEvent::Notification(notification)
                                        if notification.kind
                                            == POOL_NOTIFICATION_CONNECTION_LIMIT =>
                                    {
                                        limit_delay = Some(
                                            notification
                                                .retry_after
                                                .unwrap_or(DEFAULT_CONNECTION_LIMIT_DELAY),
                                        );
                                        crate::diagnostics::record_failure(
                                            seine_telemetry::DiagnosticCode::Warning,
                                            seine_telemetry::DiagnosticKind::Connection,
                                            None,
                                            seine_telemetry::FailureDetail::new(
                                                seine_telemetry::FailureReason::PoolConnectionLimit,
                                                seine_telemetry::DiagnosticOperation::PoolConnect,
                                            ),
                                        );
                                    }
                                    PoolEvent::Notification(_) => {}
                                    PoolEvent::LoginRejected(_) => {
                                        allow_login_difficulty_hint = false;
                                    }
                                    _ => {}
                                }
                                let login_rejected = matches!(&event, PoolEvent::LoginRejected(_));
                                let _ = event_tx.try_send(event);
                                if login_rejected {
                                    crate::diagnostics::record_failure(seine_telemetry::DiagnosticCode::Warning,
                                        seine_telemetry::DiagnosticKind::Authentication, None,
                                        seine_telemetry::FailureDetail::new(seine_telemetry::FailureReason::LoginRejected,
                                            seine_telemetry::DiagnosticOperation::PoolLogin));
                                    reject_pending_submits(
                                        &event_tx,
                                        &mut pending_submits,
                                        &mut retry_queue,
                                        SUBMIT_REJECT_REASON_LOGIN_REJECTED,
                                    );
                                    reject_queued_submits(
                                        &event_tx,
                                        &submit_rx,
                                        SUBMIT_REJECT_REASON_LOGIN_REJECTED,
                                    );
                                    let _ = event_tx.try_send(PoolEvent::Disconnected(
                                        friendly_pool_disconnect_message(&endpoint),
                                    ));
                                    connection.abort();
                                    sleep_with_shutdown(
                                        process_shutdown.as_ref(),
                                        shutdown.as_ref(),
                                        backoff.failed(),
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }
                if let Some(probe) = primary_probe.take() {
                    probe.abort();
                }
                session_logged_in = login_confirmed;
            }
            Err(err) => {
                reject_queued_submits(&event_tx, &submit_rx, SUBMIT_REJECT_REASON_DISCONNECTED);
                let detail = format!("{err:#}");
                let _ = event_tx.try_send(PoolEvent::Disconnected(friendly_pool_connect_error(
                    &endpoint, &detail,
                )));
                sleep_with_shutdown(
                    process_shutdown.as_ref(),
                    shutdown.as_ref(),
                    backoff.failed(),
                );
            }
        }
        let previous = failover.active;
        if switch_to_primary {
            failover.switch_to_primary();
        } else if failover.session_ended(session_logged_in) {
            let _ = event_tx.try_send(PoolEvent::Notification(PoolNotification {
                kind: POOL_NOTIFICATION_FAILOVER.to_string(),
                message: format!(
                    "pool {} unavailable; switching to {}",
                    endpoints[previous].display(),
                    endpoints[failover.active].display()
                ),
                retry_after: None,
            }));
        }
    }
}

/// Pool failover bookkeeping. Seine moves to the next pool in the list after
/// repeated attempts that never log in, stays on a pool while sessions log in,
/// and probes the primary pool periodically while on a backup.
#[derive(Debug)]
struct PoolFailover {
    pools: usize,
    active: usize,
    failed_attempts: u32,
    primary_recheck: Duration,
    returned_to_primary: bool,
}

impl PoolFailover {
    fn new(pools: usize) -> Self {
        Self {
            pools: pools.max(1),
            active: 0,
            failed_attempts: 0,
            primary_recheck: PRIMARY_RECHECK_INTERVAL,
            returned_to_primary: false,
        }
    }

    /// Records how a connection attempt ended; returns true when it moved to
    /// another pool.
    fn session_ended(&mut self, logged_in: bool) -> bool {
        if logged_in {
            self.failed_attempts = 0;
            if self.active == 0 {
                self.returned_to_primary = false;
                self.primary_recheck = PRIMARY_RECHECK_INTERVAL;
            }
            return false;
        }
        self.failed_attempts += 1;
        if self.pools < 2 || self.failed_attempts < FAILOVER_AFTER_FAILED_ATTEMPTS {
            return false;
        }
        if self.active == 0 && self.returned_to_primary {
            // The primary answered the probe but still would not log in.
            self.returned_to_primary = false;
            self.primary_recheck = (self.primary_recheck * 2).min(MAX_PRIMARY_RECHECK_INTERVAL);
        }
        self.failed_attempts = 0;
        self.active = (self.active + 1) % self.pools;
        true
    }

    fn switch_to_primary(&mut self) {
        self.active = 0;
        self.failed_attempts = 0;
        self.returned_to_primary = true;
    }
}

async fn connect_pool_transport(endpoint: &PoolEndpoint) -> Result<PoolConnection> {
    let stream = match endpoint.proxy.as_ref() {
        Some(proxy) => proxy
            .connect(&endpoint.host, endpoint.port)
            .await
            .inspect_err(|_| {
                crate::diagnostics::record_failure(
                    seine_telemetry::DiagnosticCode::Warning,
                    seine_telemetry::DiagnosticKind::Connection,
                    None,
                    seine_telemetry::FailureDetail::new(
                        seine_telemetry::FailureReason::ConnectionFailed,
                        seine_telemetry::DiagnosticOperation::PoolConnect,
                    ),
                );
            })?,
        None => connect_pool_tcp_stream(endpoint).await?,
    };
    if !endpoint.tls {
        let (reader_half, writer_half) = stream.into_split();
        return Ok(spawn_pool_io(reader_half, writer_half));
    }
    let stream = connect_pool_tls(endpoint, stream).await.inspect_err(|_| {
        crate::diagnostics::record_failure(
            seine_telemetry::DiagnosticCode::Warning,
            seine_telemetry::DiagnosticKind::Connection,
            None,
            seine_telemetry::FailureDetail::new(
                seine_telemetry::FailureReason::ConnectionFailed,
                seine_telemetry::DiagnosticOperation::PoolConnect,
            ),
        );
    })?;
    let (reader_half, writer_half) = tokio::io::split(stream);
    Ok(spawn_pool_io(reader_half, writer_half))
}

fn spawn_pool_io<R, W>(reader_half: R, writer_half: W) -> PoolConnection
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
    let reader_task = tokio::spawn(async move {
        run_tcp_reader(TokioBufReader::new(reader_half), inbound_tx).await;
    });
    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
    let writer_task = tokio::spawn(async move {
        run_tcp_writer(writer_half, outbound_rx).await;
    });
    PoolConnection {
        outbound_tx,
        inbound_rx,
        reader_task,
        writer_task,
    }
}

/// TLS to a pool named by hostname verifies its certificate against the
/// public web roots. A pool named by IP address is encrypted without
/// checking the certificate's identity, since pool certificates name hosts.
async fn connect_pool_tls(
    endpoint: &PoolEndpoint,
    stream: TokioTcpStream,
) -> Result<tokio_rustls::client::TlsStream<TokioTcpStream>> {
    use tokio_rustls::rustls::pki_types::ServerName;

    let (config, server_name) = match endpoint.host.parse::<std::net::IpAddr>() {
        Ok(ip) => (pool_tls::encrypt_only_config(), ServerName::from(ip)),
        Err(_) => (
            pool_tls::verified_config(),
            ServerName::try_from(endpoint.host.clone())
                .map_err(|_| anyhow!("invalid TLS host name {}", endpoint.host))?,
        ),
    };
    let connector = tokio_rustls::TlsConnector::from(config);
    match tokio::time::timeout(CONNECT_TIMEOUT, connector.connect(server_name, stream)).await {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(err)) => Err(anyhow!(
            "TLS handshake failed with pool at {}: {err}",
            endpoint.display()
        )),
        Err(_) => Err(anyhow!(
            "TLS handshake timed out with pool at {}",
            endpoint.display()
        )),
    }
}

mod pool_tls {
    use std::sync::{Arc, OnceLock};

    use tokio_rustls::rustls::client::danger::{
        HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
    };
    use tokio_rustls::rustls::crypto::{self, ring, CryptoProvider};
    use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use tokio_rustls::rustls::{
        ClientConfig, DigitallySignedStruct, Error, RootCertStore, SignatureScheme,
    };

    fn provider() -> Arc<CryptoProvider> {
        Arc::new(ring::default_provider())
    }

    pub(super) fn verified_config() -> Arc<ClientConfig> {
        static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
        CONFIG
            .get_or_init(|| {
                let roots = RootCertStore {
                    roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
                };
                Arc::new(
                    ClientConfig::builder_with_provider(provider())
                        .with_safe_default_protocol_versions()
                        .expect("ring provider supports the default TLS versions")
                        .with_root_certificates(roots)
                        .with_no_client_auth(),
                )
            })
            .clone()
    }

    pub(super) fn encrypt_only_config() -> Arc<ClientConfig> {
        static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
        CONFIG
            .get_or_init(|| {
                let provider = provider();
                Arc::new(
                    ClientConfig::builder_with_provider(provider.clone())
                        .with_safe_default_protocol_versions()
                        .expect("ring provider supports the default TLS versions")
                        .dangerous()
                        .with_custom_certificate_verifier(Arc::new(EncryptOnly(provider)))
                        .with_no_client_auth(),
                )
            })
            .clone()
    }

    /// Accepts any certificate but still checks the handshake signatures, so
    /// the session key is bound to the certificate the pool presented.
    #[derive(Debug)]
    struct EncryptOnly(Arc<CryptoProvider>);

    impl ServerCertVerifier for EncryptOnly {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            crypto::verify_tls12_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            crypto::verify_tls13_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }
}

async fn connect_pool_tcp_stream(endpoint: &PoolEndpoint) -> Result<TokioTcpStream> {
    let addrs = resolve_pool_addrs(&endpoint.authority)
        .with_context(|| format!("failed to resolve pool endpoint {}", endpoint.display()))?;
    if addrs.is_empty() {
        crate::diagnostics::record_failure(
            seine_telemetry::DiagnosticCode::Warning,
            seine_telemetry::DiagnosticKind::Connection,
            None,
            seine_telemetry::FailureDetail::new(
                seine_telemetry::FailureReason::DnsFailed,
                seine_telemetry::DiagnosticOperation::PoolResolve,
            ),
        );
        bail!(
            "pool endpoint resolved to no addresses: {}",
            endpoint.display()
        );
    }

    let mut last_err: Option<String> = None;
    let mut failure = seine_telemetry::FailureDetail::new(
        seine_telemetry::FailureReason::ConnectionFailed,
        seine_telemetry::DiagnosticOperation::PoolConnect,
    );
    for addr in addrs {
        match tokio::time::timeout(CONNECT_TIMEOUT, TokioTcpStream::connect(addr)).await {
            Ok(Ok(stream)) => {
                let _ = stream.set_nodelay(true);
                return Ok(stream);
            }
            Ok(Err(err)) => {
                failure = crate::diagnostics::io_detail(
                    seine_telemetry::DiagnosticOperation::PoolConnect,
                    &err,
                );
                last_err = Some(err.to_string());
            }
            Err(_) => {
                failure = seine_telemetry::FailureDetail::new(
                    seine_telemetry::FailureReason::ConnectionTimedOut,
                    seine_telemetry::DiagnosticOperation::PoolConnect,
                );
                last_err = Some(format!("connection timed out to {addr}"));
            }
        }
    }

    crate::diagnostics::record_failure(
        seine_telemetry::DiagnosticCode::Warning,
        seine_telemetry::DiagnosticKind::Connection,
        None,
        failure,
    );
    Err(anyhow!(
        "failed to connect to pool at {}: {}",
        endpoint.display(),
        last_err.unwrap_or_else(|| "no addresses available".to_string())
    ))
}

fn resolve_pool_addrs(authority: &str) -> Result<Vec<SocketAddr>> {
    let mut addrs: Vec<SocketAddr> = authority
        .to_socket_addrs()
        .inspect_err(|err| {
            crate::diagnostics::record_io(seine_telemetry::DiagnosticOperation::PoolResolve, err);
        })?
        .collect();
    addrs.sort_by_key(|addr| !addr.is_ipv4());
    Ok(addrs)
}

async fn run_tcp_reader<R: tokio::io::AsyncRead + Unpin>(
    mut reader: TokioBufReader<R>,
    inbound_tx: mpsc::UnboundedSender<InboundFrame>,
) {
    loop {
        match read_line_limited_async(&mut reader, MAX_POOL_MESSAGE_BYTES).await {
            Ok(Some(line)) => {
                if inbound_tx.send(InboundFrame::Text(line)).is_err() {
                    break;
                }
            }
            Ok(None) => {
                crate::diagnostics::record_failure(
                    seine_telemetry::DiagnosticCode::Warning,
                    seine_telemetry::DiagnosticKind::Connection,
                    None,
                    seine_telemetry::FailureDetail::new(
                        seine_telemetry::FailureReason::ConnectionClosed,
                        seine_telemetry::DiagnosticOperation::PoolRead,
                    ),
                );
                break;
            }
            Err(err) => {
                if let Some(io) = err.downcast_ref::<std::io::Error>() {
                    crate::diagnostics::record_io(
                        seine_telemetry::DiagnosticOperation::PoolRead,
                        io,
                    );
                }
                let _ = inbound_tx.send(InboundFrame::ReadError(err.to_string()));
                break;
            }
        }
    }
}

async fn run_tcp_writer<W>(mut writer: W, mut outbound_rx: mpsc::UnboundedReceiver<ClientOutbound>)
where
    W: tokio::io::AsyncWrite + Unpin,
{
    while let Some(message) = outbound_rx.recv().await {
        let result = match message {
            ClientOutbound::Text(text) => {
                let mut payload = text.into_bytes();
                payload.push(b'\n');
                writer.write_all(&payload).await
            }
            ClientOutbound::Keepalive => writer.write_all(b"\n").await,
        };
        if let Err(error) = result {
            crate::diagnostics::record_io(seine_telemetry::DiagnosticOperation::PoolWrite, &error);
            break;
        }
    }
}

pub(crate) async fn read_line_limited_async<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut TokioBufReader<R>,
    max_bytes: usize,
) -> Result<Option<String>> {
    let mut data = Vec::<u8>::with_capacity(256);
    loop {
        let byte = match reader.read_u8().await {
            Ok(value) => value,
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                if data.is_empty() {
                    return Ok(None);
                }
                break;
            }
            Err(err) => return Err(err.into()),
        };

        if byte == b'\n' {
            break;
        }
        if data.len() >= max_bytes {
            bail!("pool message exceeds {max_bytes} bytes");
        }
        if byte != b'\r' {
            data.push(byte);
        }
    }

    String::from_utf8(data)
        .map(Some)
        .map_err(|_| anyhow!("pool message is not valid UTF-8"))
}

fn login_payload(address: &str, worker: &str, difficulty_hint: Option<u64>) -> Result<String> {
    let request = StratumRequest {
        id: LOGIN_REQUEST_ID,
        method: "login",
        params: LoginParams {
            address,
            worker,
            protocol_version: STRATUM_PROTOCOL_VERSION,
            capabilities: client_capabilities(),
            difficulty_hint,
        },
    };
    serde_json::to_string(&request).context("failed to serialize stratum login payload")
}

fn submit_payload(
    request_id: u64,
    submit: &PoolSubmit,
    include_claimed_hash: bool,
    include_timing: bool,
) -> Result<String> {
    let claimed_hash_hex = if include_claimed_hash {
        submit.claimed_hash.map(hex::encode)
    } else {
        None
    };
    // Retries include the pool's backoff, so only a first submit is timed.
    let timing = submit.timing.filter(|_| submit.retries == 0);
    let found_to_send = timing.map(|timing| timing.found_at.elapsed());
    if let Some(found_to_send) = found_to_send {
        crate::miner::block_latency::record(
            crate::miner::block_latency::POOL_FOUND_TO_SEND,
            found_to_send,
        );
    }
    let micros = |elapsed: Duration| elapsed.as_micros().min(u64::MAX as u128) as u64;
    let (found_to_send_us, job_switch_us) = if include_timing {
        (
            found_to_send.map(micros),
            timing.and_then(|timing| timing.job_switch).map(micros),
        )
    } else {
        (None, None)
    };
    let request = StratumRequest {
        id: request_id,
        method: "submit",
        params: SubmitParams {
            job_id: &submit.job_id,
            nonce: submit.nonce,
            claimed_hash: claimed_hash_hex.as_deref(),
            miner_version: env!("CARGO_PKG_VERSION"),
            backend: submit.backend.as_deref(),
            found_to_send_us,
            job_switch_us,
            worker: submit.worker.as_deref(),
        },
    };
    serde_json::to_string(&request).context("failed to serialize stratum submit payload")
}

fn refill_payload(request_id: u64, job_id: &str) -> Result<String> {
    serde_json::to_string(&serde_json::json!({
        "id": request_id,
        "method": STRATUM_METHOD_REFILL,
        "params": { "job_id": job_id },
    }))
    .context("failed to serialize stratum refill payload")
}

#[derive(Debug)]
enum RefillOutcome {
    Job(PoolJob),
    /// The job moved on before the refill was handled; a new job is coming.
    Stale,
    /// The pool asked for slower refills; retry shortly on the same session.
    RateLimited,
    /// At the pool's cap, or malformed.
    Refused,
}

/// Decodes the reply to the in-flight refill request `request_id`, or `None`
/// when the line is some other message.
fn decode_refill_response(raw: &str, request_id: u64) -> Option<RefillOutcome> {
    let msg: StratumMessage = serde_json::from_str(raw).ok()?;
    if msg.method.is_some() || msg.id != Some(request_id) {
        return None;
    }
    if let Some(error) = msg.error {
        let error = error.trim();
        return Some(if error.eq_ignore_ascii_case("stale job") {
            RefillOutcome::Stale
        } else if error.eq_ignore_ascii_case("refill rate limited") {
            RefillOutcome::RateLimited
        } else {
            RefillOutcome::Refused
        });
    }
    Some(
        msg.result
            .and_then(|result| serde_json::from_value::<PoolJob>(result).ok())
            .map(|mut job| {
                job.received_at = Some(Instant::now());
                RefillOutcome::Job(job)
            })
            .unwrap_or(RefillOutcome::Refused),
    )
}

fn friendly_pool_connect_error(endpoint: &PoolEndpoint, detail: &str) -> String {
    let lower = detail.to_ascii_lowercase();
    if lower.contains("socks5 proxy") {
        return format!("pool {} unreachable: {detail}", endpoint.display());
    }
    if lower.contains("connection refused") {
        if endpoint.is_local() {
            return format!("pool offline at {}", endpoint.display());
        }
        return format!("pool unreachable at {}", endpoint.display());
    }
    if lower.contains("tls handshake") {
        if lower.contains("certificate") {
            return format!(
                "pool TLS certificate not trusted at {} (connect by IP address to skip the name check)",
                endpoint.display()
            );
        }
        return format!(
            "pool TLS failed at {} (check that this port uses TLS)",
            endpoint.display()
        );
    }
    if lower.contains("timed out") {
        return format!("pool timed out at {}", endpoint.display());
    }
    if lower.contains("failed to resolve")
        || lower.contains("resolved to no addresses")
        || lower.contains("name or service not known")
    {
        return format!("invalid pool address {}", endpoint.display());
    }
    format!("pool offline at {}", endpoint.display())
}

fn friendly_pool_disconnect_message(endpoint: &PoolEndpoint) -> String {
    format!("pool offline at {}", endpoint.display())
}

#[cfg(test)]
fn send_login(
    stream: &mut TcpStream,
    address: &str,
    worker: &str,
    difficulty_hint: Option<u64>,
) -> Result<()> {
    let payload = login_payload(address, worker, difficulty_hint)?;
    write_text_line(stream, &payload)
}

#[cfg(test)]
fn send_submit(
    stream: &mut TcpStream,
    request_id: u64,
    submit: &PoolSubmit,
    include_claimed_hash: bool,
) -> Result<()> {
    let payload = submit_payload(request_id, submit, include_claimed_hash, true)?;
    write_text_line(stream, &payload)
}

#[cfg(test)]
fn write_text_line(stream: &mut TcpStream, payload: &str) -> Result<()> {
    let mut line = payload.as_bytes().to_vec();
    line.push(b'\n');
    stream
        .write_all(&line)
        .context("failed writing stratum payload")
}

#[cfg(test)]
fn decode_pool_message(
    raw: &str,
    login_confirmed: &mut bool,
    pending_submits: &mut HashMap<u64, PoolSubmit>,
) -> Option<PoolEvent> {
    decode_pool_message_with_submit(raw, login_confirmed, pending_submits).map(|(event, _)| event)
}

/// Decodes one pool line. For submit acks the matching in-flight submit is
/// returned too, so a retryable reject can be resubmitted unchanged.
fn decode_pool_message_with_submit(
    raw: &str,
    login_confirmed: &mut bool,
    pending_submits: &mut HashMap<u64, PoolSubmit>,
) -> Option<(PoolEvent, Option<PoolSubmit>)> {
    let msg: StratumMessage = serde_json::from_str(raw).ok()?;

    if let Some(method) = msg.method.as_deref() {
        if method == "job" {
            let params = msg.params?;
            let mut job: PoolJob = serde_json::from_value(params).ok()?;
            job.received_at = Some(Instant::now());
            return Some((PoolEvent::Job(job), None));
        }
        if method == STRATUM_METHOD_NOTIFICATION {
            let notification = parse_pool_notification(msg.params)?;
            return Some((PoolEvent::Notification(notification), None));
        }
    }

    let id = msg.id?;
    if id == LOGIN_REQUEST_ID {
        if let Some(err) = msg.error {
            *login_confirmed = false;
            return Some((PoolEvent::LoginRejected(err), None));
        }
        let status_ok = msg
            .status
            .as_deref()
            .map(|status| status.eq_ignore_ascii_case("ok"))
            .unwrap_or(false);
        if status_ok {
            *login_confirmed = true;
            let ack = parse_login_result(msg.result);
            return Some((PoolEvent::LoginAccepted(ack), None));
        }
        *login_confirmed = false;
        return Some((
            PoolEvent::LoginRejected("pool rejected login".to_string()),
            None,
        ));
    }

    let submit = pending_submits.remove(&id)?;
    let result = msg.result.as_ref();
    let accepted = if msg.error.is_some() {
        false
    } else if let Some(result) = result {
        if let Some(value) = result.get("accepted").and_then(Value::as_bool) {
            value
        } else {
            msg.status
                .as_deref()
                .map(|status| status.eq_ignore_ascii_case("ok"))
                .unwrap_or(false)
        }
    } else {
        msg.status
            .as_deref()
            .map(|status| status.eq_ignore_ascii_case("ok"))
            .unwrap_or(false)
    };
    let difficulty = result
        .and_then(|value| value.get("difficulty"))
        .and_then(Value::as_u64);
    let error = if accepted {
        None
    } else {
        let result_status = result
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str)
            .map(str::to_string);
        Some(
            msg.error
                .or(result_status)
                .or_else(|| msg.status)
                .unwrap_or_else(|| "pool rejected share".to_string()),
        )
    };

    let ack = PoolSubmitAck {
        job_id: submit.job_id.clone(),
        nonce: submit.nonce,
        accepted,
        difficulty,
        error,
        retries: submit.retries,
    };
    Some((PoolEvent::SubmitAck(ack), Some(submit)))
}

fn parse_pool_notification(params: Option<Value>) -> Option<PoolNotification> {
    let params = params?;
    if let Some(message) = params.as_str() {
        let message = message.trim();
        if message.is_empty() {
            return None;
        }
        return Some(PoolNotification {
            kind: "pool_notice".to_string(),
            message: message.to_string(),
            retry_after: None,
        });
    }
    let object = params.as_object()?;
    let message = object.get("message")?.as_str()?.trim();
    if message.is_empty() {
        return None;
    }
    let kind = object
        .get("kind")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("pool_notice");
    let retry_after = object
        .get("retry_after_ms")
        .and_then(Value::as_u64)
        .map(Duration::from_millis);
    Some(PoolNotification {
        kind: kind.to_string(),
        message: message.to_string(),
        retry_after,
    })
}

/// Reconnect delays: none after a healthy session, otherwise 2 s doubling to
/// 60 s across consecutive quick failures, with jitter so many rigs that fail
/// together do not retry together.
#[derive(Debug, Default)]
struct ReconnectBackoff {
    failures: u32,
}

impl ReconnectBackoff {
    /// The wait after a failed connect, a rejected login, or a session that
    /// ended before it was healthy.
    fn failed(&mut self) -> Duration {
        let base = RECONNECT_DELAY
            .saturating_mul(1u32 << self.failures.min(6))
            .min(MAX_RECONNECT_BACKOFF);
        self.failures = self.failures.saturating_add(1);
        jittered(base)
    }

    /// The wait after the pool closed the session: the delay a
    /// `connection_limit` notice asked for, if longer.
    fn after_close(&mut self, limit_delay: Option<Duration>, healthy: bool) -> Duration {
        let delay = if healthy {
            self.failures = 0;
            Duration::ZERO
        } else {
            self.failed()
        };
        match limit_delay {
            Some(limit) => delay.max(limit.clamp(RECONNECT_DELAY, MAX_CONNECTION_LIMIT_DELAY)),
            None => delay,
        }
    }
}

/// `base` scaled by a random factor between 0.75 and 1.25.
fn jittered(base: Duration) -> Duration {
    let mut buf = [0u8; 2];
    let unit = if getrandom::getrandom(&mut buf).is_ok() {
        f64::from(u16::from_le_bytes(buf)) / f64::from(u16::MAX)
    } else {
        0.5
    };
    base.mul_f64(0.75 + unit * 0.5)
}

fn parse_login_result(result: Option<Value>) -> PoolLoginAck {
    let parsed = result
        .and_then(|value| serde_json::from_value::<LoginResult>(value).ok())
        .unwrap_or_default();
    let protocol_version = parsed
        .protocol_version
        .filter(|version| *version > 0)
        .unwrap_or(1);

    PoolLoginAck {
        protocol_version,
        capabilities: normalize_capability_list(parsed.capabilities),
        required_capabilities: normalize_capability_list(parsed.required_capabilities),
    }
}

fn normalize_capability_list(values: Vec<String>) -> Vec<String> {
    if values.is_empty() {
        return Vec::new();
    }
    let mut seen = HashSet::<String>::new();
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let capability = value.trim().to_ascii_lowercase();
        if capability.is_empty() {
            continue;
        }
        if seen.insert(capability.clone()) {
            out.push(capability);
        }
    }
    out
}

fn should_send_claimed_hash(ack: &PoolLoginAck) -> bool {
    if ack.capabilities.is_empty()
        && ack.required_capabilities.is_empty()
        && ack.protocol_version < STRATUM_PROTOCOL_VERSION
    {
        // Legacy pool response without negotiation metadata: stay optimistic and
        // include claimed hash, because older pools generally ignore unknown fields.
        return true;
    }
    ack.supports(STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH)
        || ack
            .required_capabilities
            .iter()
            .any(|capability| capability == STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH)
}

fn evaluate_login_ack(ack: &PoolLoginAck) -> std::result::Result<bool, String> {
    for capability in &ack.required_capabilities {
        if !client_capabilities()
            .iter()
            .any(|supported| supported == capability)
        {
            return Err(format!("pool requires unsupported capability {capability}"));
        }
    }
    Ok(should_send_claimed_hash(ack))
}

fn reject_submit(event_tx: &Sender<PoolEvent>, submit: PoolSubmit, reason: &str) {
    let _ = event_tx.try_send(PoolEvent::SubmitAck(PoolSubmitAck {
        job_id: submit.job_id,
        nonce: submit.nonce,
        accepted: false,
        difficulty: None,
        error: Some(reason.to_string()),
        retries: submit.retries,
    }));
}

fn reject_abandoned_submits(event_tx: &Sender<PoolEvent>, abandoned: Vec<AbandonedSubmit>) {
    for AbandonedSubmit { submit, reason } in abandoned {
        reject_submit(event_tx, submit, &reason);
    }
}

fn reject_pending_submits(
    event_tx: &Sender<PoolEvent>,
    pending_submits: &mut HashMap<u64, PoolSubmit>,
    retry_queue: &mut SubmitRetryQueue,
    reason: &str,
) {
    for (_, submit) in pending_submits.drain() {
        reject_submit(event_tx, submit, reason);
    }
    // Busy-rejected shares waiting for a resubmit are tied to this session's
    // job ids, so they cannot survive a disconnect either.
    for submit in retry_queue.drain() {
        reject_submit(event_tx, submit, reason);
    }
}

/// Turns a submit ack into the event to report. Retryable rejects are kept in
/// `retry_queue` instead and reported as `SubmitRetry`.
fn route_submit_ack(
    event_tx: &Sender<PoolEvent>,
    retry_queue: &mut SubmitRetryQueue,
    mut ack: PoolSubmitAck,
    submit: PoolSubmit,
    now: Instant,
) -> PoolEvent {
    ack.retries = submit.retries;
    if ack.accepted {
        return PoolEvent::SubmitAck(ack);
    }
    let reason = ack.error.clone().unwrap_or_default();
    if !is_retryable_submit_reject(&reason) {
        return PoolEvent::SubmitAck(ack);
    }
    match retry_queue.schedule(submit, &reason, now) {
        ScheduleOutcome::Scheduled {
            retry_number,
            max_retries,
            delay,
            block_candidate,
            evicted,
        } => {
            if let Some(evicted) = evicted {
                reject_abandoned_submits(event_tx, vec![evicted]);
            }
            PoolEvent::SubmitRetry(PoolSubmitRetry {
                nonce: ack.nonce,
                reason,
                retry_number,
                max_retries,
                delay,
                block_candidate,
            })
        }
        ScheduleOutcome::GaveUp(abandoned) => {
            ack.error = Some(abandoned.reason);
            PoolEvent::SubmitAck(ack)
        }
    }
}

fn reject_queued_submits(
    event_tx: &Sender<PoolEvent>,
    submit_rx: &Receiver<PoolSubmit>,
    reason: &str,
) {
    while let Ok(submit) = submit_rx.try_recv() {
        reject_submit(event_tx, submit, reason);
    }
}

const PLAIN_POOL_SCHEMES: &[&str] = &["stratum+tcp://", "tcp://"];
const TLS_POOL_SCHEMES: &[&str] = &["stratum+ssl://", "stratum+tls://", "ssl://", "tls://"];

/// Normalizes a pool URL, or a comma-separated list of them in failover
/// order, to canonical `stratum+tcp://host:port` / `stratum+ssl://host:port`
/// entries joined by commas.
pub fn normalize_pool_urls(input: &str) -> Result<String> {
    Ok(parse_pool_endpoints(input)?
        .into_iter()
        .map(|endpoint| endpoint.url)
        .collect::<Vec<_>>()
        .join(","))
}

/// The first (primary) pool in a normalized pool URL list.
pub fn primary_pool_url(pool_urls: &str) -> &str {
    pool_urls.split(',').next().unwrap_or(pool_urls).trim()
}

fn parse_pool_endpoints(input: &str) -> Result<Vec<PoolEndpoint>> {
    let endpoints = input
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(parse_pool_endpoint)
        .collect::<Result<Vec<_>>>()?;
    if endpoints.is_empty() {
        bail!("pool URL is empty");
    }
    Ok(endpoints)
}

fn parse_pool_endpoint(pool_url: &str) -> Result<PoolEndpoint> {
    let trimmed = pool_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        bail!("pool URL is empty");
    }
    if trimmed.starts_with("stratum+ws://") || trimmed.starts_with("stratum+wss://") {
        bail!(
            "websocket pool URLs are no longer supported; use host:port, stratum+tcp://host:port or stratum+ssl://host:port"
        );
    }

    let strip = |schemes: &[&str]| {
        schemes
            .iter()
            .find_map(|scheme| trimmed.strip_prefix(scheme))
    };
    let (tls, rest) = if let Some(rest) = strip(TLS_POOL_SCHEMES) {
        (true, rest)
    } else if let Some(rest) = strip(PLAIN_POOL_SCHEMES) {
        (false, rest)
    } else {
        (false, trimmed)
    };
    let authority = rest.split('/').next().unwrap_or(rest).trim();
    if authority.is_empty() {
        bail!("pool URL authority is empty");
    }
    if authority.contains("://") {
        bail!("unsupported pool URL scheme in {trimmed}");
    }
    if rest != authority {
        bail!("pool URL paths are not supported");
    }
    let (host, port) = parse_pool_host_port(authority)
        .ok_or_else(|| anyhow!("pool URL must include a host and port: {trimmed}"))?;
    let host_for_url = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.clone()
    };
    let authority = format!("{host_for_url}:{port}");
    let scheme = if tls {
        "stratum+ssl://"
    } else {
        "stratum+tcp://"
    };
    Ok(PoolEndpoint {
        url: format!("{scheme}{authority}"),
        authority,
        host,
        port,
        tls,
        proxy: None,
    })
}

/// Splits `host:port`, `[v6]:port` or a bare port (localhost).
fn parse_pool_host_port(authority: &str) -> Option<(String, u16)> {
    if authority.chars().all(|c| c.is_ascii_digit()) {
        return Some(("127.0.0.1".to_string(), authority.parse().ok()?));
    }
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, port) = rest.split_once(']')?;
        (host, port.strip_prefix(':')?)
    } else {
        authority.rsplit_once(':')?
    };
    let host = host.trim();
    if host.is_empty() {
        return None;
    }
    Some((host.to_string(), port.parse().ok()?))
}

fn should_shutdown(process_shutdown: &AtomicBool, shutdown: &AtomicBool) -> bool {
    process_shutdown.load(Ordering::Relaxed) || shutdown.load(Ordering::Relaxed)
}

fn sleep_with_shutdown(process_shutdown: &AtomicBool, shutdown: &AtomicBool, duration: Duration) {
    let deadline = std::time::Instant::now() + duration;
    while !should_shutdown(process_shutdown, shutdown) {
        if std::time::Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn pq_login_and_gpu_nonce_ranges_preserve_exact_wire_values() {
        let address = bs58::encode(vec![0x73; 1253]).into_string();
        let wire = super::login_payload(&address, "rig", Some(9_007_199_254_740_993)).unwrap();
        assert!(wire.len() < 8 * 1024);
        let value: serde_json::Value = serde_json::from_str(&wire).unwrap();
        assert_eq!(value["params"]["address"], address);
        assert_eq!(
            value["params"]["difficulty_hint"].as_u64(),
            Some(9_007_199_254_740_993)
        );
        let job: super::PoolJob = serde_json::from_value(serde_json::json!({
            "job_id": "large", "header_base": "ab".repeat(156), "target": "ff".repeat(32),
            "height": 103680, "nonce_start": (i64::MAX as u64) - (1u64 << 40) + 1,
            "nonce_end": i64::MAX as u64,
        }))
        .unwrap();
        assert_eq!(job.nonce_count(), 1u64 << 40);
        assert_eq!(job.nonce_end, i64::MAX as u64);
        assert_eq!(hex::decode(&job.header_base).unwrap().len(), 156);
    }
    use super::*;
    use std::io::{BufRead, BufReader, ErrorKind, Read};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn refused_connection_retains_typed_cause_without_endpoint() {
        let _test_lock = crate::diagnostics::TEST_LOCK.lock().unwrap();
        let guard = crate::diagnostics::install(crate::diagnostics::test_context());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let endpoint = parse_pool_endpoint(&format!("stratum+tcp://{address}")).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(runtime
            .block_on(connect_pool_tcp_stream(&endpoint))
            .is_err());
        let snapshot = guard.0.snapshot(false);
        assert_eq!(snapshot.events.len(), 1);
        let detail = snapshot.events[0].detail.as_ref().unwrap();
        assert_eq!(
            detail.operation,
            seine_telemetry::DiagnosticOperation::PoolConnect
        );
        assert_eq!(
            detail.reason,
            seine_telemetry::FailureReason::ConnectionRefused
        );
        assert!(detail.os_error.is_some());
        assert!(!serde_json::to_string(&snapshot)
            .unwrap()
            .contains(&address.to_string()));
    }

    fn capture_payload<F>(writer: F) -> Value
    where
        F: FnOnce(&mut TcpStream),
    {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("test listener should bind to loopback");
        let addr = listener
            .local_addr()
            .expect("listener should expose local address");

        let reader_thread = thread::spawn(move || {
            let (socket, _) = listener.accept().expect("test listener should accept");
            let mut line = String::new();
            let mut reader = BufReader::new(socket);
            reader
                .read_line(&mut line)
                .expect("pool submit payload should be readable");
            line
        });

        let mut stream = TcpStream::connect(addr).expect("test stream should connect");
        writer(&mut stream);
        drop(stream);

        let raw = reader_thread
            .join()
            .expect("reader thread should complete cleanly");
        serde_json::from_str(raw.trim()).expect("submit payload should decode as valid json")
    }

    fn capture_login_payload(address: &str, worker: &str, difficulty_hint: Option<u64>) -> Value {
        capture_payload(|stream| {
            send_login(stream, address, worker, difficulty_hint)
                .expect("login payload should serialize");
        })
    }

    fn capture_submit_payload(
        submit: PoolSubmit,
        request_id: u64,
        include_claimed_hash: bool,
    ) -> Value {
        capture_payload(|stream| {
            send_submit(stream, request_id, &submit, include_claimed_hash)
                .expect("submit payload should serialize");
        })
    }

    #[test]
    fn submit_payload_reports_timing_only_when_negotiated_and_first_sent() {
        let submit = PoolSubmit {
            job_id: "job-1".to_string(),
            nonce: 7,
            claimed_hash: None,
            backend: None,
            retries: 0,
            timing: Some(SubmitTiming {
                found_at: Instant::now() - Duration::from_millis(3),
                job_switch: Some(Duration::from_micros(1_500)),
            }),
            worker: None,
        };
        let params = |submit: &PoolSubmit, include_timing: bool| -> Value {
            let payload =
                submit_payload(1, submit, false, include_timing).expect("payload serializes");
            serde_json::from_str::<Value>(&payload).expect("payload is json")["params"].clone()
        };

        let timed = params(&submit, true);
        assert!(timed["found_to_send_us"]
            .as_u64()
            .is_some_and(|us| us >= 3_000));
        assert_eq!(timed["job_switch_us"].as_u64(), Some(1_500));

        let untimed = params(&submit, false);
        assert!(untimed.get("found_to_send_us").is_none());
        assert!(untimed.get("job_switch_us").is_none());

        let retry = PoolSubmit {
            retries: 1,
            ..submit
        };
        let retried = params(&retry, true);
        assert!(retried.get("found_to_send_us").is_none());
        assert!(retried.get("job_switch_us").is_none());
    }

    #[test]
    fn send_login_includes_protocol_negotiation_fields() {
        let payload = capture_login_payload("XbTestAddress", "rig01", None);

        assert_eq!(
            payload.get("id").and_then(Value::as_u64),
            Some(LOGIN_REQUEST_ID)
        );
        assert_eq!(payload.get("method").and_then(Value::as_str), Some("login"));
        assert_eq!(
            payload
                .get("params")
                .and_then(|params| params.get("protocol_version"))
                .and_then(Value::as_u64),
            Some(STRATUM_PROTOCOL_VERSION as u64)
        );
        let capabilities = payload
            .get("params")
            .and_then(|params| params.get("capabilities"))
            .and_then(Value::as_array)
            .expect("login params should include capabilities array");
        assert!(
            capabilities
                .iter()
                .any(|entry| entry.as_str() == Some(STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH)),
            "client should advertise claimed hash support"
        );
        assert!(
            payload
                .get("params")
                .and_then(|params| params.get("difficulty_hint"))
                .is_none(),
            "difficulty hint should be omitted when unknown"
        );
    }

    #[test]
    fn blake2b_capability_is_advertised_only_when_fork_work_is_mined() {
        assert!(!client_capabilities_for(false).contains(&STRATUM_CAPABILITY_POW_BLAKE2B_V1));
        let with = client_capabilities_for(true);
        assert!(with.contains(&STRATUM_CAPABILITY_POW_BLAKE2B_V1));
        assert_eq!(&with[..with.len() - 1], CLIENT_CAPABILITIES);
        // Until the parameters are final this build needs the opt-in.
        if !blocknet_pow_spec::BLAKE2B_POW_PARAMS_FINAL
            && !crate::miner::provisional_faraday_pow_allowed()
        {
            assert_eq!(client_capabilities(), CLIENT_CAPABILITIES);
        }
    }

    #[test]
    fn send_login_includes_difficulty_hint_when_available() {
        let payload = capture_login_payload("XbTestAddress", "rig01", Some(321));
        assert_eq!(
            payload
                .get("params")
                .and_then(|params| params.get("difficulty_hint"))
                .and_then(Value::as_u64),
            Some(321)
        );
    }

    #[test]
    fn send_submit_includes_claimed_hash_for_v2_pools() {
        let payload = capture_submit_payload(
            PoolSubmit {
                job_id: "job-1".to_string(),
                nonce: 42,
                claimed_hash: Some([0xAB; 32]),
                backend: Some("nvidia#1".to_string()),
                retries: 0,
                timing: None,
                worker: None,
            },
            99,
            true,
        );
        let expected_hash = "ab".repeat(32);

        assert_eq!(payload.get("id").and_then(Value::as_u64), Some(99));
        assert_eq!(
            payload.get("method").and_then(Value::as_str),
            Some("submit")
        );
        assert_eq!(
            payload
                .get("params")
                .and_then(|params| params.get("job_id"))
                .and_then(Value::as_str),
            Some("job-1")
        );
        assert_eq!(
            payload
                .get("params")
                .and_then(|params| params.get("nonce"))
                .and_then(Value::as_u64),
            Some(42)
        );
        assert_eq!(
            payload
                .get("params")
                .and_then(|params| params.get("claimed_hash"))
                .and_then(Value::as_str),
            Some(expected_hash.as_str())
        );
        assert_eq!(
            payload
                .get("params")
                .and_then(|params| params.get("miner_version"))
                .and_then(Value::as_str),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(
            payload
                .get("params")
                .and_then(|params| params.get("backend"))
                .and_then(Value::as_str),
            Some("nvidia#1")
        );
    }

    #[test]
    fn send_submit_omits_claimed_hash_for_legacy_pools() {
        let payload = capture_submit_payload(
            PoolSubmit {
                job_id: "job-2".to_string(),
                nonce: 7,
                claimed_hash: Some([0xCD; 32]),
                backend: None,
                retries: 0,
                timing: None,
                worker: None,
            },
            100,
            false,
        );

        let params = payload
            .get("params")
            .expect("submit payload should include params object");
        assert!(
            params.get("claimed_hash").is_none(),
            "legacy submit payload should omit claimed_hash"
        );
    }

    #[test]
    fn decode_login_result_with_negotiated_capabilities() {
        let mut login_confirmed = false;
        let mut pending_submits = HashMap::<u64, PoolSubmit>::new();
        let raw = r#"{"id":1,"status":"ok","result":{"protocol_version":2,"capabilities":["submit_claimed_hash","share_validation_status"],"required_capabilities":["submit_claimed_hash"]}}"#;

        let event = decode_pool_message(raw, &mut login_confirmed, &mut pending_submits)
            .expect("login response should decode");
        assert!(login_confirmed, "login should be marked confirmed");

        match event {
            PoolEvent::LoginAccepted(ack) => {
                assert_eq!(ack.protocol_version, 2);
                assert!(ack.supports(STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH));
                assert_eq!(
                    ack.required_capabilities,
                    vec![STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH.to_string()]
                );
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn decode_login_result_defaults_to_legacy_when_missing() {
        let mut login_confirmed = false;
        let mut pending_submits = HashMap::<u64, PoolSubmit>::new();
        let raw = r#"{"id":1,"status":"ok"}"#;

        let event = decode_pool_message(raw, &mut login_confirmed, &mut pending_submits)
            .expect("legacy login response should decode");
        assert!(login_confirmed, "login should be marked confirmed");

        match event {
            PoolEvent::LoginAccepted(ack) => {
                assert_eq!(ack.protocol_version, 1);
                assert!(ack.capabilities.is_empty());
                assert!(ack.required_capabilities.is_empty());
                assert!(
                    should_send_claimed_hash(&ack),
                    "legacy responses should keep claimed hash enabled"
                );
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn claimed_hash_toggle_respects_negotiation() {
        let legacy = PoolLoginAck {
            protocol_version: 1,
            capabilities: Vec::new(),
            required_capabilities: Vec::new(),
        };
        assert!(should_send_claimed_hash(&legacy));

        let negotiated_without_claimed_hash = PoolLoginAck {
            protocol_version: 2,
            capabilities: vec![STRATUM_CAPABILITY_VALIDATION_STATUS.to_string()],
            required_capabilities: Vec::new(),
        };
        assert!(!should_send_claimed_hash(&negotiated_without_claimed_hash));

        let negotiated_with_claimed_hash = PoolLoginAck {
            protocol_version: 2,
            capabilities: vec![STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH.to_string()],
            required_capabilities: Vec::new(),
        };
        assert!(should_send_claimed_hash(&negotiated_with_claimed_hash));

        let required_claimed_hash = PoolLoginAck {
            protocol_version: 2,
            capabilities: Vec::new(),
            required_capabilities: vec![STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH.to_string()],
        };
        assert!(should_send_claimed_hash(&required_claimed_hash));
    }

    #[test]
    fn decode_submit_ack_extracts_difficulty() {
        let mut login_confirmed = true;
        let mut pending_submits = HashMap::<u64, PoolSubmit>::new();
        pending_submits.insert(
            42,
            PoolSubmit {
                job_id: "job-123".to_string(),
                nonce: 17,
                claimed_hash: None,
                backend: None,
                retries: 0,
                timing: None,
                worker: None,
            },
        );
        let raw = r#"{"id":42,"status":"ok","result":{"accepted":true,"difficulty":128}}"#;

        let event = decode_pool_message(raw, &mut login_confirmed, &mut pending_submits)
            .expect("submit response should decode");
        match event {
            PoolEvent::SubmitAck(ack) => {
                assert!(ack.accepted);
                assert_eq!(ack.job_id, "job-123");
                assert_eq!(ack.nonce, 17);
                assert_eq!(ack.difficulty, Some(128));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn decode_notification_event() {
        let mut login_confirmed = true;
        let mut pending_submits = HashMap::<u64, PoolSubmit>::new();
        let raw = r#"{"method":"notification","params":{"kind":"pool_block_solved","message":"pool solved a block"}}"#;

        let event = decode_pool_message(raw, &mut login_confirmed, &mut pending_submits)
            .expect("notification should decode");
        match event {
            PoolEvent::Notification(notification) => {
                assert_eq!(notification.kind, "pool_block_solved");
                assert_eq!(notification.message, "pool solved a block");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn connection_limit_notice_sets_a_bounded_reconnect_delay() {
        let mut login_confirmed = false;
        let mut pending_submits = HashMap::<u64, PoolSubmit>::new();
        let raw = r#"{"method":"notification","params":{"kind":"connection_limit","message":"pool connection limit reached; reconnecting shortly","retry_after_ms":30000}}"#;
        let Some(PoolEvent::Notification(notification)) =
            decode_pool_message(raw, &mut login_confirmed, &mut pending_submits)
        else {
            panic!("notification should decode");
        };
        assert_eq!(notification.kind, POOL_NOTIFICATION_CONNECTION_LIMIT);
        assert_eq!(notification.retry_after, Some(Duration::from_secs(30)));

        let limit = |delay| ReconnectBackoff::default().after_close(Some(delay), false);
        assert_eq!(limit(Duration::from_secs(30)), Duration::from_secs(30));
        assert!(limit(Duration::ZERO) <= RECONNECT_DELAY.mul_f64(1.25));
        assert_eq!(limit(Duration::from_secs(3600)), MAX_CONNECTION_LIMIT_DELAY);
        assert_eq!(
            ReconnectBackoff::default().after_close(None, true),
            Duration::ZERO
        );
    }

    #[test]
    fn quick_failures_back_off_to_a_minute_and_a_healthy_session_resets() {
        let mut backoff = ReconnectBackoff::default();
        let waits: Vec<Duration> = (0..8).map(|_| backoff.after_close(None, false)).collect();
        let within = |wait: Duration, base: u64| {
            let base = Duration::from_secs(base);
            wait >= base.mul_f64(0.75) && wait <= base.mul_f64(1.25)
        };
        for (wait, base) in waits.iter().zip([2, 4, 8, 16, 32, 60, 60, 60]) {
            assert!(within(*wait, base), "{wait:?} vs {base}s");
        }
        assert_eq!(backoff.after_close(None, true), Duration::ZERO);
        assert!(
            within(backoff.failed(), 2),
            "a healthy session resets the backoff"
        );
    }

    #[test]
    fn decode_notification_string_payload_uses_default_kind() {
        let mut login_confirmed = true;
        let mut pending_submits = HashMap::<u64, PoolSubmit>::new();
        let raw = r#"{"method":"notification","params":"great success"}"#;

        let event = decode_pool_message(raw, &mut login_confirmed, &mut pending_submits)
            .expect("notification should decode");
        match event {
            PoolEvent::Notification(notification) => {
                assert_eq!(notification.kind, "pool_notice");
                assert_eq!(notification.message, "great success");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn decode_submit_ack_without_status_or_accepted_is_rejected() {
        let mut login_confirmed = true;
        let mut pending_submits = HashMap::<u64, PoolSubmit>::new();
        pending_submits.insert(
            9,
            PoolSubmit {
                job_id: "job-ambiguous".to_string(),
                nonce: 88,
                claimed_hash: None,
                backend: None,
                retries: 0,
                timing: None,
                worker: None,
            },
        );
        let raw = r#"{"id":9,"result":{}}"#;

        let event = decode_pool_message(raw, &mut login_confirmed, &mut pending_submits)
            .expect("submit response should decode");
        match event {
            PoolEvent::SubmitAck(ack) => {
                assert!(
                    !ack.accepted,
                    "ambiguous submit ack should be treated as rejected"
                );
                assert_eq!(ack.error.as_deref(), Some("pool rejected share"));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn evaluate_login_ack_rejects_unsupported_required_capability() {
        let ack = PoolLoginAck {
            protocol_version: 2,
            capabilities: vec![STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH.to_string()],
            required_capabilities: vec!["future_capability".to_string()],
        };
        let err = evaluate_login_ack(&ack).expect_err("unsupported capability should fail login");
        assert!(err.contains("unsupported capability"));
        assert!(err.contains("future_capability"));
    }

    #[test]
    fn pool_client_waits_for_login_before_sending_submit() {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("test listener should bind to loopback");
        let addr = listener
            .local_addr()
            .expect("listener should expose local address");

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("server should accept client");
            let mut reader = BufReader::new(
                socket
                    .try_clone()
                    .expect("server socket clone should succeed"),
            );
            reader
                .get_mut()
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set read timeout");

            let mut login_line = String::new();
            reader
                .read_line(&mut login_line)
                .expect("server should read login");
            let login_value: Value =
                serde_json::from_str(login_line.trim()).expect("login payload should decode");
            assert_eq!(
                login_value.get("method").and_then(Value::as_str),
                Some("login")
            );

            // Before login acknowledgement, submit must not be written.
            reader
                .get_mut()
                .set_read_timeout(Some(Duration::from_millis(250)))
                .expect("set short read timeout");
            let mut pre_login_submit = String::new();
            match reader.read_line(&mut pre_login_submit) {
                Err(err) if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
                Ok(_) => panic!(
                    "submit arrived before login ack: {}",
                    pre_login_submit.trim()
                ),
                Err(err) => panic!("unexpected read error before login ack: {err}"),
            }

            let login_ack = serde_json::json!({
                "id": LOGIN_REQUEST_ID,
                "status": "ok",
                "result": {
                    "protocol_version": 2,
                    "capabilities": ["submit_claimed_hash"],
                    "required_capabilities": []
                }
            });
            let mut login_ack_bytes =
                serde_json::to_vec(&login_ack).expect("serialize login ack should succeed");
            login_ack_bytes.push(b'\n');
            socket
                .write_all(&login_ack_bytes)
                .expect("server should write login ack");

            reader
                .get_mut()
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set read timeout for submit");
            let mut submit_line = String::new();
            reader
                .read_line(&mut submit_line)
                .expect("server should read submit after login ack");
            let submit_value: Value =
                serde_json::from_str(submit_line.trim()).expect("submit payload should decode");
            assert_eq!(
                submit_value.get("method").and_then(Value::as_str),
                Some("submit")
            );
        });

        let shutdown = Arc::new(AtomicBool::new(false));
        let client = PoolClient::connect(
            &format!("127.0.0.1:{}", addr.port()),
            "BTestAddress".to_string(),
            "rig01".to_string(),
            Arc::clone(&shutdown),
        )
        .expect("pool client should connect");
        let mut saw_login_accepted = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if let Some(PoolEvent::LoginAccepted(_)) =
                client.recv_event_timeout(Duration::from_millis(200))
            {
                saw_login_accepted = true;
                break;
            }
        }
        assert!(saw_login_accepted, "expected login accepted event");
        client
            .submit_share("job-1".to_string(), 42, Some([0xAA; 32]))
            .expect("submit should enqueue");

        server.join().expect("server thread should finish");
        shutdown.store(true, Ordering::Relaxed);
    }

    #[test]
    fn pool_client_reconnects_after_login_rejection() {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("test listener should bind to loopback");
        listener
            .set_nonblocking(true)
            .expect("listener should allow nonblocking accept");
        let addr = listener
            .local_addr()
            .expect("listener should expose local address");

        let server = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(8);
            let accept_next = || loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(err) if err.kind() == ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "timed out waiting for client connection"
                        );
                        thread::sleep(Duration::from_millis(20));
                    }
                    Err(err) => panic!("accept failed: {err}"),
                }
            };

            let mut first = accept_next();
            let mut first_reader = BufReader::new(
                first
                    .try_clone()
                    .expect("first socket clone should succeed"),
            );
            first_reader
                .get_mut()
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set read timeout");
            let mut first_login = String::new();
            first_reader
                .read_line(&mut first_login)
                .expect("server should read first login");
            let first_login_value: Value =
                serde_json::from_str(first_login.trim()).expect("first login should decode");
            assert_eq!(
                first_login_value.get("method").and_then(Value::as_str),
                Some("login")
            );
            first
                .write_all(br#"{"id":1,"error":"bad login"}"#)
                .expect("server should write login rejection");
            first
                .write_all(b"\n")
                .expect("server should terminate line");
            drop(first);

            let mut second = accept_next();
            let mut second_reader = BufReader::new(
                second
                    .try_clone()
                    .expect("second socket clone should succeed"),
            );
            second_reader
                .get_mut()
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set read timeout");
            let mut second_login = String::new();
            second_reader
                .read_line(&mut second_login)
                .expect("server should read reconnect login");
            let second_login_value: Value =
                serde_json::from_str(second_login.trim()).expect("second login should decode");
            assert_eq!(
                second_login_value.get("method").and_then(Value::as_str),
                Some("login")
            );
            second
                .write_all(br#"{"id":1,"status":"ok","result":{"protocol_version":2,"capabilities":["submit_claimed_hash"],"required_capabilities":[]}}"#)
                .expect("server should write second login ack");
            second
                .write_all(b"\n")
                .expect("server should terminate line");
        });

        let shutdown = Arc::new(AtomicBool::new(false));
        let _client = PoolClient::connect(
            &format!("127.0.0.1:{}", addr.port()),
            "BTestAddress".to_string(),
            "rig01".to_string(),
            Arc::clone(&shutdown),
        )
        .expect("pool client should connect");

        server.join().expect("server thread should finish");
        shutdown.store(true, Ordering::Relaxed);
    }

    #[test]
    fn pool_client_rejects_queued_submit_when_login_is_rejected() {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("test listener should bind to loopback");
        let addr = listener
            .local_addr()
            .expect("listener should expose local address");

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("server should accept client");
            let mut reader = BufReader::new(
                socket
                    .try_clone()
                    .expect("server socket clone should succeed"),
            );
            reader
                .get_mut()
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set read timeout");
            let mut login_line = String::new();
            reader
                .read_line(&mut login_line)
                .expect("server should read login");
            let login_value: Value =
                serde_json::from_str(login_line.trim()).expect("login payload should decode");
            assert_eq!(
                login_value.get("method").and_then(Value::as_str),
                Some("login")
            );
            thread::sleep(Duration::from_millis(200));

            socket
                .write_all(br#"{"id":1,"error":"rejected"}"#)
                .expect("server should write rejection");
            socket
                .write_all(b"\n")
                .expect("server should terminate line");
        });

        let shutdown = Arc::new(AtomicBool::new(false));
        let client = PoolClient::connect(
            &format!("127.0.0.1:{}", addr.port()),
            "BTestAddress".to_string(),
            "rig01".to_string(),
            Arc::clone(&shutdown),
        )
        .expect("pool client should connect");
        let mut saw_connected = false;
        let connect_deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < connect_deadline {
            if let Some(PoolEvent::Connected) =
                client.recv_event_timeout(Duration::from_millis(200))
            {
                saw_connected = true;
                break;
            }
        }
        assert!(saw_connected, "expected connected event before submit");
        client
            .submit_share("job-queued".to_string(), 7, None)
            .expect("submit should enqueue");

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut saw_rejected_submit = false;
        while Instant::now() < deadline {
            if let Some(event) = client.recv_event_timeout(Duration::from_millis(200)) {
                if let PoolEvent::SubmitAck(ack) = event {
                    assert!(!ack.accepted, "queued submit should be rejected");
                    let message = ack.error.unwrap_or_default();
                    assert!(
                        message.contains("login rejected") || message.contains("disconnected"),
                        "expected login/disconnect rejection reason, got: {message}"
                    );
                    saw_rejected_submit = true;
                    break;
                }
            }
        }

        server.join().expect("server thread should finish");
        shutdown.store(true, Ordering::Relaxed);
        assert!(saw_rejected_submit, "expected rejected submit ack event");
    }

    #[test]
    fn parse_pool_endpoint_rejects_websocket_schemes() {
        let ws = parse_pool_endpoint("stratum+ws://pool.example.com:3334")
            .expect_err("ws endpoints should be rejected");
        assert!(ws.to_string().contains("no longer supported"));

        let wss = parse_pool_endpoint("stratum+wss://pool.example.com:443")
            .expect_err("wss endpoints should be rejected");
        assert!(wss.to_string().contains("no longer supported"));
    }

    #[test]
    fn pool_urls_accept_tls_schemes_and_failover_lists() {
        assert_eq!(
            normalize_pool_urls(
                "bntpool.com:3333, stratum+ssl://1.2.3.4:3443,stratum+tls://pool.example.com:443/,ssl://[::1]:9,tcp://h:1"
            )
            .unwrap(),
            "stratum+tcp://bntpool.com:3333,stratum+ssl://1.2.3.4:3443,stratum+ssl://pool.example.com:443,stratum+ssl://[::1]:9,stratum+tcp://h:1"
        );
        let endpoints =
            parse_pool_endpoints("stratum+ssl://[::1]:9,pool.example.com:3333").unwrap();
        assert!(endpoints[0].tls);
        assert_eq!(endpoints[0].host, "::1");
        assert_eq!(endpoints[0].authority, "[::1]:9");
        assert!(!endpoints[1].tls);
        assert_eq!(
            primary_pool_url("stratum+ssl://a:1,stratum+tcp://b:2"),
            "stratum+ssl://a:1"
        );
        assert!(normalize_pool_urls(" , ").is_err());
        assert!(normalize_pool_urls("pool.example.com").is_err());
        assert!(normalize_pool_urls("http://pool.example.com:80").is_err());
    }

    #[test]
    fn failover_moves_on_after_failed_attempts_and_stays_while_logged_in() {
        let mut failover = PoolFailover::new(2);
        assert!(!failover.session_ended(false));
        assert_eq!(failover.active, 0);
        assert!(failover.session_ended(false));
        assert_eq!(failover.active, 1);
        // A logged-in backup session that drops reconnects to the backup.
        assert!(!failover.session_ended(true));
        assert!(!failover.session_ended(false));
        assert_eq!(failover.active, 1);
        assert!(failover.session_ended(false));
        assert_eq!(failover.active, 0);

        let mut single = PoolFailover::new(1);
        for _ in 0..5 {
            assert!(!single.session_ended(false));
        }
        assert_eq!(single.active, 0);
    }

    #[test]
    fn failover_backs_off_primary_checks_when_primary_will_not_log_in() {
        let mut failover = PoolFailover::new(2);
        failover.session_ended(false);
        failover.session_ended(false);
        assert_eq!(failover.active, 1);
        failover.switch_to_primary();
        failover.session_ended(false);
        assert!(failover.session_ended(false));
        assert_eq!(failover.active, 1);
        assert_eq!(failover.primary_recheck, PRIMARY_RECHECK_INTERVAL * 2);

        failover.switch_to_primary();
        assert!(!failover.session_ended(true));
        assert_eq!(failover.primary_recheck, PRIMARY_RECHECK_INTERVAL);
    }

    #[test]
    fn tls_errors_get_actionable_messages() {
        let endpoint = parse_pool_endpoint("stratum+ssl://pool.example.com:3443").unwrap();
        let cert = friendly_pool_connect_error(
            &endpoint,
            "TLS handshake failed with pool at x: invalid peer certificate: UnknownIssuer",
        );
        assert!(cert.contains("certificate not trusted"));
        let plain = friendly_pool_connect_error(
            &endpoint,
            "TLS handshake failed with pool at x: received corrupt message",
        );
        assert!(plain.contains("check that this port uses TLS"));
    }

    #[test]
    fn pool_client_fails_over_to_backup_pool() {
        let _test_lock = crate::diagnostics::TEST_LOCK.lock().unwrap();
        let _guard = crate::diagnostics::install(crate::diagnostics::test_context());
        let dead = TcpListener::bind("127.0.0.1:0").expect("bind dead port");
        let dead_port = dead.local_addr().unwrap().port();
        drop(dead);
        let backup = TcpListener::bind("127.0.0.1:0").expect("bind backup");
        let backup_port = backup.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = backup.accept().expect("accept backup");
            let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            std::io::BufRead::read_line(&mut reader, &mut line).unwrap();
            let login: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(login["method"], "login");
            stream
                .write_all(b"{\"id\":1,\"status\":\"ok\",\"result\":{\"protocol_version\":2}}\n")
                .unwrap();
            // Hold the session until the client shuts down and closes it.
            let mut rest = Vec::new();
            let _ = std::io::Read::read_to_end(&mut reader, &mut rest);
        });

        let shutdown = Arc::new(AtomicBool::new(false));
        let client = PoolClient::connect(
            &format!("127.0.0.1:{dead_port},127.0.0.1:{backup_port}"),
            "addr".into(),
            "worker".into(),
            Arc::clone(&shutdown),
        )
        .expect("client");
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut saw_failover = false;
        let mut logged_in = false;
        while Instant::now() < deadline && !logged_in {
            match client.recv_event_timeout(Duration::from_millis(200)) {
                Some(PoolEvent::Notification(note)) if note.kind == POOL_NOTIFICATION_FAILOVER => {
                    assert!(note.message.contains(&backup_port.to_string()));
                    saw_failover = true;
                }
                Some(PoolEvent::LoginAccepted(_)) => logged_in = true,
                _ => {}
            }
        }
        shutdown.store(true, Ordering::Relaxed);
        drop(client);
        server.join().unwrap();
        assert!(saw_failover, "expected a failover notice");
        assert!(logged_in, "expected login on the backup pool");
    }

    /// A fake SOCKS5 proxy that checks the handshake, then answers as the
    /// pool itself.
    fn spawn_fake_socks_pool(
        expect_auth: Option<(&'static str, &'static str)>,
    ) -> (u16, std::thread::JoinHandle<(String, u16)>) {
        use std::io::Read;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut head = [0u8; 2];
            stream.read_exact(&mut head).unwrap();
            assert_eq!(head[0], 5);
            let mut methods = vec![0u8; head[1] as usize];
            stream.read_exact(&mut methods).unwrap();
            match expect_auth {
                Some((user, pass)) => {
                    assert!(methods.contains(&2));
                    stream.write_all(&[5, 2]).unwrap();
                    let mut ver_len = [0u8; 2];
                    stream.read_exact(&mut ver_len).unwrap();
                    let mut got_user = vec![0u8; ver_len[1] as usize];
                    stream.read_exact(&mut got_user).unwrap();
                    let mut plen = [0u8; 1];
                    stream.read_exact(&mut plen).unwrap();
                    let mut got_pass = vec![0u8; plen[0] as usize];
                    stream.read_exact(&mut got_pass).unwrap();
                    assert_eq!(got_user, user.as_bytes());
                    assert_eq!(got_pass, pass.as_bytes());
                    stream.write_all(&[1, 0]).unwrap();
                }
                None => stream.write_all(&[5, 0]).unwrap(),
            }
            let mut req = [0u8; 5];
            stream.read_exact(&mut req).unwrap();
            assert_eq!(
                &req[..4],
                &[5, 1, 0, 3],
                "pool hostname should go to the proxy unresolved"
            );
            let mut host = vec![0u8; req[4] as usize];
            stream.read_exact(&mut host).unwrap();
            let mut port = [0u8; 2];
            stream.read_exact(&mut port).unwrap();
            stream.write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0]).unwrap();

            let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            std::io::BufRead::read_line(&mut reader, &mut line).unwrap();
            let login: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(login["method"], "login");
            stream
                .write_all(b"{\"id\":1,\"status\":\"ok\",\"result\":{\"protocol_version\":2}}\n")
                .unwrap();
            let mut rest = Vec::new();
            let _ = reader.read_to_end(&mut rest);
            (String::from_utf8(host).unwrap(), u16::from_be_bytes(port))
        });
        (port, handle)
    }

    fn login_through_proxy(proxy: &str) {
        let proxy = SocksProxy::parse(proxy).unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let client = PoolClient::connect_via(
            "stratum+tcp://pool.invalid:3333",
            Some(&proxy),
            "addr".into(),
            "worker".into(),
            Arc::clone(&shutdown),
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut logged_in = false;
        while Instant::now() < deadline && !logged_in {
            if let Some(PoolEvent::LoginAccepted(_)) =
                client.recv_event_timeout(Duration::from_millis(200))
            {
                logged_in = true;
            }
        }
        shutdown.store(true, Ordering::Relaxed);
        drop(client);
        assert!(logged_in, "expected login through the proxy");
    }

    #[test]
    fn pool_client_logs_in_through_socks5_proxy() {
        let (port, server) = spawn_fake_socks_pool(None);
        login_through_proxy(&format!("socks5://127.0.0.1:{port}"));
        assert_eq!(server.join().unwrap(), ("pool.invalid".to_string(), 3333));
    }

    #[test]
    fn pool_client_authenticates_to_socks5_proxy() {
        let (port, server) = spawn_fake_socks_pool(Some(("rig", "pw")));
        login_through_proxy(&format!("socks5://rig:pw@127.0.0.1:{port}"));
        assert_eq!(server.join().unwrap(), ("pool.invalid".to_string(), 3333));
    }

    #[test]
    fn parse_pool_endpoint_rejects_paths() {
        let err = parse_pool_endpoint("stratum+tcp://pool.example.com:3334/miner")
            .expect_err("path-based pool endpoints are out of scope");
        assert!(err.to_string().contains("paths are not supported"));
    }

    #[test]
    fn pool_client_reuses_difficulty_hint_after_reconnect() {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("test listener should bind to loopback");
        listener
            .set_nonblocking(true)
            .expect("listener should allow nonblocking accept");
        let addr = listener
            .local_addr()
            .expect("listener should expose local address");

        let server = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(12);
            let accept_next = || loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(err) if err.kind() == ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "timed out waiting for client connection"
                        );
                        thread::sleep(Duration::from_millis(20));
                    }
                    Err(err) => panic!("accept failed: {err}"),
                }
            };

            let mut first = accept_next();
            let mut first_reader = BufReader::new(
                first
                    .try_clone()
                    .expect("first socket clone should succeed"),
            );
            first_reader
                .get_mut()
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set read timeout");
            let mut first_login = String::new();
            first_reader
                .read_line(&mut first_login)
                .expect("server should read first login");
            let first_login_value: Value =
                serde_json::from_str(first_login.trim()).expect("first login should decode");
            assert_eq!(
                first_login_value
                    .pointer("/params/difficulty_hint")
                    .and_then(Value::as_u64),
                None
            );

            let first_ack = serde_json::json!({
                "id": LOGIN_REQUEST_ID,
                "status": "ok",
                "result": {
                    "protocol_version": 2,
                    "capabilities": [
                        STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH,
                        STRATUM_CAPABILITY_DIFFICULTY_HINT
                    ],
                    "required_capabilities": []
                }
            });
            let mut first_ack_bytes =
                serde_json::to_vec(&first_ack).expect("serialize login ack should succeed");
            first_ack_bytes.push(b'\n');
            first
                .write_all(&first_ack_bytes)
                .expect("server should write first login ack");

            let job_notify = serde_json::json!({
                "method": "job",
                "params": {
                    "job_id": "job-1",
                    "header_base": "00",
                    "target": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                    "difficulty": 321,
                    "height": 1,
                    "nonce_start": 0,
                    "nonce_end": 10
                }
            });
            let mut job_notify_bytes =
                serde_json::to_vec(&job_notify).expect("serialize job should succeed");
            job_notify_bytes.push(b'\n');
            first
                .write_all(&job_notify_bytes)
                .expect("server should send job notify");
            thread::sleep(Duration::from_millis(250));
            drop(first_reader);
            drop(first);

            let mut second = accept_next();
            let mut second_reader = BufReader::new(
                second
                    .try_clone()
                    .expect("second socket clone should succeed"),
            );
            second_reader
                .get_mut()
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set read timeout");
            let mut second_login = String::new();
            second_reader
                .read_line(&mut second_login)
                .expect("server should read reconnect login");
            let second_login_value: Value =
                serde_json::from_str(second_login.trim()).expect("second login should decode");
            assert_eq!(
                second_login_value
                    .pointer("/params/difficulty_hint")
                    .and_then(Value::as_u64),
                Some(321)
            );

            let second_ack = serde_json::json!({
                "id": LOGIN_REQUEST_ID,
                "status": "ok",
                "result": {
                    "protocol_version": 2,
                    "capabilities": [STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH],
                    "required_capabilities": []
                }
            });
            let mut second_ack_bytes =
                serde_json::to_vec(&second_ack).expect("serialize login ack should succeed");
            second_ack_bytes.push(b'\n');
            second
                .write_all(&second_ack_bytes)
                .expect("server should write second login ack");
        });

        let shutdown = Arc::new(AtomicBool::new(false));
        let _client = PoolClient::connect(
            &format!("127.0.0.1:{}", addr.port()),
            "BTestAddress".to_string(),
            "rig01".to_string(),
            Arc::clone(&shutdown),
        )
        .expect("pool client should connect");

        server.join().expect("server thread should finish");
        shutdown.store(true, Ordering::Relaxed);
    }

    #[test]
    fn newer_job_cancels_a_delayed_nonce_refill() {
        let request = Mutex::new(Some("old".to_string()));
        assert!(!take_ready_nonce_refill(&request, Some("old"), false));
        assert_eq!(request.lock().unwrap().as_deref(), Some("old"));
        assert!(!take_ready_nonce_refill(&request, Some("new"), true));
        assert!(request.lock().unwrap().is_none());
    }

    /// A pool advertising `nonce_refill_v1` answers `refill` on the same
    /// session; `refuse` makes it reply with an error instead.
    fn run_refill_pool(refuse: bool) -> (Vec<PoolEvent>, usize) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let mut logins = 0;
            listener.set_nonblocking(true).unwrap();
            let deadline = Instant::now() + Duration::from_secs(6);
            while Instant::now() < deadline {
                let socket = match listener.accept() {
                    Ok((socket, _)) => socket,
                    Err(err) if err.kind() == ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                    Err(err) => panic!("accept: {err}"),
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_millis(300)))
                    .unwrap();
                let mut socket = socket;
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut line = String::new();
                while Instant::now() < deadline {
                    line.clear();
                    match reader.read_line(&mut line) {
                        Ok(0) => break,
                        Ok(_) => {}
                        Err(_) => continue,
                    }
                    let Ok(msg) = serde_json::from_str::<Value>(line.trim()) else {
                        continue;
                    };
                    match msg["method"].as_str() {
                        Some("login") => {
                            logins += 1;
                            writeln!(
                                socket,
                                "{}",
                                serde_json::json!({"id": 1, "status": "ok", "result": {
                                "protocol_version": 2,
                                "capabilities": ["submit_claimed_hash", "nonce_refill_v1"]}})
                            )
                            .unwrap();
                            writeln!(socket, "{}", serde_json::json!({"method": "job", "params": {
                                "job_id": "job", "template_id": "t", "header_base": "11".repeat(92),
                                "target": "ff".repeat(32), "height": 1, "nonce_start": 0, "nonce_end": 1000}})).unwrap();
                        }
                        Some("refill") => {
                            assert_eq!(msg["params"]["job_id"], "job");
                            let reply = if refuse {
                                serde_json::json!({"id": msg["id"], "error": "refill limit reached"})
                            } else {
                                serde_json::json!({"id": msg["id"], "status": "ok", "result": {
                                    "job_id": "job-2", "template_id": "t", "header_base": "11".repeat(92),
                                    "target": "ff".repeat(32), "height": 1,
                                    "nonce_start": 5000, "nonce_end": 6000}})
                            };
                            writeln!(socket, "{reply}").unwrap();
                        }
                        _ => {}
                    }
                }
                if logins >= 2 {
                    break;
                }
            }
            logins
        });
        let shutdown = Arc::new(AtomicBool::new(false));
        let client = PoolClient::connect(
            &address.to_string(),
            "test".into(),
            "rig".into(),
            Arc::clone(&shutdown),
        )
        .unwrap();
        let mut events = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut requested = false;
        while Instant::now() < deadline {
            let Some(event) = client.recv_event_timeout(Duration::from_millis(100)) else {
                continue;
            };
            if !requested && matches!(&event, PoolEvent::Job(job) if job.job_id == "job") {
                client.request_nonce_refill("job");
                requested = true;
            }
            let done = matches!(&event, PoolEvent::Job(job) if job.job_id == "job-2")
                || matches!(&event, PoolEvent::LoginAccepted(_))
                    && events
                        .iter()
                        .any(|e| matches!(e, PoolEvent::LoginAccepted(_)));
            events.push(event);
            if done {
                break;
            }
        }
        shutdown.store(true, Ordering::Release);
        drop(client);
        (events, server.join().unwrap())
    }

    #[test]
    fn nonce_refill_uses_pool_refill_without_reconnecting() {
        let (events, logins) = run_refill_pool(false);
        let refill = events.iter().find_map(|event| match event {
            PoolEvent::Job(job) if job.job_id == "job-2" => Some(job),
            _ => None,
        });
        let refill = refill.expect("refill job delivered");
        assert_eq!((refill.nonce_start, refill.nonce_end), (5000, 6000));
        assert_eq!(logins, 1, "refill must not reconnect");
        assert!(!events
            .iter()
            .any(|e| matches!(e, PoolEvent::Disconnected(_))));
    }

    #[test]
    fn refused_nonce_refill_falls_back_to_a_fresh_session() {
        let (events, logins) = run_refill_pool(true);
        assert_eq!(logins, 2, "a refused refill reconnects for new work");
        assert!(events
            .iter()
            .any(|e| matches!(e, PoolEvent::Disconnected(_))));
    }

    #[test]
    fn nonce_refill_waits_for_queued_submit_and_delayed_ack() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let server =
            thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(8);
                let accept = || loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(err) if err.kind() == ErrorKind::WouldBlock => {
                            assert!(Instant::now() < deadline, "refill never reconnected");
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(err) => panic!("accept: {err}"),
                    }
                };
                for attempt in 0..2 {
                    let mut socket = accept();
                    socket.set_nonblocking(false).unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    let mut reader = BufReader::new(socket.try_clone().unwrap());
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    let login: Value = serde_json::from_str(line.trim()).unwrap();
                    assert_eq!(login["method"], "login");
                    writeln!(
                        socket,
                        "{}",
                        serde_json::json!({
                            "id": 1, "status": "ok", "result": {
                                "protocol_version": 2, "capabilities": ["submit_claimed_hash"]
                            }
                        })
                    )
                    .unwrap();
                    writeln!(socket, "{}", serde_json::json!({
                    "method": "job", "params": {"job_id": "job", "header_base": "11".repeat(92),
                    "target": "ff".repeat(32), "height": 1, "nonce_start": 0, "nonce_end": 1000}
                })).unwrap();
                    if attempt == 0 {
                        line.clear();
                        assert!(
                            reader.read_line(&mut line).unwrap() > 0,
                            "refill dropped queued share"
                        );
                        let submit: Value = serde_json::from_str(line.trim()).unwrap();
                        assert_eq!(submit["method"], "submit");
                        assert_eq!(submit["params"]["nonce"], 7);
                        reader
                            .get_mut()
                            .set_read_timeout(Some(Duration::from_millis(150)))
                            .unwrap();
                        let mut probe = [0; 1];
                        match reader.read(&mut probe) {
                            Err(err)
                                if matches!(
                                    err.kind(),
                                    ErrorKind::WouldBlock | ErrorKind::TimedOut
                                ) => {}
                            other => panic!("refill disconnected before ACK: {other:?}"),
                        }
                        writeln!(
                            socket,
                            "{}",
                            serde_json::json!({
                                "id": submit["id"], "status": "ok", "result": {"accepted": true}
                            })
                        )
                        .unwrap();
                        reader
                            .get_mut()
                            .set_read_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        assert_eq!(
                            reader.read(&mut probe).unwrap(),
                            0,
                            "expected refill disconnect after ACK"
                        );
                    }
                }
            });
        let shutdown = Arc::new(AtomicBool::new(false));
        let client = PoolClient::connect(
            &address.to_string(),
            "test".into(),
            "rig".into(),
            Arc::clone(&shutdown),
        )
        .unwrap();
        let login_deadline = Instant::now() + Duration::from_secs(3);
        let mut logins = 0;
        while Instant::now() < login_deadline && logins == 0 {
            if matches!(
                client.recv_event_timeout(Duration::from_millis(100)),
                Some(PoolEvent::LoginAccepted(_))
            ) {
                logins = 1;
            }
        }
        assert_eq!(logins, 1);
        // Queue the share before requesting refill; it must receive its ACK first.
        client.submit_share("job".into(), 7, Some([0; 32])).unwrap();
        client.request_nonce_refill("job");
        let deadline = Instant::now() + Duration::from_secs(8);
        let mut accepted = false;
        while Instant::now() < deadline && logins < 2 {
            match client.recv_event_timeout(Duration::from_millis(100)) {
                Some(PoolEvent::LoginAccepted(_)) => logins += 1,
                Some(PoolEvent::SubmitAck(ack)) => {
                    assert!(
                        ack.accepted,
                        "refill rejected a pending share: {:?}",
                        ack.error
                    );
                    accepted = true;
                }
                _ => {}
            }
        }
        shutdown.store(true, Ordering::Release);
        server.join().unwrap();
        assert!(accepted);
        assert_eq!(logins, 2);
    }

    #[test]
    fn pool_client_request_reconnect_establishes_a_fresh_session() {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("test listener should bind to loopback");
        listener
            .set_nonblocking(true)
            .expect("listener should allow nonblocking accept");
        let addr = listener
            .local_addr()
            .expect("listener should expose local address");

        let server = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(10);
            let accept_next = || loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(err) if err.kind() == ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "timed out waiting for client connection"
                        );
                        thread::sleep(Duration::from_millis(20));
                    }
                    Err(err) => panic!("accept failed: {err}"),
                }
            };

            for attempt in 0..2 {
                let mut socket = accept_next();
                // Windows accepts can inherit the listener's nonblocking mode. This server
                // side of the test uses blocking reads with timeouts, so normalize it
                // explicitly instead of depending on platform-specific inheritance.
                socket
                    .set_nonblocking(false)
                    .expect("accepted test socket should allow blocking mode");
                let mut reader =
                    BufReader::new(socket.try_clone().expect("socket clone should succeed"));
                reader
                    .get_mut()
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("set read timeout");
                let mut login_line = String::new();
                reader
                    .read_line(&mut login_line)
                    .expect("server should read login");
                let login_value: Value =
                    serde_json::from_str(login_line.trim()).expect("login should decode");
                assert_eq!(
                    login_value.get("method").and_then(Value::as_str),
                    Some("login")
                );

                socket
                    .write_all(br#"{"id":1,"status":"ok","result":{"protocol_version":2,"capabilities":["submit_claimed_hash"],"required_capabilities":[]}}
"#)
                    .expect("server should write login ack");

                if attempt == 0 {
                    reader
                        .get_mut()
                        .set_read_timeout(Some(Duration::from_millis(200)))
                        .expect("set drain timeout");
                    let disconnect_deadline = Instant::now() + Duration::from_secs(4);
                    let mut drain = [0u8; 64];
                    loop {
                        match reader.get_mut().read(&mut drain) {
                            Ok(0) => break,
                            Ok(_) => {}
                            Err(err)
                                if matches!(
                                    err.kind(),
                                    ErrorKind::WouldBlock
                                        | ErrorKind::TimedOut
                                        | ErrorKind::Interrupted
                                ) =>
                            {
                                assert!(
                                    Instant::now() < disconnect_deadline,
                                    "timed out waiting for client reconnect request"
                                );
                            }
                            Err(err) => panic!("server should observe client disconnect: {err}"),
                        }
                    }
                }
            }
        });

        let shutdown = Arc::new(AtomicBool::new(false));
        let client = PoolClient::connect(
            &format!("127.0.0.1:{}", addr.port()),
            "BTestAddress".to_string(),
            "rig01".to_string(),
            Arc::clone(&shutdown),
        )
        .expect("pool client should connect");

        let mut login_acks = 0usize;
        let first_deadline = Instant::now() + Duration::from_secs(8);
        while Instant::now() < first_deadline && login_acks < 1 {
            if matches!(
                client.recv_event_timeout(Duration::from_millis(200)),
                Some(PoolEvent::LoginAccepted(_))
            ) {
                login_acks += 1;
            }
        }
        assert_eq!(login_acks, 1, "expected initial login accepted event");

        client.request_reconnect();

        let second_deadline = Instant::now() + Duration::from_secs(8);
        while Instant::now() < second_deadline && login_acks < 2 {
            if matches!(
                client.recv_event_timeout(Duration::from_millis(200)),
                Some(PoolEvent::LoginAccepted(_))
            ) {
                login_acks += 1;
            }
        }

        server.join().expect("server thread should finish");
        shutdown.store(true, Ordering::Relaxed);
        assert_eq!(login_acks, 2, "expected reconnect login accepted event");
    }

    enum ScriptedReply {
        Error(&'static str),
        Accept,
        ErrorThenNewBlock(&'static str),
        ErrorThenClose(&'static str),
    }

    /// Fake pool: logs the client in, sends one job, then answers each submit
    /// with the next scripted reply. Returns every submit payload it read.
    fn spawn_scripted_pool(
        replies: Vec<ScriptedReply>,
        linger_for_extra_submit: Duration,
    ) -> (u16, thread::JoinHandle<Vec<Value>>) {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("test listener should bind to loopback");
        let port = listener.local_addr().expect("listener address").port();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("server should accept client");
            let mut reader = BufReader::new(socket.try_clone().expect("clone socket"));
            reader
                .get_mut()
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            let write_line = |socket: &mut TcpStream, value: Value| {
                let mut bytes = serde_json::to_vec(&value).expect("serialize reply");
                bytes.push(b'\n');
                socket.write_all(&bytes).expect("write reply");
            };
            let mut line = String::new();
            reader.read_line(&mut line).expect("read login");
            write_line(
                &mut socket,
                serde_json::json!({
                    "id": LOGIN_REQUEST_ID,
                    "status": "ok",
                    "result": {
                        "protocol_version": 2,
                        "capabilities": ["submit_claimed_hash", "same_template_rebind_v1"],
                        "required_capabilities": []
                    }
                }),
            );
            let job = |job_id: &str, height: u64| {
                serde_json::json!({
                    "method": "job",
                    "params": {
                        "job_id": job_id,
                        "template_id": format!("tpl-{height}"),
                        "header_base": "00".repeat(92),
                        "target": "ff".repeat(32),
                        "difficulty": 1,
                        "network_target": "00".repeat(32),
                        "height": height,
                        "nonce_start": 0,
                        "nonce_end": 999
                    }
                })
            };
            write_line(&mut socket, job("job-1", 10));

            let mut submits = Vec::new();
            for reply in replies {
                line.clear();
                reader.read_line(&mut line).expect("read submit");
                let submit: Value = serde_json::from_str(line.trim()).expect("submit json");
                let id = submit.get("id").and_then(Value::as_u64).expect("submit id");
                submits.push(submit);
                match reply {
                    ScriptedReply::Accept => write_line(
                        &mut socket,
                        serde_json::json!({"id": id, "status": "ok", "result": {"accepted": true}}),
                    ),
                    ScriptedReply::Error(error) => {
                        write_line(&mut socket, serde_json::json!({"id": id, "error": error}))
                    }
                    ScriptedReply::ErrorThenNewBlock(error) => {
                        write_line(&mut socket, serde_json::json!({"id": id, "error": error}));
                        write_line(&mut socket, job("job-2", 11));
                    }
                    ScriptedReply::ErrorThenClose(error) => {
                        write_line(&mut socket, serde_json::json!({"id": id, "error": error}));
                        let _ = socket.shutdown(std::net::Shutdown::Both);
                        return submits;
                    }
                }
            }
            if !linger_for_extra_submit.is_zero() {
                reader
                    .get_mut()
                    .set_read_timeout(Some(linger_for_extra_submit))
                    .expect("set linger timeout");
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) => {}
                    Ok(_) => panic!("unexpected extra submit: {}", line.trim()),
                    Err(err)
                        if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
                    Err(err) => panic!("unexpected read error: {err}"),
                }
            }
            submits
        });
        (port, server)
    }

    fn connect_and_submit(port: u16, nonce: u64) -> (PoolClient, Arc<AtomicBool>) {
        let shutdown = Arc::new(AtomicBool::new(false));
        let client = PoolClient::connect(
            &format!("127.0.0.1:{port}"),
            "BTestAddress".to_string(),
            "rig01".to_string(),
            Arc::clone(&shutdown),
        )
        .expect("pool client should connect");
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut saw_job = false;
        while Instant::now() < deadline && !saw_job {
            if let Some(PoolEvent::Job(_)) = client.recv_event_timeout(Duration::from_millis(100)) {
                saw_job = true;
            }
        }
        assert!(saw_job, "expected job before submitting");
        client
            .submit_share("job-1".to_string(), nonce, Some([0xAB; 32]))
            .expect("submit should enqueue");
        (client, shutdown)
    }

    /// Collects share events until the final ack (or timeout).
    fn wait_for_final_ack(client: &PoolClient) -> (Vec<PoolSubmitRetry>, Option<PoolSubmitAck>) {
        let mut retries = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            match client.recv_event_timeout(Duration::from_millis(100)) {
                Some(PoolEvent::SubmitRetry(retry)) => retries.push(retry),
                Some(PoolEvent::SubmitAck(ack)) => return (retries, Some(ack)),
                _ => {}
            }
        }
        (retries, None)
    }

    #[test]
    fn pool_client_resubmits_busy_reject_with_identical_params() {
        let (port, server) = spawn_scripted_pool(
            vec![
                ScriptedReply::Error("server busy, retry"),
                ScriptedReply::Error("rate limited, retry"),
                ScriptedReply::Accept,
            ],
            Duration::ZERO,
        );
        let (client, shutdown) = connect_and_submit(port, 4242);
        let (retries, ack) = wait_for_final_ack(&client);
        let submits = server.join().expect("server thread should finish");
        shutdown.store(true, Ordering::Relaxed);

        let ack = ack.expect("expected a final submit ack");
        assert!(ack.accepted, "resubmitted share should be accepted");
        assert_eq!(ack.nonce, 4242);
        assert_eq!(ack.retries, 2);
        assert_eq!(
            retries
                .iter()
                .map(|retry| retry.retry_number)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(retries[0].reason, "server busy, retry");
        assert_eq!(retries[1].reason, "rate limited, retry");
        assert!(!retries[0].block_candidate);

        assert_eq!(submits.len(), 3);
        let params = |submit: &Value| submit.get("params").cloned().expect("submit params");
        for submit in &submits[1..] {
            assert_eq!(
                params(submit),
                params(&submits[0]),
                "retry must resend the same params"
            );
            assert_ne!(
                submit.get("id"),
                submits[0].get("id"),
                "retry uses a fresh request id"
            );
        }
    }

    #[test]
    fn pool_client_does_not_retry_duplicate_share_rejects() {
        let (port, server) = spawn_scripted_pool(
            vec![ScriptedReply::Error("duplicate share")],
            Duration::from_millis(800),
        );
        let (client, shutdown) = connect_and_submit(port, 7);
        let (retries, ack) = wait_for_final_ack(&client);
        let submits = server.join().expect("server thread should finish");
        shutdown.store(true, Ordering::Relaxed);

        let ack = ack.expect("expected a final submit ack");
        assert!(!ack.accepted);
        assert_eq!(ack.error.as_deref(), Some("duplicate share"));
        assert_eq!(ack.retries, 0);
        assert!(retries.is_empty());
        assert_eq!(submits.len(), 1);
    }

    #[test]
    fn pool_client_abandons_busy_retry_after_new_block() {
        let (port, server) = spawn_scripted_pool(
            vec![ScriptedReply::ErrorThenNewBlock("server busy, retry")],
            Duration::from_millis(800),
        );
        let (client, shutdown) = connect_and_submit(port, 9);
        let (retries, ack) = wait_for_final_ack(&client);
        let submits = server.join().expect("server thread should finish");
        shutdown.store(true, Ordering::Relaxed);

        assert_eq!(retries.len(), 1);
        let ack = ack.expect("expected a final submit ack");
        assert!(!ack.accepted);
        assert_eq!(ack.retries, 1);
        let reason = ack.error.unwrap_or_default();
        assert!(reason.contains("new block"), "unexpected reason: {reason}");
        assert_eq!(submits.len(), 1, "stale share must not be resubmitted");
    }

    #[test]
    fn pool_client_drops_busy_retry_on_disconnect() {
        let (port, server) = spawn_scripted_pool(
            vec![ScriptedReply::ErrorThenClose("server busy, retry")],
            Duration::ZERO,
        );
        let (client, shutdown) = connect_and_submit(port, 11);
        let (retries, ack) = wait_for_final_ack(&client);
        let submits = server.join().expect("server thread should finish");
        shutdown.store(true, Ordering::Relaxed);

        assert_eq!(retries.len(), 1);
        let ack = ack.expect("expected a final submit ack");
        assert!(!ack.accepted);
        assert_eq!(
            ack.error.as_deref(),
            Some(SUBMIT_REJECT_REASON_DISCONNECTED)
        );
        assert_eq!(ack.retries, 1);
        assert_eq!(submits.len(), 1);
    }
}
