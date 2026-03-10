use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender, TryRecvError};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::net::TcpStream as TokioTcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const LOGIN_REQUEST_ID: u64 = 1;
const RECONNECT_DELAY: Duration = Duration::from_secs(2);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const CHANNEL_CAPACITY: usize = 4096;
const MAX_SUBMITS_PER_IO_CYCLE: usize = 32;
const SUBMIT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const LOCAL_POOL_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const MAX_POOL_MESSAGE_BYTES: usize = 8 * 1024;
const STRATUM_PROTOCOL_VERSION: u32 = 2;
const STRATUM_CAPABILITY_LOGIN_NEGOTIATION: &str = "login_negotiation";
const STRATUM_CAPABILITY_VALIDATION_STATUS: &str = "share_validation_status";
const STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH: &str = "submit_claimed_hash";
const STRATUM_CAPABILITY_DIFFICULTY_HINT: &str = "difficulty_hint";
const STRATUM_METHOD_NOTIFICATION: &str = "notification";
const SUBMIT_REJECT_REASON_DISCONNECTED: &str = "pool disconnected";
const SUBMIT_REJECT_REASON_LOGIN_REJECTED: &str = "pool login rejected";
const SUBMIT_REJECT_REASON_SEND_FAILED: &str = "submit interrupted during reconnect";
const SUBMIT_REJECT_REASON_RESYNC: &str = "submit interrupted during pool resync";

pub const POOL_NOTIFICATION_POOL_BLOCK_SOLVED: &str = "pool_block_solved";
pub const POOL_NOTIFICATION_MINER_BLOCK_FOUND: &str = "miner_block_found";

const CLIENT_CAPABILITIES: &[&str] = &[
    STRATUM_CAPABILITY_LOGIN_NEGOTIATION,
    STRATUM_CAPABILITY_VALIDATION_STATUS,
    STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH,
    STRATUM_CAPABILITY_DIFFICULTY_HINT,
];

#[derive(Debug, Clone, Deserialize)]
pub struct PoolJob {
    pub job_id: String,
    pub header_base: String,
    pub target: String,
    #[serde(default)]
    pub difficulty: Option<u64>,
    pub height: u64,
    pub nonce_start: u64,
    pub nonce_end: u64,
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
}

#[derive(Debug, Clone)]
pub enum PoolEvent {
    Connected,
    Disconnected(String),
    LoginAccepted(PoolLoginAck),
    LoginRejected(String),
    Job(PoolJob),
    SubmitAck(PoolSubmitAck),
    Notification(PoolNotification),
}

#[derive(Debug, Clone)]
struct PoolSubmit {
    job_id: String,
    nonce: u64,
    claimed_hash: Option<[u8; 32]>,
}

#[derive(Debug)]
pub struct PoolClient {
    submit_tx: Sender<PoolSubmit>,
    event_rx: Receiver<PoolEvent>,
    reconnect_requested: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
}

impl PoolClient {
    pub fn connect(
        pool_url: &str,
        address: String,
        worker: String,
        process_shutdown: Arc<AtomicBool>,
    ) -> Result<Self> {
        let endpoint = parse_pool_endpoint(pool_url)?;
        let (submit_tx, submit_rx) = bounded::<PoolSubmit>(CHANNEL_CAPACITY);
        let (event_tx, event_rx) = bounded::<PoolEvent>(CHANNEL_CAPACITY);
        let reconnect_requested = Arc::new(AtomicBool::new(false));
        let reconnect_requested_for_thread = Arc::clone(&reconnect_requested);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_for_thread = Arc::clone(&shutdown);

        std::thread::Builder::new()
            .name("pool-stratum".to_string())
            .spawn(move || {
                run_pool_client_thread(
                    endpoint,
                    address,
                    worker,
                    submit_rx,
                    event_tx,
                    process_shutdown,
                    reconnect_requested_for_thread,
                    shutdown_for_thread,
                )
            })
            .context("failed to spawn pool client thread")?;

        Ok(Self {
            submit_tx,
            event_rx,
            reconnect_requested,
            shutdown,
        })
    }

    pub fn submit_share(
        &self,
        job_id: String,
        nonce: u64,
        claimed_hash: Option<[u8; 32]>,
    ) -> Result<()> {
        self.submit_tx
            .send(PoolSubmit {
                job_id,
                nonce,
                claimed_hash,
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PoolTransport {
    Tcp,
    Ws,
    Wss,
}

#[derive(Debug, Clone)]
struct PoolEndpoint {
    transport: PoolTransport,
    authority: String,
    url: String,
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

    fn ws_connect_url(&self) -> Option<String> {
        match self.transport {
            PoolTransport::Ws => Some(format!("ws://{}", self.authority)),
            PoolTransport::Wss => Some(format!("wss://{}", self.authority)),
            PoolTransport::Tcp => None,
        }
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

fn run_pool_client_thread(
    endpoint: PoolEndpoint,
    address: String,
    worker: String,
    submit_rx: Receiver<PoolSubmit>,
    event_tx: Sender<PoolEvent>,
    process_shutdown: Arc<AtomicBool>,
    reconnect_requested: Arc<AtomicBool>,
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
                endpoint.display()
            )));
            return;
        }
    };

    runtime.block_on(run_pool_client_loop(
        endpoint,
        address,
        worker,
        submit_rx,
        event_tx,
        process_shutdown,
        reconnect_requested,
        shutdown,
    ));
}

async fn run_pool_client_loop(
    endpoint: PoolEndpoint,
    address: String,
    worker: String,
    submit_rx: Receiver<PoolSubmit>,
    event_tx: Sender<PoolEvent>,
    process_shutdown: Arc<AtomicBool>,
    reconnect_requested: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
) {
    let mut next_request_id = LOGIN_REQUEST_ID + 1;
    let mut allow_login_difficulty_hint = false;
    let mut login_difficulty_hint: Option<u64> = None;
    while !should_shutdown(process_shutdown.as_ref(), shutdown.as_ref()) {
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
                let mut login_confirmed = false;
                let mut submit_claimed_hash_enabled = true;
                let mut last_outbound = Instant::now();
                let mut submit_tick = tokio::time::interval(SUBMIT_POLL_INTERVAL);
                submit_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                loop {
                    if should_shutdown(process_shutdown.as_ref(), shutdown.as_ref()) {
                        connection.abort();
                        return;
                    }
                    if reconnect_requested.swap(false, Ordering::Relaxed) {
                        reject_pending_submits(
                            &event_tx,
                            &mut pending_submits,
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

                                for _ in 0..MAX_SUBMITS_PER_IO_CYCLE {
                                    let Ok(submit) = submit_rx.try_recv() else {
                                        break;
                                    };
                                    let request_id = next_request_id;
                                    next_request_id =
                                        next_request_id.wrapping_add(1).max(LOGIN_REQUEST_ID + 1);
                                    let payload = match submit_payload(
                                        request_id,
                                        &submit,
                                        submit_claimed_hash_enabled,
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
                                        reject_pending_submits(
                                            &event_tx,
                                            &mut pending_submits,
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
                                    break;
                                }
                            };

                            let line = match inbound {
                                InboundFrame::Text(line) => line,
                                InboundFrame::ReadError(err) => {
                                    reject_pending_submits(
                                        &event_tx,
                                        &mut pending_submits,
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
                                    break;
                                }
                            };

                            let trimmed = line.trim();
                            if trimmed.is_empty() {
                                continue;
                            }
                            if let Some(mut event) = decode_pool_message(
                                trimmed,
                                &mut login_confirmed,
                                &mut pending_submits,
                            ) {
                                if let PoolEvent::LoginAccepted(ack) = &event {
                                    match evaluate_login_ack(ack) {
                                        Ok(include_claimed_hash) => {
                                            submit_claimed_hash_enabled = include_claimed_hash;
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
                                        if let Some(difficulty) = job.difficulty {
                                            login_difficulty_hint = Some(difficulty.max(1));
                                        }
                                    }
                                    PoolEvent::SubmitAck(ack) => {
                                        if let Some(difficulty) = ack.difficulty {
                                            login_difficulty_hint = Some(difficulty.max(1));
                                        }
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
                                    reject_pending_submits(
                                        &event_tx,
                                        &mut pending_submits,
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
                                        RECONNECT_DELAY,
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }
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
                    RECONNECT_DELAY,
                );
            }
        }
    }
}

async fn connect_pool_transport(endpoint: &PoolEndpoint) -> Result<PoolConnection> {
    match endpoint.transport {
        PoolTransport::Tcp => connect_pool_tcp_transport(endpoint).await,
        PoolTransport::Ws | PoolTransport::Wss => connect_pool_ws_transport(endpoint).await,
    }
}

async fn connect_pool_tcp_transport(endpoint: &PoolEndpoint) -> Result<PoolConnection> {
    let stream = connect_pool_tcp_stream(endpoint).await?;
    let (reader_half, writer_half) = stream.into_split();
    let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
    let reader_task = tokio::spawn(async move {
        run_tcp_reader(TokioBufReader::new(reader_half), inbound_tx).await;
    });
    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
    let writer_task = tokio::spawn(async move {
        run_tcp_writer(writer_half, outbound_rx).await;
    });
    Ok(PoolConnection {
        outbound_tx,
        inbound_rx,
        reader_task,
        writer_task,
    })
}

async fn connect_pool_ws_transport(endpoint: &PoolEndpoint) -> Result<PoolConnection> {
    let Some(url) = endpoint.ws_connect_url() else {
        bail!("websocket pool endpoint missing connect url");
    };
    let (ws_stream, _) = connect_async(url.as_str())
        .await
        .with_context(|| format!("failed to connect to pool websocket {}", endpoint.display()))?;
    let (writer, reader) = ws_stream.split();
    let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
    let reader_task = tokio::spawn(async move {
        run_ws_reader(reader, inbound_tx).await;
    });
    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
    let writer_task = tokio::spawn(async move {
        run_ws_writer(writer, outbound_rx).await;
    });
    Ok(PoolConnection {
        outbound_tx,
        inbound_rx,
        reader_task,
        writer_task,
    })
}

async fn connect_pool_tcp_stream(endpoint: &PoolEndpoint) -> Result<TokioTcpStream> {
    let addrs = resolve_pool_addrs(&endpoint.authority)
        .with_context(|| format!("failed to resolve pool endpoint {}", endpoint.display()))?;
    if addrs.is_empty() {
        bail!("pool endpoint resolved to no addresses: {}", endpoint.display());
    }

    let mut last_err: Option<String> = None;
    for addr in addrs {
        match tokio::time::timeout(CONNECT_TIMEOUT, TokioTcpStream::connect(addr)).await {
            Ok(Ok(stream)) => {
                let _ = stream.set_nodelay(true);
                return Ok(stream);
            }
            Ok(Err(err)) => {
                last_err = Some(err.to_string());
            }
            Err(_) => {
                last_err = Some(format!("connection timed out to {addr}"));
            }
        }
    }

    Err(anyhow!(
        "failed to connect to pool at {}: {}",
        endpoint.display(),
        last_err.unwrap_or_else(|| "no addresses available".to_string())
    ))
}

fn resolve_pool_addrs(authority: &str) -> Result<Vec<SocketAddr>> {
    let mut addrs: Vec<SocketAddr> = authority.to_socket_addrs()?.collect();
    addrs.sort_by_key(|addr| !addr.is_ipv4());
    Ok(addrs)
}

async fn run_tcp_reader(
    mut reader: TokioBufReader<tokio::net::tcp::OwnedReadHalf>,
    inbound_tx: mpsc::UnboundedSender<InboundFrame>,
) {
    loop {
        match read_line_limited_async(&mut reader, MAX_POOL_MESSAGE_BYTES).await {
            Ok(Some(line)) => {
                if inbound_tx.send(InboundFrame::Text(line)).is_err() {
                    break;
                }
            }
            Ok(None) => break,
            Err(err) => {
                let _ = inbound_tx.send(InboundFrame::ReadError(err.to_string()));
                break;
            }
        }
    }
}

async fn run_ws_reader<S>(mut reader: S, inbound_tx: mpsc::UnboundedSender<InboundFrame>)
where
    S: Stream<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    while let Some(message) = reader.next().await {
        match message {
            Ok(Message::Text(text)) => {
                if inbound_tx
                    .send(InboundFrame::Text(text.to_string()))
                    .is_err()
                {
                    break;
                }
            }
            Ok(Message::Binary(_)) => {
                let _ = inbound_tx.send(InboundFrame::ReadError(
                    "binary websocket frames are not supported".to_string(),
                ));
                break;
            }
            Ok(Message::Close(_)) => break,
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
            Ok(_) => {}
            Err(err) => {
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
        if result.is_err() {
            break;
        }
    }
}

async fn run_ws_writer<W>(mut writer: W, mut outbound_rx: mpsc::UnboundedReceiver<ClientOutbound>)
where
    W: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    while let Some(message) = outbound_rx.recv().await {
        let result = match message {
            ClientOutbound::Text(text) => writer.send(Message::Text(text.into())).await,
            ClientOutbound::Keepalive => writer.send(Message::Ping(Vec::new().into())).await,
        };
        if result.is_err() {
            break;
        }
    }
}

async fn read_line_limited_async(
    reader: &mut TokioBufReader<tokio::net::tcp::OwnedReadHalf>,
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
            capabilities: CLIENT_CAPABILITIES,
            difficulty_hint,
        },
    };
    serde_json::to_string(&request).context("failed to serialize stratum login payload")
}

fn submit_payload(
    request_id: u64,
    submit: &PoolSubmit,
    include_claimed_hash: bool,
) -> Result<String> {
    let claimed_hash_hex = if include_claimed_hash {
        submit.claimed_hash.map(hex::encode)
    } else {
        None
    };
    let request = StratumRequest {
        id: request_id,
        method: "submit",
        params: SubmitParams {
            job_id: &submit.job_id,
            nonce: submit.nonce,
            claimed_hash: claimed_hash_hex.as_deref(),
        },
    };
    serde_json::to_string(&request).context("failed to serialize stratum submit payload")
}

fn friendly_pool_connect_error(endpoint: &PoolEndpoint, detail: &str) -> String {
    let lower = detail.to_ascii_lowercase();
    if lower.contains("connection refused") {
        if endpoint.is_local() {
            return format!("pool offline at {}", endpoint.display());
        }
        return format!("pool unreachable at {}", endpoint.display());
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

fn should_enable_idle_keepalive(endpoint: &str) -> bool {
    !endpoint.trim().is_empty()
}

fn send_login(
    stream: &mut TcpStream,
    address: &str,
    worker: &str,
    difficulty_hint: Option<u64>,
) -> Result<()> {
    let payload = login_payload(address, worker, difficulty_hint)?;
    write_text_line(stream, &payload)
}

fn send_submit(
    stream: &mut TcpStream,
    request_id: u64,
    submit: &PoolSubmit,
    include_claimed_hash: bool,
) -> Result<()> {
    let payload = submit_payload(request_id, submit, include_claimed_hash)?;
    write_text_line(stream, &payload)
}

fn write_text_line(stream: &mut TcpStream, payload: &str) -> Result<()> {
    let mut line = payload.as_bytes().to_vec();
    line.push(b'\n');
    stream
        .write_all(&line)
        .context("failed writing stratum payload")
}

fn decode_pool_message(
    raw: &str,
    login_confirmed: &mut bool,
    pending_submits: &mut HashMap<u64, PoolSubmit>,
) -> Option<PoolEvent> {
    let msg: StratumMessage = serde_json::from_str(raw).ok()?;

    if let Some(method) = msg.method.as_deref() {
        if method == "job" {
            let params = msg.params?;
            let job: PoolJob = serde_json::from_value(params).ok()?;
            return Some(PoolEvent::Job(job));
        }
        if method == STRATUM_METHOD_NOTIFICATION {
            let notification = parse_pool_notification(msg.params)?;
            return Some(PoolEvent::Notification(notification));
        }
    }

    let id = msg.id?;
    if id == LOGIN_REQUEST_ID {
        if let Some(err) = msg.error {
            *login_confirmed = false;
            return Some(PoolEvent::LoginRejected(err));
        }
        let status_ok = msg
            .status
            .as_deref()
            .map(|status| status.eq_ignore_ascii_case("ok"))
            .unwrap_or(false);
        if status_ok {
            *login_confirmed = true;
            let ack = parse_login_result(msg.result);
            return Some(PoolEvent::LoginAccepted(ack));
        }
        *login_confirmed = false;
        return Some(PoolEvent::LoginRejected("pool rejected login".to_string()));
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

    Some(PoolEvent::SubmitAck(PoolSubmitAck {
        job_id: submit.job_id,
        nonce: submit.nonce,
        accepted,
        difficulty,
        error,
    }))
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
    Some(PoolNotification {
        kind: kind.to_string(),
        message: message.to_string(),
    })
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
        if !CLIENT_CAPABILITIES
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
    }));
}

fn reject_pending_submits(
    event_tx: &Sender<PoolEvent>,
    pending_submits: &mut HashMap<u64, PoolSubmit>,
    reason: &str,
) {
    for (_, submit) in pending_submits.drain() {
        reject_submit(event_tx, submit, reason);
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

fn parse_pool_endpoint(pool_url: &str) -> Result<PoolEndpoint> {
    let trimmed = pool_url.trim();
    if trimmed.is_empty() {
        bail!("pool URL is empty");
    }

    let (transport, rest) = if let Some(rest) = trimmed.strip_prefix("stratum+tcp://") {
        (PoolTransport::Tcp, rest)
    } else if let Some(rest) = trimmed.strip_prefix("stratum+ws://") {
        (PoolTransport::Ws, rest)
    } else if let Some(rest) = trimmed.strip_prefix("stratum+wss://") {
        (PoolTransport::Wss, rest)
    } else {
        (PoolTransport::Tcp, trimmed)
    };
    let authority = rest.split('/').next().unwrap_or(rest).trim();
    if authority.is_empty() {
        bail!("pool URL authority is empty");
    }
    if authority.contains("://") {
        bail!("unsupported pool URL scheme");
    }
    if rest != authority {
        bail!("pool URL paths are not supported");
    }
    let url = match transport {
        PoolTransport::Tcp => format!("stratum+tcp://{authority}"),
        PoolTransport::Ws => format!("stratum+ws://{authority}"),
        PoolTransport::Wss => format!("stratum+wss://{authority}"),
    };
    Ok(PoolEndpoint {
        transport,
        authority: authority.to_string(),
        url,
    })
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
    use super::*;
    use std::io::{BufRead, BufReader, ErrorKind, Read};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

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
    }

    #[test]
    fn send_submit_omits_claimed_hash_for_legacy_pools() {
        let payload = capture_submit_payload(
            PoolSubmit {
                job_id: "job-2".to_string(),
                nonce: 7,
                claimed_hash: Some([0xCD; 32]),
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
    fn idle_keepalive_is_enabled_for_remote_endpoints() {
        assert!(should_enable_idle_keepalive("127.0.0.1:3333"));
        assert!(should_enable_idle_keepalive("pool.example.com:3333"));
        assert!(!should_enable_idle_keepalive("   "));
    }

    #[test]
    fn parse_pool_endpoint_supports_websocket_schemes() {
        let ws = parse_pool_endpoint("stratum+ws://pool.example.com:3334")
            .expect("ws endpoint should parse");
        assert_eq!(ws.transport, PoolTransport::Ws);
        assert_eq!(ws.authority, "pool.example.com:3334");
        assert_eq!(ws.display(), "stratum+ws://pool.example.com:3334");

        let wss = parse_pool_endpoint("stratum+wss://pool.example.com:443")
            .expect("wss endpoint should parse");
        assert_eq!(wss.transport, PoolTransport::Wss);
        assert_eq!(wss.authority, "pool.example.com:443");
        assert_eq!(wss.display(), "stratum+wss://pool.example.com:443");
    }

    #[test]
    fn parse_pool_endpoint_rejects_paths() {
        let err = parse_pool_endpoint("stratum+ws://pool.example.com:3334/miner")
            .expect_err("path-based websocket endpoints are out of scope");
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
}
