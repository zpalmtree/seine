use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender, TryRecvError};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const LOGIN_REQUEST_ID: u64 = 1;
const RECONNECT_DELAY: Duration = Duration::from_secs(2);
const SOCKET_IO_TIMEOUT: Duration = Duration::from_millis(250);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const CHANNEL_CAPACITY: usize = 4096;
const STRATUM_PROTOCOL_VERSION: u32 = 2;
const STRATUM_CAPABILITY_LOGIN_NEGOTIATION: &str = "login_negotiation";
const STRATUM_CAPABILITY_VALIDATION_STATUS: &str = "share_validation_status";
const STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH: &str = "submit_claimed_hash";

const CLIENT_CAPABILITIES: &[&str] = &[
    STRATUM_CAPABILITY_LOGIN_NEGOTIATION,
    STRATUM_CAPABILITY_VALIDATION_STATUS,
    STRATUM_CAPABILITY_SUBMIT_CLAIMED_HASH,
];

#[derive(Debug, Clone, Deserialize)]
pub struct PoolJob {
    pub job_id: String,
    pub header_base: String,
    pub target: String,
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
pub enum PoolEvent {
    Connected,
    Disconnected(String),
    LoginAccepted(PoolLoginAck),
    LoginRejected(String),
    Job(PoolJob),
    SubmitAck(PoolSubmitAck),
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
}

impl PoolClient {
    pub fn connect(
        pool_url: &str,
        address: String,
        worker: String,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Self> {
        let endpoint = parse_pool_endpoint(pool_url)?;
        let (submit_tx, submit_rx) = bounded::<PoolSubmit>(CHANNEL_CAPACITY);
        let (event_tx, event_rx) = bounded::<PoolEvent>(CHANNEL_CAPACITY);

        std::thread::Builder::new()
            .name("pool-stratum".to_string())
            .spawn(move || {
                run_pool_client_thread(endpoint, address, worker, submit_rx, event_tx, shutdown)
            })
            .context("failed to spawn pool client thread")?;

        Ok(Self {
            submit_tx,
            event_rx,
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

fn run_pool_client_thread(
    endpoint: String,
    address: String,
    worker: String,
    submit_rx: Receiver<PoolSubmit>,
    event_tx: Sender<PoolEvent>,
    shutdown: Arc<AtomicBool>,
) {
    let mut next_request_id = LOGIN_REQUEST_ID + 1;
    while !shutdown.load(Ordering::Relaxed) {
        match connect_pool_stream(&endpoint) {
            Ok((mut stream, mut reader)) => {
                let _ = event_tx.try_send(PoolEvent::Connected);
                if let Err(err) = send_login(&mut stream, &address, &worker) {
                    let _ = event_tx.try_send(PoolEvent::Disconnected(format!(
                        "lost connection while sending pool login; retrying in {}s ({err:#})",
                        RECONNECT_DELAY.as_secs()
                    )));
                    sleep_with_shutdown(&shutdown, RECONNECT_DELAY);
                    continue;
                }
                let mut pending_submits = HashMap::<u64, PoolSubmit>::new();
                let mut login_confirmed = false;
                let mut submit_claimed_hash_enabled = true;

                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        return;
                    }

                    let mut reconnect_requested = false;
                    while let Ok(submit) = submit_rx.try_recv() {
                        let request_id = next_request_id;
                        next_request_id = next_request_id.wrapping_add(1).max(LOGIN_REQUEST_ID + 1);
                        if send_submit(
                            &mut stream,
                            request_id,
                            &submit,
                            submit_claimed_hash_enabled,
                        )
                        .is_err()
                        {
                            let _ = event_tx.try_send(PoolEvent::Disconnected(format!(
                                "lost connection while sending share; reconnecting in {}s",
                                RECONNECT_DELAY.as_secs()
                            )));
                            pending_submits.clear();
                            reconnect_requested = true;
                            break;
                        }
                        pending_submits.insert(request_id, submit);
                    }
                    if reconnect_requested {
                        sleep_with_shutdown(&shutdown, RECONNECT_DELAY);
                        break;
                    }

                    let mut line = String::new();
                    match reader.read_line(&mut line) {
                        Ok(0) => {
                            let _ = event_tx.try_send(PoolEvent::Disconnected(format!(
                                "pool connection closed; reconnecting in {}s",
                                RECONNECT_DELAY.as_secs()
                            )));
                            break;
                        }
                        Ok(_) => {
                            let trimmed = line.trim();
                            if trimmed.is_empty() {
                                continue;
                            }
                            if let Some(event) = decode_pool_message(
                                trimmed,
                                &mut login_confirmed,
                                &mut pending_submits,
                            ) {
                                if let PoolEvent::LoginAccepted(ack) = &event {
                                    submit_claimed_hash_enabled = should_send_claimed_hash(ack);
                                }
                                let _ = event_tx.try_send(event);
                            }
                        }
                        Err(err)
                            if matches!(
                                err.kind(),
                                ErrorKind::WouldBlock
                                    | ErrorKind::TimedOut
                                    | ErrorKind::Interrupted
                            ) => {}
                        Err(err) => {
                            let _ = event_tx.try_send(PoolEvent::Disconnected(format!(
                                "pool connection lost; reconnecting in {}s ({err})",
                                RECONNECT_DELAY.as_secs()
                            )));
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                let detail = format!("{err:#}");
                let _ = event_tx.try_send(PoolEvent::Disconnected(format!(
                    "{} ({detail})",
                    friendly_pool_connect_error(&endpoint, &detail)
                )));
                sleep_with_shutdown(&shutdown, RECONNECT_DELAY);
            }
        }
    }
}

fn friendly_pool_connect_error(endpoint: &str, detail: &str) -> String {
    let lower = detail.to_ascii_lowercase();
    if lower.contains("connection refused") {
        if is_local_pool_endpoint(endpoint) {
            return format!(
                "no local pool found at {endpoint}; start the pool and the miner will retry automatically in {}s",
                RECONNECT_DELAY.as_secs()
            );
        }
        return format!(
            "pool at {endpoint} refused the connection; verify host/port and that the pool is running (retrying in {}s)",
            RECONNECT_DELAY.as_secs()
        );
    }
    if lower.contains("timed out") {
        return format!(
            "pool connection to {endpoint} timed out; check host/port or firewall (retrying in {}s)",
            RECONNECT_DELAY.as_secs()
        );
    }
    if lower.contains("failed to resolve")
        || lower.contains("resolved to no addresses")
        || lower.contains("name or service not known")
    {
        return format!(
            "cannot resolve pool endpoint {endpoint}; check the configured pool address (retrying in {}s)",
            RECONNECT_DELAY.as_secs()
        );
    }
    format!(
        "unable to connect to pool at {endpoint}; retrying in {}s",
        RECONNECT_DELAY.as_secs()
    )
}

fn is_local_pool_endpoint(endpoint: &str) -> bool {
    endpoint.starts_with("127.0.0.1:")
        || endpoint.starts_with("localhost:")
        || endpoint.starts_with("[::1]:")
}

fn connect_pool_stream(endpoint: &str) -> Result<(TcpStream, BufReader<TcpStream>)> {
    let mut addrs: Vec<SocketAddr> = endpoint
        .to_socket_addrs()
        .with_context(|| format!("failed to resolve pool endpoint {endpoint}"))?
        .collect();
    if addrs.is_empty() {
        bail!("pool endpoint resolved to no addresses: {endpoint}");
    }
    addrs.sort_by_key(|addr| !addr.is_ipv4());

    let mut last_err: Option<std::io::Error> = None;
    let mut stream = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
            Ok(connected) => {
                stream = Some(connected);
                break;
            }
            Err(err) => {
                last_err = Some(err);
            }
        }
    }
    let stream = stream.ok_or_else(|| {
        anyhow!(
            "failed to connect to pool at {endpoint}: {}",
            last_err
                .map(|err| err.to_string())
                .unwrap_or_else(|| "no addresses available".to_string())
        )
    })?;

    stream
        .set_read_timeout(Some(SOCKET_IO_TIMEOUT))
        .context("failed to set pool read timeout")?;
    stream
        .set_write_timeout(Some(SOCKET_IO_TIMEOUT))
        .context("failed to set pool write timeout")?;
    let reader = BufReader::new(
        stream
            .try_clone()
            .context("failed to clone pool stream for reader")?,
    );
    Ok((stream, reader))
}

fn send_login(stream: &mut TcpStream, address: &str, worker: &str) -> Result<()> {
    let request = StratumRequest {
        id: LOGIN_REQUEST_ID,
        method: "login",
        params: LoginParams {
            address,
            worker,
            protocol_version: STRATUM_PROTOCOL_VERSION,
            capabilities: CLIENT_CAPABILITIES,
        },
    };
    send_json_line(stream, &request)
}

fn send_submit(
    stream: &mut TcpStream,
    request_id: u64,
    submit: &PoolSubmit,
    include_claimed_hash: bool,
) -> Result<()> {
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
    send_json_line(stream, &request)
}

fn send_json_line<T: Serialize>(stream: &mut TcpStream, payload: &T) -> Result<()> {
    let mut line = serde_json::to_vec(payload).context("failed to serialize stratum payload")?;
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
        result
            .get("accepted")
            .and_then(Value::as_bool)
            .unwrap_or(true)
    } else {
        msg.status
            .as_deref()
            .map(|status| status.eq_ignore_ascii_case("ok"))
            .unwrap_or(true)
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
}

fn parse_pool_endpoint(pool_url: &str) -> Result<String> {
    let trimmed = pool_url.trim();
    if trimmed.is_empty() {
        bail!("pool URL is empty");
    }
    let authority = trimmed
        .strip_prefix("stratum+tcp://")
        .unwrap_or(trimmed)
        .split('/')
        .next()
        .unwrap_or(trimmed)
        .trim();
    if authority.is_empty() {
        bail!("pool URL authority is empty");
    }
    if authority.contains("://") {
        bail!("unsupported pool URL scheme");
    }
    Ok(authority.to_string())
}

fn sleep_with_shutdown(shutdown: &AtomicBool, duration: Duration) {
    let deadline = std::time::Instant::now() + duration;
    while !shutdown.load(Ordering::Relaxed) {
        if std::time::Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

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

    fn capture_login_payload(address: &str, worker: &str) -> Value {
        capture_payload(|stream| {
            send_login(stream, address, worker).expect("login payload should serialize");
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
        let payload = capture_login_payload("XbTestAddress", "rig01");

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
}
