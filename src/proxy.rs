//! Farm proxy: rigs on a local network share upstream pool sessions.
//!
//! Rigs are grouped by the payout address they log in with, and each address
//! mines through its own upstream session, so every share pays the address
//! its rig asked for (dev-fee rounds included). Each rig gets its own slice of
//! a session's nonce range, so the pool sees one session while every rig hashes
//! distinct nonces. Shares are checked against the submitting rig's slice
//! before they go upstream, and the pool's answer is routed back to that rig.
//! Rigs top up their slice with `refill`; the proxy asks the pool for more range
//! the same way, or opens another session for the address when the pool does
//! not offer `refill`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::config::Config;
use crate::miner::ui::{info, success, warn};
use crate::pool::{
    PoolClient, PoolEvent, PoolJob, PoolNotification, SocksProxy,
    POOL_NOTIFICATION_MINER_BLOCK_FOUND, POOL_NOTIFICATION_POOL_BLOCK_SOLVED,
    STRATUM_CAPABILITY_NONCE_REFILL_V1,
};

const MAX_RIG_MESSAGE_BYTES: usize = 8 * 1024;
const CAP_SUBMIT_CLAIMED_HASH: &str = "submit_claimed_hash";
const CAP_SAME_TEMPLATE_REBIND_V1: &str = "same_template_rebind_v1";
const CAP_POW_BLAKE2B_V1: &str = "pow_blake2b_v1";
const FARADAY_UPGRADE_MESSAGE: &str =
    "this proxy is mining the Faraday upgrade; update Seine to a release that supports it";
/// A rig's first slice is this fraction of an upstream range.
const INITIAL_SLICE_DIVISOR: u64 = 128;
/// A rig's slice never grows past this fraction of an upstream range.
const MAX_SLICE_DIVISOR: u64 = 4;
/// A rig that finishes a slice faster than this gets a slice twice as large.
const QUICK_REFILL: Duration = Duration::from_secs(30);
/// A rig that takes longer than this gets a slice half as large.
const SLOW_REFILL: Duration = Duration::from_secs(10 * 60);
/// An unanswered upstream refill is asked for again after this long.
const UPSTREAM_REFILL_RETRY: Duration = Duration::from_secs(15);
/// Refills per session and template before opening another session; pools
/// cap live assignments per connection.
const MAX_REFILLS_PER_TEMPLATE: usize = 200;
/// Distinct payout addresses served at once; rigs paying another address are
/// turned away until an address's last rig leaves and its sessions close.
const MAX_ADDRESSES: usize = 16;
/// How long rigs paying an address the pool rejected are turned away.
const REJECTED_ADDRESS_HOLD: Duration = Duration::from_secs(5 * 60);
/// Upstream sessions per payout address.
const MAX_SESSIONS_PER_ADDRESS: usize = 64;
/// Shortest gap between opening upstream sessions.
const SESSION_OPEN_INTERVAL: Duration = Duration::from_millis(500);
/// Range kept per template beyond what rigs' current slices add up to, in
/// percent, so a new template hands every rig a slice at once.
const CAPACITY_HEADROOM_PCT: u64 = 125;
/// Upstream range assumed before any session has a job.
const DEFAULT_RANGE: u64 = 1_000_000;
/// A session no rig has used for this long is closed, except the first
/// session of --address.
const IDLE_SESSION_CLOSE: Duration = Duration::from_secs(10 * 60);
const SUMMARY_INTERVAL: Duration = Duration::from_secs(60);
const TICK: Duration = Duration::from_secs(1);

pub fn run(cfg: &Config, shutdown: Arc<AtomicBool>) -> Result<()> {
    let listen = cfg
        .proxy_listen
        .clone()
        .ok_or_else(|| anyhow!("--proxy-listen is required for the farm proxy"))?;
    let pool_url = cfg
        .pool_url
        .clone()
        .ok_or_else(|| anyhow!("the farm proxy needs --pool-url"))?;
    let destination = cfg
        .mining_address
        .clone()
        .ok_or_else(|| anyhow!("the farm proxy needs --address"))?;
    let address = crate::shortname::ShortnameResolver::default().resolve(&destination)?;
    if address != destination.trim() {
        info("ADDRESS", format!("resolved {destination} to {address}"));
    }
    let worker = cfg
        .pool_worker
        .clone()
        .unwrap_or_else(|| "proxy".to_string());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .thread_name("seine-proxy")
        .build()
        .context("failed to start the proxy runtime")?;
    runtime.block_on(async {
        let listener = TcpListener::bind(&listen)
            .await
            .with_context(|| format!("failed to listen on {listen}"))?;
        serve(
            listener,
            pool_url,
            cfg.pool_proxy.clone(),
            address,
            worker,
            shutdown,
        )
        .await
    })
}

async fn serve(
    listener: TcpListener,
    pool_url: String,
    pool_proxy: Option<SocksProxy>,
    address: String,
    worker: String,
    shutdown: Arc<AtomicBool>,
) -> Result<()> {
    // Validate the pool URL list up front rather than on the first session.
    crate::pool::normalize_pool_urls(&pool_url)?;
    let local = listener
        .local_addr()
        .map_or_else(|_| "?".to_string(), |addr| addr.to_string());
    success(
        "PROXY",
        format!(
            "listening on {local} for rigs; mining to {} via {pool_url} as {worker}",
            compact_address(&address)
        ),
    );

    let (hub_tx, hub_rx) = mpsc::unbounded_channel::<HubMsg>();
    let ticker_tx = hub_tx.clone();
    let ticker_shutdown = Arc::clone(&shutdown);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(TICK);
        while !ticker_shutdown.load(Ordering::Relaxed) {
            tick.tick().await;
            if ticker_tx.send(HubMsg::Tick).is_err() {
                break;
            }
        }
    });
    let accept_tx = hub_tx.clone();
    tokio::spawn(async move {
        let mut next_conn = 0u64;
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    next_conn += 1;
                    tokio::spawn(serve_rig(next_conn, stream, peer, accept_tx.clone()));
                }
                Err(err) => {
                    warn("PROXY", format!("accept failed: {err}"));
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    });

    let upstreams = Upstreams {
        pool_url,
        pool_proxy,
        worker,
        hub_tx,
        shutdown: Arc::clone(&shutdown),
    };
    let mut hub = Hub::new(upstreams, address);
    hub.run(hub_rx, shutdown).await;
    Ok(())
}

/// Opens upstream pool sessions and forwards their events into the hub.
struct Upstreams {
    pool_url: String,
    pool_proxy: Option<SocksProxy>,
    worker: String,
    hub_tx: mpsc::UnboundedSender<HubMsg>,
    shutdown: Arc<AtomicBool>,
}

impl Upstreams {
    fn open(&self, session: usize, address: &str) -> Result<(Arc<PoolClient>, Arc<AtomicBool>)> {
        let stop = Arc::new(AtomicBool::new(false));
        let client = Arc::new(PoolClient::connect_via(
            &self.pool_url,
            self.pool_proxy.as_ref(),
            address.to_string(),
            self.worker.clone(),
            Arc::clone(&stop),
        )?);
        let bridge_client = Arc::clone(&client);
        let bridge_stop = Arc::clone(&stop);
        let shutdown = Arc::clone(&self.shutdown);
        let hub_tx = self.hub_tx.clone();
        std::thread::Builder::new()
            .name(format!("proxy-upstream-{session}"))
            .spawn(move || {
                while !shutdown.load(Ordering::Relaxed) && !bridge_stop.load(Ordering::Relaxed) {
                    if let Some(event) =
                        bridge_client.recv_event_timeout(Duration::from_millis(200))
                    {
                        if hub_tx.send(HubMsg::Upstream(session, event)).is_err() {
                            break;
                        }
                    }
                }
                bridge_stop.store(true, Ordering::Relaxed);
            })
            .context("failed to start a proxy upstream thread")?;
        Ok((client, stop))
    }
}

async fn serve_rig(
    conn: u64,
    stream: TcpStream,
    peer: SocketAddr,
    hub_tx: mpsc::UnboundedSender<HubMsg>,
) {
    let _ = stream.set_nodelay(true);
    let (read_half, mut write_half) = stream.into_split();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    if hub_tx.send(HubMsg::Connected { conn, peer, tx }).is_err() {
        return;
    }
    let writer = tokio::spawn(async move {
        while let Some(mut line) = rx.recv().await {
            line.push('\n');
            if write_half.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
        let _ = write_half.shutdown().await;
    });
    let mut reader = BufReader::new(read_half);
    while let Ok(Some(line)) =
        crate::pool::read_line_limited_async(&mut reader, MAX_RIG_MESSAGE_BYTES).await
    {
        if hub_tx.send(HubMsg::Line { conn, line }).is_err() {
            break;
        }
    }
    let _ = hub_tx.send(HubMsg::Closed { conn });
    let _ = writer.await;
}

enum HubMsg {
    Upstream(usize, PoolEvent),
    Connected {
        conn: u64,
        peer: SocketAddr,
        tx: mpsc::UnboundedSender<String>,
    },
    Line {
        conn: u64,
        line: String,
    },
    Closed {
        conn: u64,
    },
    Tick,
}

/// One upstream nonce range: the job the pool assigned and how much of it
/// has been handed to rigs.
#[derive(Debug, Clone)]
struct Assignment {
    upstream_job_id: String,
    target: String,
    difficulty: Option<u64>,
    start: u64,
    end: u64,
    next: u64,
    exhausted: bool,
    /// The pool replaced this range; shares on it go stale after its grace.
    retired: bool,
}

impl Assignment {
    fn from_job(job: &PoolJob) -> Self {
        Self {
            upstream_job_id: job.job_id.clone(),
            target: job.target.clone(),
            difficulty: job.difficulty,
            start: job.nonce_start,
            end: job.nonce_end,
            next: job.nonce_start,
            exhausted: job.nonce_end < job.nonce_start,
            retired: false,
        }
    }

    fn remaining(&self) -> u64 {
        if self.exhausted || self.retired {
            0
        } else {
            self.end - self.next + 1
        }
    }

    /// Takes up to `size` nonces from the front of the unissued range.
    fn take(&mut self, size: u64) -> Option<(u64, u64)> {
        let available = self.remaining();
        if available == 0 || size == 0 {
            return None;
        }
        let start = self.next;
        let end = start + size.min(available) - 1;
        if end == self.end {
            self.exhausted = true;
        } else {
            self.next = end + 1;
        }
        Some((start, end))
    }
}

/// Upstream work for one block template on one session.
#[derive(Debug)]
struct Template {
    generation: u64,
    height: u64,
    template_key: String,
    header_base: String,
    network_target: Option<String>,
    template_id: Option<String>,
    range_size: u64,
    assignments: Vec<Assignment>,
    refill_requested_at: Option<Instant>,
    /// The upstream session this work came from has ended.
    dead: bool,
}

impl Template {
    fn from_job(generation: u64, job: &PoolJob) -> Self {
        Self {
            generation,
            height: job.height,
            template_key: template_key(job),
            header_base: job.header_base.clone(),
            network_target: job.network_target.clone(),
            template_id: job.template_id.clone(),
            range_size: job.nonce_count().max(1),
            assignments: vec![Assignment::from_job(job)],
            refill_requested_at: None,
            dead: false,
        }
    }

    fn remaining(&self) -> u64 {
        self.assignments
            .iter()
            .map(Assignment::remaining)
            .fold(0u64, u64::saturating_add)
    }
}

fn template_key(job: &PoolJob) -> String {
    match job.template_id.as_deref() {
        Some(id) => format!("{}:{id}", job.height),
        None => format!("{}:{}", job.height, job.header_base),
    }
}

/// One upstream pool session, mining to one payout address.
struct Session {
    address: String,
    client: Arc<PoolClient>,
    stop: Arc<AtomicBool>,
    logged_in: bool,
    refill: bool,
    /// The most recent upstream job id, which a refill request must name.
    last_upstream_job: Option<String>,
    current: Option<Template>,
    previous: Option<Template>,
    last_used: Instant,
    /// The first session of --address stays open while the proxy runs.
    permanent: bool,
}

impl Session {
    fn template(&self, generation: u64) -> Option<&Template> {
        [self.current.as_ref(), self.previous.as_ref()]
            .into_iter()
            .flatten()
            .find(|template| template.generation == generation)
    }

    fn live(&self) -> Option<&Template> {
        self.current.as_ref().filter(|template| !template.dead)
    }
}

/// A slice of upstream range given to one rig under a proxy job id.
#[derive(Debug, Clone)]
struct LocalJob {
    conn: u64,
    session: usize,
    generation: u64,
    assignment: usize,
    start: u64,
    end: u64,
}

#[derive(Debug)]
struct Rig {
    tx: mpsc::UnboundedSender<String>,
    peer: SocketAddr,
    worker: String,
    address: String,
    logged_in: bool,
    blake2b: bool,
    slice_size: u64,
    current_job: Option<String>,
    slice_issued_at: Instant,
    accepted: u64,
    rejected: u64,
}

impl Rig {
    fn send(&self, message: Value) {
        let _ = self.tx.send(message.to_string());
    }
}

#[derive(Debug)]
enum Waiter {
    /// Logged in or moved off its slice with no work yet.
    Job(u64),
    /// Asked to refill; `Value` is its request id.
    Refill(u64, Value),
}

#[derive(Debug, Default)]
struct Window {
    accepted: u64,
    rejected: u64,
    difficulty: u128,
}

struct Hub {
    upstreams: Upstreams,
    address: String,
    sessions: HashMap<usize, Session>,
    next_session: usize,
    last_session_opened: Option<Instant>,
    generation: u64,
    rigs: HashMap<u64, Rig>,
    local_jobs: HashMap<String, LocalJob>,
    next_local_job: u64,
    pending_acks: HashMap<(usize, String, u64), VecDeque<(u64, Value)>>,
    waiting: VecDeque<Waiter>,
    /// Addresses a rig could not get a slice for since capacity was last
    /// checked; other addresses are checked on the tick.
    short_of_range: HashSet<String>,
    /// Addresses the pool rejected at login, with its message and when.
    rejected: HashMap<String, (String, Instant)>,
    window: Window,
    last_summary: Instant,
}

impl Hub {
    fn new(upstreams: Upstreams, address: String) -> Self {
        let mut hub = Self {
            upstreams,
            address: address.clone(),
            sessions: HashMap::new(),
            next_session: 0,
            last_session_opened: None,
            generation: 0,
            rigs: HashMap::new(),
            local_jobs: HashMap::new(),
            next_local_job: 0,
            pending_acks: HashMap::new(),
            waiting: VecDeque::new(),
            short_of_range: HashSet::new(),
            rejected: HashMap::new(),
            window: Window::default(),
            last_summary: Instant::now(),
        };
        if let Some(session) = hub.open_session(&address) {
            if let Some(session) = hub.sessions.get_mut(&session) {
                session.permanent = true;
            }
        }
        hub
    }

    async fn run(&mut self, mut rx: mpsc::UnboundedReceiver<HubMsg>, shutdown: Arc<AtomicBool>) {
        while let Some(message) = rx.recv().await {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            match message {
                HubMsg::Upstream(session, event) => self.on_upstream(session, event),
                HubMsg::Connected { conn, peer, tx } => {
                    self.rigs.insert(
                        conn,
                        Rig {
                            tx,
                            peer,
                            worker: peer.to_string(),
                            address: self.address.clone(),
                            logged_in: false,
                            blake2b: false,
                            slice_size: 0,
                            current_job: None,
                            slice_issued_at: Instant::now(),
                            accepted: 0,
                            rejected: 0,
                        },
                    );
                }
                HubMsg::Line { conn, line } => self.on_rig_line(conn, &line),
                HubMsg::Closed { conn } => self.on_rig_closed(conn),
                HubMsg::Tick => self.on_tick(),
            }
            for address in std::mem::take(&mut self.short_of_range) {
                self.grow_capacity(&address);
            }
        }
        for session in self.sessions.values() {
            session.stop.store(true, Ordering::Relaxed);
        }
    }

    // ---- sessions -------------------------------------------------------

    fn open_session(&mut self, address: &str) -> Option<usize> {
        let id = self.next_session;
        match self.upstreams.open(id, address) {
            Ok((client, stop)) => {
                self.next_session += 1;
                self.last_session_opened = Some(Instant::now());
                self.sessions.insert(
                    id,
                    Session {
                        address: address.to_string(),
                        client,
                        stop,
                        logged_in: false,
                        refill: false,
                        last_upstream_job: None,
                        current: None,
                        previous: None,
                        last_used: Instant::now(),
                        permanent: false,
                    },
                );
                Some(id)
            }
            Err(err) => {
                warn(
                    "PROXY",
                    format!("could not open an upstream session: {err:#}"),
                );
                None
            }
        }
    }

    /// Sessions mining to `address`, oldest first.
    fn sessions_for(&self, address: &str) -> Vec<usize> {
        let mut ids: Vec<usize> = self
            .sessions
            .iter()
            .filter(|(_, session)| session.address == address)
            .map(|(id, _)| *id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// The payout address a rig's login maps to: its own when it is a valid
    /// Blocknet address, otherwise --address. `None` when every address slot
    /// is taken by other addresses.
    fn payout_address(&self, requested: &str) -> Option<String> {
        let requested = requested.trim();
        // A rig may log in as solo:<address>; it gets its own solo upstream.
        let destination = crate::shortname::split_solo(requested).1;
        let valid = !destination.is_empty()
            && !destination.starts_with(['@', '$'])
            && crate::shortname::validate_destination(requested).is_ok();
        if !valid {
            return Some(self.address.clone());
        }
        let known: HashSet<&str> = self.sessions.values().map(|s| s.address.as_str()).collect();
        (known.contains(requested) || known.len() < MAX_ADDRESSES).then(|| requested.to_string())
    }

    /// Range one template holds and the range `address`'s rigs want per
    /// template, from their current slice sizes.
    fn demand(&self, address: &str) -> (u64, u64) {
        let range = self
            .sessions
            .values()
            .filter(|s| s.address == address)
            .find_map(|s| s.live().map(|t| t.range_size))
            .unwrap_or(DEFAULT_RANGE)
            .max(1);
        let initial = (range / INITIAL_SLICE_DIVISOR).max(1);
        let demand = self
            .rigs
            .values()
            .filter(|rig| rig.logged_in && rig.address == address)
            .map(|rig| {
                if rig.slice_size == 0 {
                    initial
                } else {
                    rig.slice_size
                }
            })
            .fold(0u64, u64::saturating_add);
        (range, demand)
    }

    /// Sessions `address` needs so one template's ranges cover a slice for
    /// every rig mining to it.
    fn session_target(&self, address: &str) -> usize {
        let (range, demand) = self.demand(address);
        if demand == 0 {
            // No rig pays this address: its idle sessions can all close.
            return 0;
        }
        let wanted = demand.saturating_mul(CAPACITY_HEADROOM_PCT) / 100;
        (wanted.div_ceil(range) as usize).clamp(1, MAX_SESSIONS_PER_ADDRESS)
    }

    /// Keeps enough upstream range for `address`: sessions sized to its rigs'
    /// demand, then a refill where the pool offers one when the live range
    /// runs low, or another session when no session can refill.
    fn grow_capacity(&mut self, address: &str) {
        let ids = self.sessions_for(address);
        let paced = self
            .last_session_opened
            .is_none_or(|at| at.elapsed() >= SESSION_OPEN_INTERVAL);
        if ids.is_empty() || (ids.len() < self.session_target(address) && paced) {
            self.open_session(address);
            return;
        }
        let (range, demand) = self.demand(address);
        let live_remaining = ids
            .iter()
            .filter_map(|id| self.sessions[id].live())
            .map(Template::remaining)
            .fold(0u64, u64::saturating_add);
        if live_remaining >= (range / 2).max(demand / 4) {
            return;
        }
        let refilling = ids.iter().any(|id| {
            self.sessions[id].live().is_some_and(|t| {
                t.refill_requested_at
                    .is_some_and(|at| at.elapsed() < UPSTREAM_REFILL_RETRY)
            })
        });
        if refilling {
            return;
        }
        let refillable = ids.iter().copied().find(|id| {
            let session = &self.sessions[id];
            session.logged_in
                && session.refill
                && session
                    .live()
                    .is_some_and(|t| t.assignments.len() <= MAX_REFILLS_PER_TEMPLATE)
        });
        if let Some(id) = refillable {
            let session = self.sessions.get_mut(&id).expect("listed session");
            if let (Some(job_id), Some(template)) =
                (session.last_upstream_job.clone(), session.current.as_mut())
            {
                session.client.request_nonce_refill(&job_id);
                template.refill_requested_at = Some(Instant::now());
            }
            return;
        }
        // No session can refill: another session brings its own range for
        // each job. Wait for sessions still connecting first.
        let connecting = ids.iter().any(|id| {
            let session = &self.sessions[id];
            !session.logged_in || session.current.is_none()
        });
        if !connecting && paced && ids.len() < MAX_SESSIONS_PER_ADDRESS {
            if let Some(id) = self.open_session(address) {
                info(
                    "PROXY",
                    format!(
                        "opened upstream session {} for {} ({} total)",
                        id,
                        compact_address(address),
                        ids.len() + 1
                    ),
                );
            }
        }
    }

    /// The pool refused `address`: close its sessions and send its rigs away
    /// so they see the reason at their next login instead of waiting for work.
    fn drop_rejected_address(&mut self, address: String, message: String) {
        let ids = self.sessions_for(&address);
        for id in ids {
            if let Some(session) = self.sessions.remove(&id) {
                session.stop.store(true, Ordering::Relaxed);
                self.local_jobs.retain(|_, local| local.session != id);
                self.pending_acks
                    .retain(|(session, _, _), _| *session != id);
            }
        }
        let rigs: Vec<u64> = self
            .rigs
            .iter()
            .filter(|(_, rig)| rig.logged_in && rig.address == address)
            .map(|(conn, _)| *conn)
            .collect();
        for conn in &rigs {
            // Dropping the rig's sender closes its connection.
            self.rigs.remove(conn);
        }
        self.waiting.retain(|waiter| match waiter {
            Waiter::Job(conn) | Waiter::Refill(conn, _) => !rigs.contains(conn),
        });
        self.rejected.insert(address, (message, Instant::now()));
    }

    fn close_idle_sessions(&mut self) {
        let in_use: HashSet<usize> = self
            .rigs
            .values()
            .filter_map(|rig| self.local_jobs.get(rig.current_job.as_deref()?))
            .map(|local| local.session)
            .collect();
        let mut idle: Vec<usize> = self
            .sessions
            .iter()
            .filter(|(id, session)| {
                !session.permanent
                    && !in_use.contains(id)
                    && session.last_used.elapsed() >= IDLE_SESSION_CLOSE
            })
            .map(|(id, _)| *id)
            .collect();
        // Newest first, keeping what each address's demand still needs.
        idle.sort_unstable_by(|a, b| b.cmp(a));
        let mut open: HashMap<String, usize> = HashMap::new();
        for session in self.sessions.values() {
            *open.entry(session.address.clone()).or_default() += 1;
        }
        let targets: HashMap<String, usize> = open
            .keys()
            .map(|address| (address.clone(), self.session_target(address)))
            .collect();
        idle.retain(|id| {
            let address = &self.sessions[id].address;
            let count = open.get_mut(address).expect("counted address");
            if *count > targets[address] {
                *count -= 1;
                true
            } else {
                false
            }
        });
        for id in idle {
            if let Some(session) = self.sessions.remove(&id) {
                session.stop.store(true, Ordering::Relaxed);
                self.local_jobs.retain(|_, local| local.session != id);
                self.pending_acks
                    .retain(|(session, _, _), _| *session != id);
            }
        }
    }

    // ---- upstream -------------------------------------------------------

    fn on_upstream(&mut self, id: usize, event: PoolEvent) {
        let Some(session) = self.sessions.get_mut(&id) else {
            return;
        };
        match event {
            PoolEvent::Connected => {}
            PoolEvent::LoginAccepted(ack) => {
                session.logged_in = true;
                session.refill = ack.supports(STRATUM_CAPABILITY_NONCE_REFILL_V1);
                if id == 0 {
                    success("PROXY", "upstream pool login accepted");
                }
            }
            PoolEvent::LoginRejected(message) => {
                session.logged_in = false;
                warn(
                    "PROXY",
                    format!(
                        "upstream pool rejected login for {}: {message}",
                        compact_address(&session.address)
                    ),
                );
                if !session.permanent {
                    let address = session.address.clone();
                    self.drop_rejected_address(address, message);
                }
            }
            PoolEvent::Disconnected(message) => {
                session.logged_in = false;
                if session.permanent {
                    warn("PROXY", format!("upstream: {message}"));
                }
                // Work from the ended session cannot be submitted any more.
                if let Some(mut current) = session.current.take() {
                    current.dead = true;
                    let generation = current.generation;
                    session.previous = Some(current);
                    self.move_rigs_off(id, generation, |_| true);
                }
            }
            PoolEvent::Job(job) => self.on_upstream_job(id, job),
            PoolEvent::SubmitAck(ack) => {
                let key = (id, ack.job_id.clone(), ack.nonce);
                let Some(queue) = self.pending_acks.get_mut(&key) else {
                    return;
                };
                let Some((conn, request_id)) = queue.pop_front() else {
                    return;
                };
                if queue.is_empty() {
                    self.pending_acks.remove(&key);
                }
                let Some(rig) = self.rigs.get_mut(&conn) else {
                    return;
                };
                if ack.accepted {
                    rig.accepted += 1;
                    self.window.accepted += 1;
                    self.window.difficulty += u128::from(ack.difficulty.unwrap_or(0));
                    rig.send(json!({
                        "id": request_id,
                        "status": "ok",
                        "result": {"accepted": true, "difficulty": ack.difficulty},
                    }));
                } else {
                    rig.rejected += 1;
                    self.window.rejected += 1;
                    let reason = ack.error.unwrap_or_else(|| "rejected".to_string());
                    rig.send(json!({"id": request_id, "error": reason}));
                }
            }
            PoolEvent::SubmitRetry(_) => {}
            PoolEvent::Notification(notification) => {
                let permanent = session.permanent;
                self.on_upstream_notification(notification, permanent);
            }
        }
    }

    fn on_upstream_notification(&mut self, notification: PoolNotification, log_other: bool) {
        let kind = notification.kind.as_str();
        if kind == POOL_NOTIFICATION_POOL_BLOCK_SOLVED
            || kind == POOL_NOTIFICATION_MINER_BLOCK_FOUND
        {
            success("POOL", &notification.message);
            let message = json!({
                "method": "notification",
                "params": {"kind": notification.kind, "message": notification.message},
            });
            for rig in self.rigs.values().filter(|rig| rig.logged_in) {
                rig.send(message.clone());
            }
        } else if log_other {
            info("POOL", &notification.message);
        }
    }

    fn on_upstream_job(&mut self, id: usize, job: PoolJob) {
        enum Fit {
            /// The pool re-issued a range this session holds under a new job id.
            Reissue {
                generation: u64,
                index: usize,
                target_changed: bool,
                retired_others: bool,
            },
            /// The extra range a refill asked for.
            Refill,
            New,
        }
        let Some(session) = self.sessions.get_mut(&id) else {
            return;
        };
        session.last_upstream_job = Some(job.job_id.clone());
        let key = template_key(&job);
        let fit = match session.current.as_mut().filter(|t| t.template_key == key) {
            Some(current) => {
                if let Some(index) = current.assignments.iter().position(|assignment| {
                    assignment.start == job.nonce_start && assignment.end == job.nonce_end
                }) {
                    let assignment = &mut current.assignments[index];
                    let target_changed =
                        assignment.target != job.target || assignment.difficulty != job.difficulty;
                    assignment.upstream_job_id = job.job_id.clone();
                    assignment.target = job.target.clone();
                    assignment.difficulty = job.difficulty;
                    // A re-issued job supersedes every other range this
                    // session holds for the template, refills included.
                    let mut retired_others = false;
                    for (other, assignment) in current.assignments.iter_mut().enumerate() {
                        if other != index && !assignment.retired {
                            assignment.retired = true;
                            retired_others = true;
                        }
                    }
                    current.refill_requested_at = None;
                    Fit::Reissue {
                        generation: current.generation,
                        index,
                        target_changed,
                        retired_others,
                    }
                } else if current.refill_requested_at.take().is_some() {
                    current.assignments.push(Assignment::from_job(&job));
                    Fit::Refill
                } else {
                    Fit::New
                }
            }
            None => Fit::New,
        };
        match fit {
            Fit::Reissue {
                generation,
                index,
                target_changed,
                retired_others,
            } => {
                if target_changed {
                    self.reissue_slices(id, generation, index);
                }
                if retired_others {
                    self.move_rigs_off(id, generation, |assignment| assignment.retired);
                }
            }
            Fit::Refill => self.serve_waiting(),
            Fit::New => self.start_generation(id, &job),
        }
    }

    fn start_generation(&mut self, id: usize, job: &PoolJob) {
        self.generation += 1;
        let generation = self.generation;
        let Some(session) = self.sessions.get_mut(&id) else {
            return;
        };
        let old = session.current.take();
        session.previous = old;
        session.current = Some(Template::from_job(generation, job));
        // Shares on this session's template before last are stale everywhere.
        let keep_from = session
            .previous
            .as_ref()
            .map_or(generation, |t| t.generation);
        self.local_jobs
            .retain(|_, local| local.session != id || local.generation >= keep_from);

        let fork = job.height >= blocknet_pow_spec::FARADAY_ACTIVATION_HEIGHT;
        let mut dropped = Vec::new();
        let mut moving = Vec::new();
        for (conn, rig) in &self.rigs {
            if !rig.logged_in {
                continue;
            }
            if fork && !rig.blake2b {
                rig.send(json!({
                    "method": "notification",
                    "params": {"kind": "pool_notice", "message": FARADAY_UPGRADE_MESSAGE},
                }));
                dropped.push(*conn);
                continue;
            }
            // Rigs on this session's older work, and rigs of this address
            // still waiting for any work, get a slice of the new template.
            let on_session = rig
                .current_job
                .as_deref()
                .and_then(|job_id| self.local_jobs.get(job_id))
                .is_none_or(|local| local.session == id);
            if on_session && rig.address == self.sessions[&id].address {
                moving.push(*conn);
            }
        }
        for conn in dropped {
            // Dropping the sender closes the rig's connection.
            self.rigs.remove(&conn);
        }
        let moving: HashSet<u64> = moving.into_iter().collect();
        self.waiting.retain(|waiter| match waiter {
            Waiter::Job(conn) => !moving.contains(conn),
            Waiter::Refill(..) => true,
        });
        for conn in moving {
            self.push_new_slice(conn);
        }
        self.serve_waiting();
    }

    /// Sends rigs mining slices of `assignment` the same slices under new
    /// job ids carrying the assignment's new share target.
    fn reissue_slices(&mut self, session: usize, generation: u64, assignment: usize) {
        let updates: Vec<(u64, LocalJob)> = self
            .rigs
            .iter()
            .filter_map(|(conn, rig)| {
                let local = self.local_jobs.get(rig.current_job.as_deref()?)?;
                (local.session == session
                    && local.generation == generation
                    && local.assignment == assignment)
                    .then(|| (*conn, local.clone()))
            })
            .collect();
        for (conn, local) in updates {
            let job_id = self.register_local_job(local.clone());
            if let Some(params) = self.job_params(&job_id, &local) {
                if let Some(rig) = self.rigs.get_mut(&conn) {
                    rig.current_job = Some(job_id);
                    rig.send(json!({"method": "job", "params": params}));
                }
            }
        }
    }

    /// Gives rigs mining ranges of `session`/`generation` that match `gone`
    /// a fresh slice of live range.
    fn move_rigs_off(
        &mut self,
        session: usize,
        generation: u64,
        gone: impl Fn(&Assignment) -> bool,
    ) {
        let Some(template) = self
            .sessions
            .get(&session)
            .and_then(|s| s.template(generation))
        else {
            return;
        };
        let stranded: Vec<u64> = self
            .rigs
            .iter()
            .filter(|(_, rig)| rig.logged_in)
            .filter_map(|(conn, rig)| {
                let local = self.local_jobs.get(rig.current_job.as_deref()?)?;
                let affected = local.session == session
                    && local.generation == generation
                    && template
                        .assignments
                        .get(local.assignment)
                        .is_some_and(&gone);
                affected.then_some(*conn)
            })
            .collect();
        for conn in stranded {
            self.push_new_slice(conn);
        }
    }

    // ---- nonce slices ---------------------------------------------------

    /// Carves the next slice for `conn` from a session of its payout address,
    /// growing upstream capacity when it runs low. `None` when no range is
    /// left yet.
    fn allocate_slice(&mut self, conn: u64) -> Option<(String, Value)> {
        let address = self.rigs.get(&conn)?.address.clone();
        let candidates = self.sessions_for(&address);
        let mut slice = None;
        for id in candidates {
            let session = self.sessions.get_mut(&id).expect("listed session");
            let Some(template) = session.current.as_mut().filter(|t| !t.dead) else {
                continue;
            };
            let rig = self.rigs.get_mut(&conn)?;
            let range = template.range_size;
            let min_slice = (range / INITIAL_SLICE_DIVISOR).max(1);
            let max_slice = (range / MAX_SLICE_DIVISOR).max(min_slice);
            if rig.slice_size == 0 {
                rig.slice_size = min_slice;
            }
            let size = rig.slice_size.clamp(min_slice, max_slice);
            let Some(index) = template
                .assignments
                .iter()
                .rposition(|assignment| assignment.remaining() > 0)
            else {
                continue;
            };
            if let Some((start, end)) = template.assignments[index].take(size) {
                session.last_used = Instant::now();
                slice = Some(LocalJob {
                    conn,
                    session: id,
                    generation: template.generation,
                    assignment: index,
                    start,
                    end,
                });
                break;
            }
        }
        let Some(local) = slice else {
            self.short_of_range.insert(address);
            return None;
        };
        let job_id = self.register_local_job(local.clone());
        let params = self.job_params(&job_id, &local)?;
        let rig = self.rigs.get_mut(&conn)?;
        rig.current_job = Some(job_id.clone());
        rig.slice_issued_at = Instant::now();
        Some((job_id, params))
    }

    fn register_local_job(&mut self, local: LocalJob) -> String {
        self.next_local_job += 1;
        let job_id = format!("p{:x}", self.next_local_job);
        self.local_jobs.insert(job_id.clone(), local);
        job_id
    }

    fn job_params(&self, job_id: &str, local: &LocalJob) -> Option<Value> {
        let template = self
            .sessions
            .get(&local.session)?
            .template(local.generation)?;
        let assignment = template.assignments.get(local.assignment)?;
        Some(json!({
            "job_id": job_id,
            "template_id": template.template_id,
            "header_base": template.header_base,
            "target": assignment.target,
            "difficulty": assignment.difficulty,
            "network_target": template.network_target,
            "height": template.height,
            "nonce_start": local.start,
            "nonce_end": local.end,
        }))
    }

    fn push_new_slice(&mut self, conn: u64) {
        match self.allocate_slice(conn) {
            Some((_, params)) => {
                if let Some(rig) = self.rigs.get(&conn) {
                    rig.send(json!({"method": "job", "params": params}));
                }
            }
            None => {
                let already = self.waiting.iter().any(|waiter| match waiter {
                    Waiter::Job(waiting) | Waiter::Refill(waiting, _) => *waiting == conn,
                });
                if !already {
                    self.waiting.push_back(Waiter::Job(conn));
                }
            }
        }
    }

    fn serve_waiting(&mut self) {
        let waiting = std::mem::take(&mut self.waiting);
        let mut still_waiting = VecDeque::new();
        for waiter in waiting {
            let conn = match &waiter {
                Waiter::Job(conn) | Waiter::Refill(conn, _) => *conn,
            };
            if !self.rigs.contains_key(&conn) {
                continue;
            }
            let Some((_, params)) = self.allocate_slice(conn) else {
                still_waiting.push_back(waiter);
                continue;
            };
            let rig = &self.rigs[&conn];
            match waiter {
                Waiter::Job(_) => rig.send(json!({"method": "job", "params": params})),
                Waiter::Refill(_, id) => {
                    rig.send(json!({"id": id, "status": "ok", "result": params}))
                }
            }
        }
        still_waiting.extend(std::mem::take(&mut self.waiting));
        self.waiting = still_waiting;
    }

    // ---- rigs -----------------------------------------------------------

    fn on_rig_line(&mut self, conn: u64, line: &str) {
        let line = line.trim();
        if line.is_empty() {
            return;
        }
        let Ok(message) = serde_json::from_str::<Value>(line) else {
            return;
        };
        let id = message.get("id").cloned().unwrap_or(Value::Null);
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        let logged_in = self.rigs.get(&conn).is_some_and(|rig| rig.logged_in);
        match message.get("method").and_then(Value::as_str) {
            Some("login") if logged_in => self.reply_error(conn, id, "already logged in"),
            Some("login") => self.on_rig_login(conn, id, &params),
            Some(_) if !logged_in => self.reply_error(conn, id, "login first"),
            Some("submit") => self.on_rig_submit(conn, id, &params),
            Some("refill") => self.on_rig_refill(conn, id, &params),
            Some(_) => self.reply_error(conn, id, "unknown method"),
            None => {}
        }
    }

    fn on_rig_login(&mut self, conn: u64, id: Value, params: &Value) {
        let capabilities: HashSet<String> = params
            .get("capabilities")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|value| value.trim().to_ascii_lowercase())
                    .collect()
            })
            .unwrap_or_default();
        if params.get("protocol_version").and_then(Value::as_u64) != Some(2) {
            return self.reply_error(conn, id, "pool requires protocol_version 2");
        }
        if !capabilities.contains(CAP_SUBMIT_CLAIMED_HASH) {
            return self.reply_error(conn, id, "pool requires submit_claimed_hash capability");
        }
        let blake2b = capabilities.contains(CAP_POW_BLAKE2B_V1);
        let fork_era = self.sessions.values().any(|session| {
            session
                .live()
                .is_some_and(|t| t.height >= blocknet_pow_spec::FARADAY_ACTIVATION_HEIGHT)
        });
        if !blake2b && fork_era {
            return self.reply_error(conn, id, FARADAY_UPGRADE_MESSAGE);
        }
        let worker = params
            .get("worker")
            .and_then(Value::as_str)
            .map(|worker| {
                worker
                    .chars()
                    .filter(|ch| !ch.is_control())
                    .take(64)
                    .collect::<String>()
            })
            .filter(|worker| !worker.trim().is_empty());
        let Some(address) =
            self.payout_address(params.get("address").and_then(Value::as_str).unwrap_or(""))
        else {
            return self.reply_error(
                conn,
                id,
                "this proxy is mining to as many payout addresses as it can; try again later",
            );
        };
        self.rejected
            .retain(|_, (_, at)| at.elapsed() < REJECTED_ADDRESS_HOLD);
        if let Some((message, _)) = self.rejected.get(&address) {
            let message = format!("pool rejected this payout address: {message}");
            return self.reply_error(conn, id, &message);
        }
        if self.sessions_for(&address).is_empty() {
            self.open_session(&address);
        }
        let Some(rig) = self.rigs.get_mut(&conn) else {
            return;
        };
        rig.logged_in = true;
        rig.blake2b = blake2b;
        rig.address = address;
        if let Some(worker) = worker {
            rig.worker = worker;
        }
        info(
            "PROXY",
            format!(
                "rig {} connected from {} paying {}",
                rig.worker,
                rig.peer,
                compact_address(&rig.address)
            ),
        );
        rig.send(json!({
            "id": id,
            "status": "ok",
            "result": {
                "protocol_version": 2,
                "capabilities": [
                    CAP_SUBMIT_CLAIMED_HASH,
                    CAP_SAME_TEMPLATE_REBIND_V1,
                    STRATUM_CAPABILITY_NONCE_REFILL_V1,
                ],
                "required_capabilities": [CAP_SUBMIT_CLAIMED_HASH],
            },
        }));
        self.push_new_slice(conn);
    }

    fn on_rig_submit(&mut self, conn: u64, id: Value, params: &Value) {
        let job_id = params.get("job_id").and_then(Value::as_str).unwrap_or("");
        let Some(nonce) = params.get("nonce").and_then(Value::as_u64) else {
            return self.reply_error(conn, id, "invalid nonce");
        };
        let Some(local) = self
            .local_jobs
            .get(job_id)
            .filter(|local| local.conn == conn)
            .cloned()
        else {
            return self.reply_error(conn, id, "stale job");
        };
        if nonce < local.start || nonce > local.end {
            return self.reply_error(conn, id, "nonce out of range");
        }
        let Some(session) = self.sessions.get(&local.session) else {
            return self.reply_error(conn, id, "stale job");
        };
        let Some(upstream_job_id) = session
            .template(local.generation)
            .filter(|t| !t.dead)
            .and_then(|t| t.assignments.get(local.assignment))
            .map(|assignment| assignment.upstream_job_id.clone())
        else {
            return self.reply_error(conn, id, "stale job");
        };
        let claimed_hash = match params.get("claimed_hash").and_then(Value::as_str) {
            Some(hex_hash) => match hex::decode(hex_hash.trim())
                .ok()
                .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
            {
                Some(hash) => Some(hash),
                None => return self.reply_error(conn, id, "invalid claimed_hash"),
            },
            None => None,
        };
        let key = (local.session, upstream_job_id.clone(), nonce);
        if self.pending_acks.contains_key(&key) {
            return self.reply_error(conn, id, "duplicate share");
        }
        let backend = params.get("backend").and_then(Value::as_str);
        let worker = self.rigs.get(&conn).map_or("", |rig| rig.worker.as_str());
        if session
            .client
            .submit_share_for_worker(upstream_job_id, nonce, claimed_hash, backend, worker)
            .is_err()
        {
            return self.reply_error(conn, id, "pool unavailable");
        }
        self.pending_acks
            .entry(key)
            .or_default()
            .push_back((conn, id));
    }

    fn on_rig_refill(&mut self, conn: u64, id: Value, params: &Value) {
        let job_id = params.get("job_id").and_then(Value::as_str).unwrap_or("");
        let Some(rig) = self.rigs.get_mut(&conn) else {
            return;
        };
        if rig.current_job.as_deref() != Some(job_id) {
            return self.reply_error(conn, id, "stale job");
        }
        let elapsed = rig.slice_issued_at.elapsed();
        if elapsed < QUICK_REFILL {
            rig.slice_size = rig.slice_size.saturating_mul(2);
        } else if elapsed > SLOW_REFILL {
            rig.slice_size /= 2;
        }
        match self.allocate_slice(conn) {
            Some((_, params)) => {
                if let Some(rig) = self.rigs.get(&conn) {
                    rig.send(json!({"id": id, "status": "ok", "result": params}));
                }
            }
            None => self.waiting.push_back(Waiter::Refill(conn, id)),
        }
    }

    fn on_rig_closed(&mut self, conn: u64) {
        if let Some(rig) = self.rigs.remove(&conn) {
            if rig.logged_in {
                info(
                    "PROXY",
                    format!(
                        "rig {} disconnected ({} accepted, {} rejected)",
                        rig.worker, rig.accepted, rig.rejected
                    ),
                );
            }
        }
        self.waiting.retain(|waiter| match waiter {
            Waiter::Job(waiting) | Waiter::Refill(waiting, _) => *waiting != conn,
        });
    }

    fn reply_error(&self, conn: u64, id: Value, message: &str) {
        if let Some(rig) = self.rigs.get(&conn) {
            rig.send(json!({"id": id, "error": message}));
        }
    }

    fn on_tick(&mut self) {
        let waiting_addresses: HashSet<String> = self
            .waiting
            .iter()
            .filter_map(|waiter| match waiter {
                Waiter::Job(conn) | Waiter::Refill(conn, _) => self.rigs.get(conn),
            })
            .map(|rig| rig.address.clone())
            .collect();
        let refilling: HashSet<String> = self
            .sessions
            .values()
            .filter(|s| s.live().is_some_and(|t| t.refill_requested_at.is_some()))
            .map(|s| s.address.clone())
            .collect();
        let active: HashSet<String> = self
            .rigs
            .values()
            .filter(|rig| rig.logged_in)
            .map(|rig| rig.address.clone())
            .collect();
        let addresses: HashSet<String> = waiting_addresses
            .union(&refilling)
            .cloned()
            .chain(active)
            .collect();
        for address in addresses {
            self.grow_capacity(&address);
        }
        self.serve_waiting();
        self.close_idle_sessions();
        if self.last_summary.elapsed() >= SUMMARY_INTERVAL {
            self.log_summary();
        }
    }

    fn log_summary(&mut self) {
        let elapsed = self.last_summary.elapsed().as_secs_f64().max(1.0);
        let rigs = self.rigs.values().filter(|rig| rig.logged_in).count();
        let window = std::mem::take(&mut self.window);
        let hashrate = window.difficulty as f64 / elapsed;
        let connected = self.sessions.values().filter(|s| s.logged_in).count();
        info(
            "PROXY",
            format!(
                "{rigs} rigs, ~{} from shares, {} accepted, {} rejected in the last {:.0}s; {connected}/{} upstream sessions connected to {}",
                format_hashrate(hashrate),
                window.accepted,
                window.rejected,
                elapsed,
                self.sessions.len(),
                self.upstreams.pool_url,
            ),
        );
        self.last_summary = Instant::now();
    }
}

fn compact_address(address: &str) -> String {
    if address.len() <= 16 {
        return address.to_string();
    }
    format!("{}…{}", &address[..8], &address[address.len() - 6..])
}

fn format_hashrate(hashes_per_second: f64) -> String {
    if hashes_per_second >= 1_000_000.0 {
        format!("{:.2} MH/s", hashes_per_second / 1_000_000.0)
    } else if hashes_per_second >= 1_000.0 {
        format!("{:.2} kH/s", hashes_per_second / 1_000.0)
    } else {
        format!("{hashes_per_second:.1} H/s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader as StdBufReader, Write};
    use std::net::{TcpListener as StdTcpListener, TcpStream as StdTcpStream};
    use std::sync::mpsc as std_mpsc;

    const RANGE: u64 = 1280;

    fn job(job_id: &str, template: &str, height: u64, start: u64) -> Value {
        json!({"method": "job", "params": {
            "job_id": job_id, "template_id": template, "header_base": "11".repeat(92),
            "target": "ff".repeat(32), "difficulty": 5, "height": height,
            "nonce_start": start, "nonce_end": start + RANGE - 1}})
    }

    /// A scripted upstream pool. Lines the proxy sends come out of the
    /// returned receiver; lines pushed into the sender go to the proxy.
    fn fake_pool() -> (u16, std_mpsc::Receiver<Value>, std_mpsc::Sender<Value>) {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (seen_tx, seen_rx) = std_mpsc::channel();
        let (push_tx, push_rx) = std_mpsc::channel::<Value>();
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let push_writer = Arc::new(std::sync::Mutex::new(stream.try_clone().unwrap()));
            let pusher = Arc::clone(&push_writer);
            std::thread::spawn(move || {
                for message in push_rx {
                    let mut out = pusher.lock().unwrap();
                    if writeln!(out, "{message}").is_err() {
                        break;
                    }
                }
            });
            let mut reader = StdBufReader::new(stream);
            let mut line = String::new();
            let mut refills = 0u64;
            while reader.read_line(&mut line).unwrap_or(0) > 0 {
                let Ok(message) = serde_json::from_str::<Value>(line.trim()) else {
                    line.clear();
                    continue;
                };
                line.clear();
                let reply = match message["method"].as_str() {
                    Some("login") => Some(vec![
                        json!({"id": 1, "status": "ok", "result": {"protocol_version": 2,
                            "capabilities": ["submit_claimed_hash", "nonce_refill_v1"],
                            "required_capabilities": ["submit_claimed_hash"]}}),
                        job("u1", "t1", 10, 0),
                    ]),
                    Some("submit") => Some(vec![json!({"id": message["id"], "status": "ok",
                        "result": {"accepted": true, "difficulty": 5}})]),
                    Some("refill") => {
                        refills += 1;
                        let mut reply = job(&format!("r{refills}"), "t1", 10, 10_000 * refills)
                            ["params"]
                            .clone();
                        reply["job_id"] = json!(format!("r{refills}"));
                        Some(vec![
                            json!({"id": message["id"], "status": "ok", "result": reply}),
                        ])
                    }
                    _ => None,
                };
                let _ = seen_tx.send(message);
                if let Some(reply) = reply {
                    let mut out = push_writer.lock().unwrap();
                    for message in reply {
                        writeln!(out, "{message}").unwrap();
                    }
                }
            }
            drop(writer.flush());
        });
        (port, seen_rx, push_tx)
    }

    fn start_proxy(pool_port: u16) -> (u16, Arc<AtomicBool>) {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let shutdown = Arc::new(AtomicBool::new(false));
        let proxy_shutdown = Arc::clone(&shutdown);
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let _ = runtime.block_on(async move {
                serve(
                    TcpListener::from_std(listener).unwrap(),
                    format!("127.0.0.1:{pool_port}"),
                    None,
                    "proxyaddr".to_string(),
                    "proxy".to_string(),
                    proxy_shutdown,
                )
                .await
            });
        });
        (port, shutdown)
    }

    struct TestRig {
        writer: StdTcpStream,
        reader: StdBufReader<StdTcpStream>,
    }

    impl TestRig {
        fn connect(port: u16, capabilities: Value) -> (Self, Value, Value) {
            let stream = StdTcpStream::connect(("127.0.0.1", port)).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut rig = Self {
                writer: stream.try_clone().unwrap(),
                reader: StdBufReader::new(stream),
            };
            rig.send(json!({"id": 1, "method": "login", "params": {
                "address": "rigaddr", "worker": "rig", "protocol_version": 2,
                "capabilities": capabilities}}));
            let login = rig.recv();
            let job = if login.get("error").is_none() {
                rig.recv()
            } else {
                Value::Null
            };
            (rig, login, job)
        }

        fn send(&mut self, message: Value) {
            writeln!(self.writer, "{message}").unwrap();
        }

        fn recv(&mut self) -> Value {
            let mut line = String::new();
            self.reader.read_line(&mut line).expect("proxy reply");
            serde_json::from_str(line.trim()).expect("json from proxy")
        }
    }

    fn range(params: &Value) -> (u64, u64) {
        (
            params["nonce_start"].as_u64().unwrap(),
            params["nonce_end"].as_u64().unwrap(),
        )
    }

    fn next_upstream(seen: &std_mpsc::Receiver<Value>, method: &str) -> Value {
        let deadline = Instant::now() + Duration::from_secs(5);
        while let Ok(message) = seen.recv_timeout(deadline - Instant::now()) {
            if message["method"] == method {
                return message;
            }
        }
        panic!("upstream never saw {method}");
    }

    #[test]
    fn rigs_share_one_upstream_session_with_disjoint_slices() {
        let (pool_port, seen, push) = fake_pool();
        let (proxy_port, shutdown) = start_proxy(pool_port);
        next_upstream(&seen, "login");
        std::thread::sleep(Duration::from_millis(300));

        let caps = json!(["submit_claimed_hash"]);
        let (mut a, login_a, job_a) = TestRig::connect(proxy_port, caps.clone());
        let (mut b, _, job_b) = TestRig::connect(proxy_port, caps);
        assert_eq!(login_a["status"], "ok");
        assert_eq!(job_a["method"], "job");
        let (a_start, a_end) = range(&job_a["params"]);
        let (b_start, b_end) = range(&job_b["params"]);
        assert_eq!(a_end - a_start + 1, RANGE / INITIAL_SLICE_DIVISOR);
        assert!(a_end < b_start || b_end < a_start, "slices overlap");
        assert!(a_end < RANGE && b_end < RANGE);

        // A share inside the rig's slice goes upstream under the pool's job id.
        a.send(json!({"id": 7, "method": "submit", "params": {
            "job_id": job_a["params"]["job_id"], "nonce": a_start,
            "claimed_hash": "00".repeat(32)}}));
        let submit = next_upstream(&seen, "submit");
        assert_eq!(submit["params"]["job_id"], "u1");
        assert_eq!(submit["params"]["nonce"], a_start);
        let ack = a.recv();
        assert_eq!(ack["id"], 7);
        assert_eq!(ack["result"]["accepted"], true);

        // A nonce from another rig's slice is refused without reaching the pool.
        a.send(json!({"id": 8, "method": "submit", "params": {
            "job_id": job_a["params"]["job_id"], "nonce": b_start,
            "claimed_hash": "00".repeat(32)}}));
        assert_eq!(a.recv()["error"], "nonce out of range");

        // Refills hand out fresh slices, growing for a fast rig, until the
        // proxy asks the pool for more range and starts using it.
        let mut job_id = job_a["params"]["job_id"].clone();
        let mut last = (a_start, a_end);
        let mut used_refill_range = false;
        for request in 0..40 {
            a.send(json!({"id": 100 + request, "method": "refill", "params": {"job_id": job_id}}));
            let reply = a.recv();
            assert_eq!(reply["status"], "ok", "{reply}");
            let next = range(&reply["result"]);
            assert!(next.0 > last.1 || next.0 >= 10_000, "refill reused nonces");
            job_id = reply["result"]["job_id"].clone();
            last = next;
            if next.0 >= 10_000 {
                used_refill_range = true;
                break;
            }
        }
        assert!(used_refill_range, "proxy never used upstream refill range");
        let refill = next_upstream(&seen, "refill");
        assert!(refill["params"]["job_id"].as_str().is_some());

        // Vardiff re-issues the original range: rig B keeps its slice at the
        // new difficulty, and rig A leaves the refill range the pool retired.
        let mut reissue = job("u1b", "t1", 10, 0);
        reissue["params"]["difficulty"] = json!(9);
        push.send(reissue).unwrap();
        let b_job = b.recv();
        assert_eq!(b_job["method"], "job");
        assert_eq!(range(&b_job["params"]), (b_start, b_end));
        assert_eq!(b_job["params"]["difficulty"], 9);
        let a_job = a.recv();
        assert_eq!(a_job["method"], "job");
        let a_next = range(&a_job["params"]);
        assert!(
            a_next.1 < RANGE || a_next.0 >= 20_000,
            "rig A still on the retired refill range: {a_next:?}"
        );
        b.send(json!({"id": 9, "method": "submit", "params": {
            "job_id": b_job["params"]["job_id"], "nonce": b_start,
            "claimed_hash": "00".repeat(32)}}));
        assert_eq!(next_upstream(&seen, "submit")["params"]["job_id"], "u1b");
        assert_eq!(b.recv()["id"], 9);

        // A new template reaches every rig with a fresh slice.
        push.send(job("u2", "t2", 11, 50_000)).unwrap();
        for rig in [&mut a, &mut b] {
            let job = rig.recv();
            assert_eq!(job["method"], "job");
            assert_eq!(job["params"]["height"], 11);
            assert!(range(&job["params"]).0 >= 50_000);
        }
        shutdown.store(true, Ordering::Relaxed);
    }

    #[test]
    fn seine_pool_client_mines_through_the_proxy_and_refills_in_place() {
        let (pool_port, seen, _push) = fake_pool();
        let (proxy_port, shutdown) = start_proxy(pool_port);
        next_upstream(&seen, "login");
        let client_shutdown = Arc::new(AtomicBool::new(false));
        let client = PoolClient::connect_via(
            &format!("127.0.0.1:{proxy_port}"),
            None,
            "rigaddr".into(),
            "rig".into(),
            Arc::clone(&client_shutdown),
        )
        .unwrap();
        let mut first = None;
        let mut refilled = None;
        let mut disconnects = 0;
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline && refilled.is_none() {
            match client.recv_event_timeout(Duration::from_millis(100)) {
                Some(PoolEvent::LoginAccepted(ack)) => {
                    assert!(ack.supports(STRATUM_CAPABILITY_NONCE_REFILL_V1));
                }
                Some(PoolEvent::Job(job)) if first.is_none() => {
                    client.request_nonce_refill(&job.job_id);
                    first = Some(job);
                }
                Some(PoolEvent::Job(job)) => refilled = Some(job),
                Some(PoolEvent::Disconnected(_)) => disconnects += 1,
                _ => {}
            }
        }
        client_shutdown.store(true, Ordering::Relaxed);
        shutdown.store(true, Ordering::Relaxed);
        let (first, refilled) = (first.expect("job"), refilled.expect("refilled job"));
        assert!(refilled.nonce_start > first.nonce_end);
        assert_eq!(
            disconnects, 0,
            "refill through the proxy must not reconnect"
        );
    }

    /// A fake pool that accepts any number of sessions. Each login gets its
    /// own range; `refill` is offered only when `refill` is true. Every
    /// message is reported with the address its session logged in with.
    fn multi_pool(refill: bool) -> (u16, std_mpsc::Receiver<(String, Value)>) {
        multi_pool_rejecting(refill, None)
    }

    /// `multi_pool` that refuses logins for `reject`.
    fn multi_pool_rejecting(
        refill: bool,
        reject: Option<&'static str>,
    ) -> (u16, std_mpsc::Receiver<(String, Value)>) {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (seen_tx, seen_rx) = std_mpsc::channel();
        std::thread::spawn(move || {
            for (session, stream) in listener.incoming().enumerate() {
                let Ok(stream) = stream else { break };
                let seen_tx = seen_tx.clone();
                std::thread::spawn(move || {
                    let mut writer = stream.try_clone().unwrap();
                    let mut reader = StdBufReader::new(stream);
                    let mut address = String::new();
                    let mut line = String::new();
                    while reader.read_line(&mut line).unwrap_or(0) > 0 {
                        let Ok(message) = serde_json::from_str::<Value>(line.trim()) else {
                            line.clear();
                            continue;
                        };
                        line.clear();
                        let mut replies = Vec::new();
                        match message["method"].as_str() {
                            Some("login") => {
                                address = message["params"]["address"]
                                    .as_str()
                                    .unwrap_or("")
                                    .to_string();
                                if reject == Some(address.as_str()) {
                                    replies.push(json!({"id": 1, "error": "address not allowed"}));
                                    let _ = seen_tx.send((address.clone(), message));
                                    for reply in replies {
                                        let _ = writeln!(writer, "{reply}");
                                    }
                                    continue;
                                }
                                let caps: Vec<&str> = if refill {
                                    vec!["submit_claimed_hash", "nonce_refill_v1"]
                                } else {
                                    vec!["submit_claimed_hash"]
                                };
                                replies.push(json!({"id": 1, "status": "ok",
                                    "result": {"protocol_version": 2, "capabilities": caps}}));
                                replies.push(job(
                                    &format!("s{session}"),
                                    "t1",
                                    10,
                                    100_000 * session as u64,
                                ));
                            }
                            Some("submit") => replies.push(json!({"id": message["id"],
                                "status": "ok", "result": {"accepted": true, "difficulty": 5}})),
                            _ => {}
                        }
                        let _ = seen_tx.send((address.clone(), message));
                        for reply in replies {
                            if writeln!(writer, "{reply}").is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        });
        (port, seen_rx)
    }

    fn start_proxy_on(pool_port: u16, address: &str) -> (u16, Arc<AtomicBool>) {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let shutdown = Arc::new(AtomicBool::new(false));
        let proxy_shutdown = Arc::clone(&shutdown);
        let address = address.to_string();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let _ = runtime.block_on(async move {
                serve(
                    TcpListener::from_std(listener).unwrap(),
                    format!("127.0.0.1:{pool_port}"),
                    None,
                    address,
                    "proxy".to_string(),
                    proxy_shutdown,
                )
                .await
            });
        });
        (port, shutdown)
    }

    fn login_as(port: u16, address: &str) -> (TestRig, Value) {
        let stream = StdTcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut rig = TestRig {
            writer: stream.try_clone().unwrap(),
            reader: StdBufReader::new(stream),
        };
        rig.send(json!({"id": 1, "method": "login", "params": {
            "address": address, "worker": "rig", "protocol_version": 2,
            "capabilities": ["submit_claimed_hash", "nonce_refill_v1"]}}));
        assert_eq!(rig.recv()["status"], "ok");
        let job = rig.recv();
        assert_eq!(job["method"], "job");
        (rig, job)
    }

    fn next_from(seen: &std_mpsc::Receiver<(String, Value)>, method: &str) -> (String, Value) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while let Ok((address, message)) = seen.recv_timeout(deadline - Instant::now()) {
            if message["method"] == method {
                return (address, message);
            }
        }
        panic!("upstream never saw {method}");
    }

    #[test]
    fn dev_fee_rigs_mine_to_the_dev_address_through_their_own_session() {
        let dev = crate::dev_fee::DEV_ADDRESS;
        let (pool_port, seen) = multi_pool(true);
        let (proxy_port, shutdown) = start_proxy_on(pool_port, "farmaddr");
        assert_eq!(next_from(&seen, "login").0, "farmaddr");

        let (mut dev_rig, job) = login_as(proxy_port, dev);
        let (address, _) = next_from(&seen, "login");
        assert_eq!(
            address, dev,
            "the dev address gets its own upstream session"
        );
        dev_rig.send(json!({"id": 5, "method": "submit", "params": {
            "job_id": job["params"]["job_id"], "nonce": job["params"]["nonce_start"],
            "claimed_hash": "00".repeat(32)}}));
        let (address, submit) = next_from(&seen, "submit");
        assert_eq!(address, dev);
        assert_eq!(submit["params"]["nonce"], job["params"]["nonce_start"]);
        assert_eq!(
            submit["params"]["worker"], "rig",
            "shares name the rig that found them"
        );
        assert_eq!(dev_rig.recv()["id"], 5);

        // A rig with an invalid address mines to the proxy's --address.
        let (mut other, job) = login_as(proxy_port, "not-an-address");
        other.send(json!({"id": 6, "method": "submit", "params": {
            "job_id": job["params"]["job_id"], "nonce": job["params"]["nonce_start"],
            "claimed_hash": "00".repeat(32)}}));
        assert_eq!(next_from(&seen, "submit").0, "farmaddr");
        shutdown.store(true, Ordering::Relaxed);
    }

    #[test]
    fn rigs_paying_an_address_the_pool_rejects_are_told_why() {
        let dev = crate::dev_fee::DEV_ADDRESS;
        let (pool_port, seen) = multi_pool_rejecting(true, Some(dev));
        let (proxy_port, shutdown) = start_proxy_on(pool_port, "farmaddr");
        next_from(&seen, "login");
        let login = |rig: &mut TestRig| {
            rig.send(json!({"id": 1, "method": "login", "params": {
                "address": dev, "worker": "rig", "protocol_version": 2,
                "capabilities": ["submit_claimed_hash", "nonce_refill_v1"]}}));
        };
        let connect = || {
            let stream = StdTcpStream::connect(("127.0.0.1", proxy_port)).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            TestRig {
                writer: stream.try_clone().unwrap(),
                reader: StdBufReader::new(stream),
            }
        };
        let mut rig = connect();
        login(&mut rig);
        assert_eq!(rig.recv()["status"], "ok");
        // The pool refuses the address: the proxy closes the rig instead of
        // leaving it waiting for work.
        let mut line = String::new();
        assert_eq!(rig.reader.read_line(&mut line).unwrap(), 0, "{line}");
        let mut again = connect();
        login(&mut again);
        let reply = again.recv();
        let error = reply["error"].as_str().unwrap_or_default();
        assert!(error.contains("address not allowed"), "{reply}");
        shutdown.store(true, Ordering::Relaxed);
    }

    #[test]
    fn pools_without_refill_get_more_sessions_instead_of_reconnects() {
        let (pool_port, seen) = multi_pool(false);
        let (proxy_port, shutdown) = start_proxy_on(pool_port, "farmaddr");
        next_from(&seen, "login");
        let (mut rig, job) = login_as(proxy_port, "farmaddr");
        let mut job_id = job["params"]["job_id"].clone();
        let mut last_start = job["params"]["nonce_start"].as_u64().unwrap();
        let mut second_session = false;
        for request in 0..60 {
            rig.send(
                json!({"id": 100 + request, "method": "refill", "params": {"job_id": job_id}}),
            );
            let reply = rig.recv();
            assert_eq!(reply["status"], "ok", "{reply}");
            let start = reply["result"]["nonce_start"].as_u64().unwrap();
            job_id = reply["result"]["job_id"].clone();
            if start >= 100_000 {
                second_session = true;
                break;
            }
            assert!(start > last_start);
            last_start = start;
        }
        assert!(
            second_session,
            "the proxy never used a second upstream session"
        );
        let logins: Vec<String> = seen
            .try_iter()
            .filter(|(_, m)| m["method"] == "login")
            .map(|(a, _)| a)
            .collect();
        assert!(
            logins.len() <= 2,
            "the proxy reconnected instead: {logins:?}"
        );
        shutdown.store(true, Ordering::Relaxed);
    }

    #[test]
    fn rigs_without_claimed_hash_are_refused() {
        let (pool_port, seen, _push) = fake_pool();
        let (proxy_port, shutdown) = start_proxy(pool_port);
        next_upstream(&seen, "login");
        let (_rig, login, _) = TestRig::connect(proxy_port, json!([]));
        assert_eq!(
            login["error"],
            "pool requires submit_claimed_hash capability"
        );
        shutdown.store(true, Ordering::Relaxed);
    }

    #[test]
    fn assignments_hand_out_whole_ranges_without_overlap() {
        let mut assignment = Assignment::from_job(
            &serde_json::from_value::<PoolJob>(job("u", "t", 1, 100)["params"].clone()).unwrap(),
        );
        let mut covered = 0;
        let mut previous_end = None;
        while let Some((start, end)) = assignment.take(500) {
            assert!(previous_end.is_none_or(|prev| start == prev + 1));
            covered += end - start + 1;
            previous_end = Some(end);
        }
        assert_eq!(covered, RANGE);
        assert_eq!(previous_end, Some(100 + RANGE - 1));
        assert_eq!(assignment.remaining(), 0);
    }
}
