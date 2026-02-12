mod bench;
mod mining;
mod scheduler;
mod stats;

use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use blocknet_pow_spec::CPU_LANE_MEMORY_BYTES;
use crossbeam_channel::{unbounded, Receiver};

use crate::api::ApiClient;
use crate::backend::cpu::CpuBackend;
use crate::backend::nvidia::NvidiaBackend;
use crate::backend::{
    BackendEvent, BackendInstanceId, NonceLease, PowBackend, WorkAssignment, WorkTemplate,
    WORK_ID_MAX,
};
use crate::config::{BackendKind, Config};
use scheduler::NonceReservation;
use stats::{format_hashrate, Stats};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const TEMPLATE_RETRY_DELAY: Duration = Duration::from_secs(2);
const MIN_EVENT_WAIT: Duration = Duration::from_millis(1);
const HASH_POLL_INTERVAL: Duration = Duration::from_millis(200);

struct BackendSlot {
    id: BackendInstanceId,
    backend: Box<dyn PowBackend>,
    lane_offset: u64,
    lanes: u64,
}

pub fn run(cfg: &Config, shutdown: Arc<AtomicBool>) -> Result<()> {
    if cfg.bench {
        return bench::run_benchmark(cfg, shutdown.as_ref());
    }

    let token = cfg
        .token
        .clone()
        .ok_or_else(|| anyhow!("missing API token in mining mode"))?;
    let client = ApiClient::new(cfg.api_url.clone(), token, REQUEST_TIMEOUT)?;

    let backend_instances = build_backend_instances(cfg);
    let (mut backends, backend_events) = activate_backends(backend_instances)?;
    let total_lanes = total_lanes(&backends);
    let cpu_lanes = cpu_lane_count(&backends);
    let cpu_ram_gib =
        (cpu_lanes as f64 * CPU_LANE_MEMORY_BYTES as f64) / (1024.0 * 1024.0 * 1024.0);

    println!(
        "starting bnminer | backends={} | cpu_lanes={} (~{:.1}GiB RAM) | lanes={} | nonce_iters_per_lane={} | api={} | sse={}",
        backend_names(&backends),
        cpu_lanes,
        cpu_ram_gib,
        total_lanes,
        cfg.nonce_iters_per_lane,
        cfg.api_url,
        if cfg.sse_enabled { "on" } else { "off" }
    );

    let tip_events = if cfg.sse_enabled {
        Some(mining::spawn_tip_listener(
            client.clone(),
            Arc::clone(&shutdown),
        ))
    } else {
        None
    };

    let result = mining::run_mining_loop(
        cfg,
        &client,
        shutdown.as_ref(),
        &mut backends,
        &backend_events,
        tip_events.as_ref(),
    );

    stop_backend_slots(&mut backends);
    result
}

fn build_backend_instances(cfg: &Config) -> Vec<Box<dyn PowBackend>> {
    cfg.backends
        .iter()
        .map(|backend_kind| match backend_kind {
            BackendKind::Cpu => Box::new(CpuBackend::new(cfg.threads)) as Box<dyn PowBackend>,
            BackendKind::Nvidia => Box::new(NvidiaBackend::new()) as Box<dyn PowBackend>,
        })
        .collect()
}

fn activate_backends(
    mut backends: Vec<Box<dyn PowBackend>>,
) -> Result<(Vec<BackendSlot>, Receiver<BackendEvent>)> {
    let mut active = Vec::new();
    let (event_tx, event_rx) = unbounded::<BackendEvent>();
    let mut next_backend_id: BackendInstanceId = 1;

    for mut backend in backends.drain(..) {
        let backend_id = next_backend_id;
        next_backend_id = next_backend_id.saturating_add(1);
        let backend_name = backend.name();
        backend.set_instance_id(backend_id);
        backend.set_event_sink(event_tx.clone());
        match backend.start() {
            Ok(()) => {
                let lanes = backend.lanes() as u64;
                if lanes == 0 {
                    eprintln!(
                        "[backend] skipping {}#{}: reported zero lanes",
                        backend_name, backend_id
                    );
                    backend.stop();
                    continue;
                }
                active.push(BackendSlot {
                    id: backend_id,
                    backend,
                    lane_offset: 0,
                    lanes,
                });
            }
            Err(err) => {
                eprintln!(
                    "[backend] {}#{} unavailable: {err:#}",
                    backend_name, backend_id
                );
                backend.stop();
            }
        }
    }

    if active.is_empty() {
        bail!("no mining backend could be started");
    }

    recalculate_lane_offsets(&mut active);

    Ok((active, event_rx))
}

fn start_backend_slots(backends: &mut [BackendSlot]) -> Result<()> {
    for slot in backends {
        slot.backend.start().with_context(|| {
            format!(
                "failed to start backend {}#{}",
                slot.backend.name(),
                slot.id
            )
        })?;
    }
    Ok(())
}

fn stop_backend_slots(backends: &mut [BackendSlot]) {
    for slot in backends {
        slot.backend.stop();
    }
}

fn distribute_work(
    backends: &mut Vec<BackendSlot>,
    epoch: u64,
    work_id: u64,
    header_base: Arc<[u8]>,
    target: [u8; 32],
    reservation: NonceReservation,
    stop_at: Instant,
) -> Result<()> {
    loop {
        if backends.is_empty() {
            bail!("all mining backends are unavailable");
        }

        let stride = total_lanes(backends);
        let template = Arc::new(WorkTemplate {
            work_id,
            epoch,
            header_base: Arc::clone(&header_base),
            target,
            stop_at,
        });
        let mut failed_indices = Vec::new();

        for (idx, slot) in backends.iter().enumerate() {
            let work = WorkAssignment {
                template: Arc::clone(&template),
                nonce_lease: NonceLease {
                    start_nonce: reservation.start_nonce,
                    lane_offset: slot.lane_offset,
                    global_stride: stride,
                    max_iters_per_lane: reservation.max_iters_per_lane,
                },
            };
            if let Err(err) = slot.backend.assign_work(work) {
                eprintln!(
                    "[backend] {}#{} failed work assignment: {err:#}",
                    slot.backend.name(),
                    slot.id
                );
                failed_indices.push(idx);
            }
        }

        if failed_indices.is_empty() {
            return Ok(());
        }

        for idx in failed_indices.into_iter().rev() {
            let mut slot = backends.remove(idx);
            let backend_name = slot.backend.name();
            let backend_id = slot.id;
            slot.backend.stop();
            eprintln!(
                "[backend] quarantined {}#{} due to assignment failure",
                backend_name, backend_id
            );
        }

        if backends.is_empty() {
            bail!("all mining backends are unavailable after assignment failure");
        }

        recalculate_lane_offsets(backends);
        eprintln!(
            "[backend] retrying work assignment with remaining={}",
            backend_names(backends)
        );
    }
}

fn collect_backend_hashes(
    backends: &[BackendSlot],
    stats: Option<&Stats>,
    round_hashes: &mut u64,
    mut round_backend_hashes: Option<&mut BTreeMap<BackendInstanceId, u64>>,
) {
    let mut collected = 0u64;
    for slot in backends {
        let slot_hashes = slot.backend.take_hashes();
        if slot_hashes == 0 {
            continue;
        }
        collected = collected.saturating_add(slot_hashes);
        if let Some(per_backend) = round_backend_hashes.as_deref_mut() {
            let entry = per_backend.entry(slot.id).or_insert(0);
            *entry = entry.saturating_add(slot_hashes);
        }
    }

    if collected == 0 {
        return;
    }

    if let Some(stats) = stats {
        stats.add_hashes(collected);
    }
    *round_hashes = round_hashes.saturating_add(collected);
}

fn quiesce_backend_slots(backends: &[BackendSlot]) -> Result<()> {
    for slot in backends {
        slot.backend.quiesce().with_context(|| {
            format!(
                "failed to quiesce backend {}#{}",
                slot.backend.name(),
                slot.id
            )
        })?;
    }
    Ok(())
}

fn remove_backend_by_id(backends: &mut Vec<BackendSlot>, backend_id: BackendInstanceId) -> bool {
    let Some(idx) = backends.iter().position(|slot| slot.id == backend_id) else {
        return false;
    };

    let mut slot = backends.remove(idx);
    slot.backend.stop();
    recalculate_lane_offsets(backends);
    true
}

fn backend_name_list(backends: &[BackendSlot]) -> Vec<String> {
    backends
        .iter()
        .map(|slot| format!("{}#{}", slot.backend.name(), slot.id))
        .collect()
}

fn backend_names(backends: &[BackendSlot]) -> String {
    backend_name_list(backends).join(",")
}

fn cpu_lane_count(backends: &[BackendSlot]) -> u64 {
    backends
        .iter()
        .filter(|slot| slot.backend.name() == "cpu")
        .map(|slot| slot.lanes)
        .sum()
}

fn format_round_backend_hashrate(
    backends: &[BackendSlot],
    round_backend_hashes: &BTreeMap<BackendInstanceId, u64>,
    elapsed_secs: f64,
) -> String {
    let elapsed_secs = elapsed_secs.max(0.001);
    let mut parts = Vec::new();
    for (backend_id, hashes) in round_backend_hashes {
        if *hashes == 0 {
            continue;
        }
        let backend_name = backends
            .iter()
            .find(|slot| slot.id == *backend_id)
            .map(|slot| slot.backend.name())
            .unwrap_or("unknown");
        let hps = (*hashes as f64) / elapsed_secs;
        parts.push(format!(
            "{backend_name}#{backend_id}={hashes} ({})",
            format_hashrate(hps)
        ));
    }

    if parts.is_empty() {
        "none".to_string()
    } else {
        parts.join(", ")
    }
}

fn recalculate_lane_offsets(backends: &mut [BackendSlot]) {
    let mut lane_offset = 0u64;
    for slot in backends {
        slot.lane_offset = lane_offset;
        lane_offset = lane_offset.wrapping_add(slot.lanes);
    }
}

fn total_lanes(backends: &[BackendSlot]) -> u64 {
    backends.iter().map(|slot| slot.lanes).sum::<u64>().max(1)
}

fn next_event_wait(
    stop_at: Instant,
    last_stats_print: Instant,
    stats_interval: Duration,
) -> Duration {
    let now = Instant::now();
    let until_stop = stop_at.saturating_duration_since(now);
    let next_stats_at = last_stats_print + stats_interval;
    let until_stats = next_stats_at.saturating_duration_since(now);
    until_stop
        .min(until_stats)
        .min(HASH_POLL_INTERVAL)
        .max(MIN_EVENT_WAIT)
}

fn next_work_id(next_id: &mut u64) -> u64 {
    if *next_id == 0 || *next_id > WORK_ID_MAX {
        *next_id = 1;
    }
    let id = *next_id;
    *next_id = if id >= WORK_ID_MAX { 1 } else { id + 1 };
    id
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use blocknet_pow_spec::POW_HEADER_BASE_LEN;
    use crossbeam_channel::Sender;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct MockState {
        fail_start: AtomicBool,
        fail_next_assign: AtomicBool,
        assign_calls: AtomicUsize,
        stop_calls: AtomicUsize,
        last_lease: Mutex<Option<NonceLease>>,
    }

    struct MockBackend {
        name: &'static str,
        lanes: usize,
        state: Arc<MockState>,
    }

    impl MockBackend {
        fn new(name: &'static str, lanes: usize, state: Arc<MockState>) -> Self {
            Self { name, lanes, state }
        }
    }

    impl PowBackend for MockBackend {
        fn name(&self) -> &'static str {
            self.name
        }

        fn lanes(&self) -> usize {
            self.lanes
        }

        fn set_instance_id(&mut self, _id: BackendInstanceId) {}

        fn set_event_sink(&mut self, _sink: Sender<BackendEvent>) {}

        fn start(&mut self) -> Result<()> {
            if self.state.fail_start.load(Ordering::Acquire) {
                return Err(anyhow!("injected start failure"));
            }
            Ok(())
        }

        fn stop(&mut self) {
            self.state.stop_calls.fetch_add(1, Ordering::Relaxed);
        }

        fn assign_work(&self, work: WorkAssignment) -> Result<()> {
            self.state.assign_calls.fetch_add(1, Ordering::Relaxed);
            if self.state.fail_next_assign.swap(false, Ordering::AcqRel) {
                return Err(anyhow!("injected assign failure"));
            }
            *self
                .state
                .last_lease
                .lock()
                .expect("mock lease lock should not be poisoned") = Some(work.nonce_lease);
            Ok(())
        }
    }

    fn slot(id: BackendInstanceId, lanes: u64, backend: Box<dyn PowBackend>) -> BackendSlot {
        BackendSlot {
            id,
            backend,
            lane_offset: 0,
            lanes,
        }
    }

    #[test]
    fn next_work_id_wraps_within_valid_range() {
        let mut id = WORK_ID_MAX;
        assert_eq!(next_work_id(&mut id), WORK_ID_MAX);
        assert_eq!(next_work_id(&mut id), 1);
    }

    #[test]
    fn total_lanes_never_zero() {
        assert_eq!(total_lanes(&[]), 1);
    }

    #[test]
    fn header_base_len_matches_pow_spec() {
        assert_eq!(POW_HEADER_BASE_LEN, 92);
    }

    #[test]
    fn remove_backend_by_id_only_removes_target_instance() {
        let first_state = Arc::new(MockState::default());
        let second_state = Arc::new(MockState::default());

        let mut backends = vec![
            slot(
                11,
                1,
                Box::new(MockBackend::new("nvidia", 1, Arc::clone(&first_state))),
            ),
            slot(
                22,
                1,
                Box::new(MockBackend::new("nvidia", 1, Arc::clone(&second_state))),
            ),
        ];
        recalculate_lane_offsets(&mut backends);

        assert!(remove_backend_by_id(&mut backends, 22));
        assert_eq!(backends.len(), 1);
        assert_eq!(backends[0].id, 11);
        assert_eq!(first_state.stop_calls.load(Ordering::Relaxed), 0);
        assert_eq!(second_state.stop_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn distribute_work_quarantines_assignment_failures_and_reassigns_lanes() {
        let first_state = Arc::new(MockState::default());
        let failed_state = Arc::new(MockState::default());
        let third_state = Arc::new(MockState::default());
        failed_state.fail_next_assign.store(true, Ordering::Relaxed);

        let mut backends = vec![
            slot(
                1,
                2,
                Box::new(MockBackend::new("cpu", 2, Arc::clone(&first_state))),
            ),
            slot(
                2,
                1,
                Box::new(MockBackend::new("nvidia", 1, Arc::clone(&failed_state))),
            ),
            slot(
                3,
                1,
                Box::new(MockBackend::new("nvidia", 1, Arc::clone(&third_state))),
            ),
        ];
        recalculate_lane_offsets(&mut backends);

        distribute_work(
            &mut backends,
            1,
            1,
            Arc::from(vec![7u8; POW_HEADER_BASE_LEN]),
            [0xFF; 32],
            NonceReservation {
                start_nonce: 100,
                max_iters_per_lane: 10,
            },
            Instant::now() + Duration::from_secs(1),
        )
        .expect("distribution should continue after quarantining failing backend");

        assert_eq!(backends.len(), 2);
        assert_eq!(backends[0].id, 1);
        assert_eq!(backends[1].id, 3);
        assert_eq!(backends[0].lane_offset, 0);
        assert_eq!(backends[1].lane_offset, 2);

        let first_lease = first_state
            .last_lease
            .lock()
            .expect("first lease lock should not be poisoned")
            .expect("first backend should receive work");
        let third_lease = third_state
            .last_lease
            .lock()
            .expect("third lease lock should not be poisoned")
            .expect("third backend should receive work");

        assert_eq!(first_lease.global_stride, 3);
        assert_eq!(first_lease.lane_offset, 0);
        assert_eq!(third_lease.global_stride, 3);
        assert_eq!(third_lease.lane_offset, 2);
        assert_eq!(failed_state.stop_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn activate_backends_stops_backend_when_start_fails() {
        let failed_state = Arc::new(MockState::default());
        let healthy_state = Arc::new(MockState::default());
        failed_state.fail_start.store(true, Ordering::Relaxed);

        let instances: Vec<Box<dyn PowBackend>> = vec![
            Box::new(MockBackend::new("cpu", 1, Arc::clone(&failed_state))),
            Box::new(MockBackend::new("cpu", 1, Arc::clone(&healthy_state))),
        ];

        let (active, _events) = activate_backends(instances)
            .expect("activation should continue with the healthy backend");

        assert_eq!(active.len(), 1);
        assert_eq!(failed_state.stop_calls.load(Ordering::Relaxed), 1);
        assert_eq!(healthy_state.stop_calls.load(Ordering::Relaxed), 0);
    }
}
