use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use blocknet_pow_spec::CPU_LANE_MEMORY_BYTES;
use crossbeam_channel::{bounded, Receiver, Sender};
use sysinfo::System;

use crate::backend::cpu::{CpuBackend, CpuBackendTuning};
use crate::backend::{
    AssignmentSemantics, BackendCapabilities, BackendEvent, BackendExecutionModel,
    BackendInstanceId, BackendTelemetry, BenchBackend, DeadlineSupport, MiningSolution, NonceChunk,
    PowBackend, PreemptionGranularity, WorkAssignment, WorkTemplate,
};
use crate::config::CpuAffinityMode;

const BACKEND_NAME: &str = "nvidia";
const EVENT_FORWARD_CAPACITY: usize = 1024;
const MAX_RECOMMENDED_LANES: usize = 8;

#[derive(Debug, Clone)]
struct NvidiaDeviceInfo {
    index: u32,
    name: String,
    memory_total_mib: u64,
}

pub struct NvidiaBackend {
    instance_id: AtomicU64,
    requested_device_index: Option<u32>,
    resolved_device: RwLock<Option<NvidiaDeviceInfo>>,
    event_sink: Arc<RwLock<Option<Sender<BackendEvent>>>>,
    event_forward_handle: Mutex<Option<JoinHandle<()>>>,
    inner: CpuBackend,
}

impl NvidiaBackend {
    pub fn new(device_index: Option<u32>) -> Self {
        let host_threads = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(1)
            .max(1);
        let memory_budget = detect_available_memory_budget_bytes();
        let estimated_instances = detect_nvidia_instance_count_for_sizing().max(1);
        let lanes = recommend_worker_lanes(host_threads, memory_budget, estimated_instances);

        let inner = CpuBackend::with_tuning(lanes, CpuAffinityMode::Auto, throughput_tuning(lanes));

        let event_sink = Arc::new(RwLock::new(None));
        let (inner_event_tx, inner_event_rx) = bounded::<BackendEvent>(EVENT_FORWARD_CAPACITY);
        inner.set_event_sink(inner_event_tx);
        let event_forward_handle =
            spawn_event_forwarder(Arc::clone(&event_sink), inner_event_rx, BACKEND_NAME);

        Self {
            instance_id: AtomicU64::new(0),
            requested_device_index: device_index,
            resolved_device: RwLock::new(None),
            event_sink,
            event_forward_handle: Mutex::new(Some(event_forward_handle)),
            inner,
        }
    }

    fn validate_or_select_device(&self) -> Result<NvidiaDeviceInfo> {
        let devices = query_nvidia_devices()?;
        let selected = if let Some(requested_index) = self.requested_device_index {
            devices
                .iter()
                .find(|device| device.index == requested_index)
                .cloned()
                .ok_or_else(|| {
                    let available = devices
                        .iter()
                        .map(|device| device.index.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    anyhow!(
                        "requested NVIDIA device index {} was not found; available indices: [{}]",
                        requested_index,
                        available
                    )
                })?
        } else {
            devices[0].clone()
        };

        let memory_total_bytes = selected.memory_total_mib.saturating_mul(1024 * 1024);
        if memory_total_bytes < CPU_LANE_MEMORY_BYTES {
            bail!(
                "NVIDIA device {} ({}) reports {} MiB VRAM; at least {} MiB is required",
                selected.index,
                selected.name,
                selected.memory_total_mib,
                (CPU_LANE_MEMORY_BYTES / (1024 * 1024)).max(1)
            );
        }

        if let Ok(mut slot) = self.resolved_device.write() {
            *slot = Some(selected.clone());
        }
        Ok(selected)
    }
}

impl Drop for NvidiaBackend {
    fn drop(&mut self) {
        let (tx, _rx) = bounded::<BackendEvent>(1);
        self.inner.set_event_sink(tx);
        if let Ok(mut slot) = self.event_forward_handle.lock() {
            if let Some(handle) = slot.take() {
                let _ = handle.join();
            }
        }
    }
}

impl PowBackend for NvidiaBackend {
    fn name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn lanes(&self) -> usize {
        self.inner.lanes()
    }

    fn set_instance_id(&self, id: BackendInstanceId) {
        self.instance_id.store(id, Ordering::Release);
        self.inner.set_instance_id(id);
    }

    fn set_event_sink(&self, sink: Sender<BackendEvent>) {
        if let Ok(mut slot) = self.event_sink.write() {
            *slot = Some(sink);
        }
    }

    fn start(&self) -> Result<()> {
        let selected = self.validate_or_select_device()?;
        self.inner.start().with_context(|| {
            format!(
                "failed to start NVIDIA backend for device {} ({})",
                selected.index, selected.name
            )
        })
    }

    fn stop(&self) {
        self.inner.stop();
    }

    fn assign_work(&self, work: WorkAssignment) -> Result<()> {
        self.inner.assign_work(work)
    }

    fn assign_work_batch(&self, work: &[WorkAssignment]) -> Result<()> {
        let Some(collapsed) = collapse_assignment_batch(work)? else {
            return Ok(());
        };
        self.inner.assign_work(collapsed)
    }

    fn assign_work_batch_with_deadline(
        &self,
        work: &[WorkAssignment],
        deadline: Instant,
    ) -> Result<()> {
        let Some(collapsed) = collapse_assignment_batch(work)? else {
            return Ok(());
        };
        self.inner.assign_work_with_deadline(&collapsed, deadline)
    }

    fn supports_assignment_batching(&self) -> bool {
        true
    }

    fn cancel_work(&self) -> Result<()> {
        self.inner.cancel_work()
    }

    fn request_timeout_interrupt(&self) -> Result<()> {
        self.inner.request_timeout_interrupt()
    }

    fn cancel_work_with_deadline(&self, deadline: Instant) -> Result<()> {
        self.inner.cancel_work_with_deadline(deadline)
    }

    fn fence(&self) -> Result<()> {
        self.inner.fence()
    }

    fn fence_with_deadline(&self, deadline: Instant) -> Result<()> {
        self.inner.fence_with_deadline(deadline)
    }

    fn take_hashes(&self) -> u64 {
        self.inner.take_hashes()
    }

    fn take_telemetry(&self) -> BackendTelemetry {
        self.inner.take_telemetry()
    }

    fn preemption_granularity(&self) -> PreemptionGranularity {
        self.inner.preemption_granularity()
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            preferred_iters_per_lane: Some(1 << 18),
            preferred_allocation_iters_per_lane: Some(1 << 20),
            preferred_hash_poll_interval: Some(Duration::from_millis(20)),
            preferred_assignment_timeout: None,
            preferred_control_timeout: None,
            preferred_assignment_timeout_strikes: None,
            preferred_worker_queue_depth: Some(16),
            max_inflight_assignments: 8,
            deadline_support: DeadlineSupport::Cooperative,
            assignment_semantics: AssignmentSemantics::Replace,
            execution_model: BackendExecutionModel::Blocking,
            nonblocking_poll_min: Some(Duration::from_micros(50)),
            nonblocking_poll_max: Some(Duration::from_millis(1)),
        }
    }

    fn bench_backend(&self) -> Option<&dyn BenchBackend> {
        Some(self)
    }
}

impl BenchBackend for NvidiaBackend {
    fn kernel_bench(&self, seconds: u64, shutdown: &AtomicBool) -> Result<u64> {
        self.validate_or_select_device()?;
        self.inner.kernel_bench(seconds, shutdown)
    }
}

fn throughput_tuning(lanes: usize) -> CpuBackendTuning {
    let lanes = lanes.max(1);
    let hash_batch_size = if lanes >= 8 {
        2048
    } else if lanes >= 4 {
        1024
    } else {
        512
    };
    let control_check_interval_hashes = if lanes >= 8 { 32 } else { 16 };
    let hash_flush_interval = if lanes >= 4 {
        Duration::from_millis(200)
    } else {
        Duration::from_millis(150)
    };
    let event_dispatch_capacity = (lanes.saturating_mul(256)).clamp(256, 4096);

    CpuBackendTuning {
        hash_batch_size,
        control_check_interval_hashes,
        hash_flush_interval,
        event_dispatch_capacity,
    }
}

fn recommend_worker_lanes(
    host_threads: usize,
    available_memory_bytes: Option<u64>,
    nvidia_instance_count: usize,
) -> usize {
    let instances = nvidia_instance_count.max(1);
    let host_threads = host_threads.max(1);
    let host_per_instance = host_threads.saturating_div(instances).max(1);

    let memory_total_lanes = available_memory_bytes
        .map(|available| {
            let usable = available.saturating_mul(3) / 4;
            usable.saturating_div(CPU_LANE_MEMORY_BYTES).max(1)
        })
        .unwrap_or(host_threads as u64);
    let memory_per_instance = (memory_total_lanes / instances as u64).max(1) as usize;

    host_per_instance
        .min(memory_per_instance)
        .max(1)
        .min(MAX_RECOMMENDED_LANES)
}

fn detect_nvidia_instance_count_for_sizing() -> usize {
    match query_nvidia_devices() {
        Ok(devices) => devices.len().max(1),
        Err(_) => 1,
    }
}

fn detect_available_memory_budget_bytes() -> Option<u64> {
    let mut sys = System::new();
    sys.refresh_memory();

    let total = sys.total_memory();
    if total == 0 {
        return None;
    }

    let mut available = sys.available_memory().min(total);
    if available == 0 {
        available = total;
    }

    if let Some(cgroup) = sys.cgroup_limits() {
        if cgroup.total_memory > 0 {
            available = available.min(cgroup.total_memory);
        }
        if cgroup.free_memory > 0 {
            available = available.min(cgroup.free_memory);
        }
    }

    Some(available.max(CPU_LANE_MEMORY_BYTES))
}

fn collapse_assignment_batch(work: &[WorkAssignment]) -> Result<Option<WorkAssignment>> {
    let Some(first) = work.first() else {
        return Ok(None);
    };
    if work.len() == 1 {
        return Ok(Some(first.clone()));
    }

    let mut expected_start = first
        .nonce_chunk
        .start_nonce
        .wrapping_add(first.nonce_chunk.nonce_count);
    let mut nonce_count = first.nonce_chunk.nonce_count;

    for (idx, assignment) in work.iter().enumerate().skip(1) {
        ensure_compatible_template(&first.template, &assignment.template).with_context(|| {
            format!(
                "assignment batch item {} references a different template",
                idx + 1
            )
        })?;
        if assignment.nonce_chunk.start_nonce != expected_start {
            bail!(
                "assignment batch is not contiguous at item {} (expected nonce {}, got {})",
                idx + 1,
                expected_start,
                assignment.nonce_chunk.start_nonce
            );
        }

        nonce_count = nonce_count
            .checked_add(assignment.nonce_chunk.nonce_count)
            .ok_or_else(|| anyhow!("assignment batch nonce_count overflow"))?;
        expected_start = expected_start.wrapping_add(assignment.nonce_chunk.nonce_count);
    }

    Ok(Some(WorkAssignment {
        template: Arc::clone(&first.template),
        nonce_chunk: NonceChunk {
            start_nonce: first.nonce_chunk.start_nonce,
            nonce_count,
        },
    }))
}

fn ensure_compatible_template(first: &Arc<WorkTemplate>, second: &Arc<WorkTemplate>) -> Result<()> {
    if first.work_id != second.work_id {
        bail!(
            "mismatched work ids ({} vs {})",
            first.work_id,
            second.work_id
        );
    }
    if first.epoch != second.epoch {
        bail!("mismatched epochs ({} vs {})", first.epoch, second.epoch);
    }
    if first.target != second.target {
        bail!("mismatched target");
    }
    if first.stop_at != second.stop_at {
        bail!("mismatched stop_at");
    }
    if first.header_base.as_ref() != second.header_base.as_ref() {
        bail!("mismatched header_base");
    }
    Ok(())
}

fn spawn_event_forwarder(
    sink: Arc<RwLock<Option<Sender<BackendEvent>>>>,
    rx: Receiver<BackendEvent>,
    backend_name: &'static str,
) -> JoinHandle<()> {
    thread::spawn(move || {
        while let Ok(event) = rx.recv() {
            let translated = match event {
                BackendEvent::Solution(solution) => BackendEvent::Solution(MiningSolution {
                    backend: backend_name,
                    ..solution
                }),
                BackendEvent::Error {
                    backend_id,
                    message,
                    ..
                } => BackendEvent::Error {
                    backend_id,
                    backend: backend_name,
                    message,
                },
            };

            let outbound = match sink.read() {
                Ok(slot) => slot.clone(),
                Err(_) => None,
            };
            let Some(outbound) = outbound else {
                continue;
            };
            if outbound.send(translated).is_err() {
                break;
            }
        }
    })
}

fn query_nvidia_devices() -> Result<Vec<NvidiaDeviceInfo>> {
    let output = Command::new("nvidia-smi")
        .args([
            "--query-gpu=index,name,memory.total",
            "--format=csv,noheader,nounits",
        ])
        .output()
        .context("failed to execute nvidia-smi; ensure NVIDIA drivers are installed")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stderr.is_empty() {
            bail!(
                "nvidia-smi returned non-zero exit status ({})",
                output.status
            );
        }
        bail!("nvidia-smi query failed: {stderr}");
    }

    let stdout = String::from_utf8(output.stdout).context("nvidia-smi output was not UTF-8")?;
    let devices = parse_nvidia_smi_query_output(&stdout)?;
    if devices.is_empty() {
        bail!("nvidia-smi reported no NVIDIA devices");
    }
    Ok(devices)
}

fn parse_nvidia_smi_query_output(raw: &str) -> Result<Vec<NvidiaDeviceInfo>> {
    let mut devices = Vec::new();
    for (line_idx, raw_line) in raw.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        let columns = line.split(',').map(str::trim).collect::<Vec<_>>();
        if columns.len() < 3 {
            bail!(
                "unexpected nvidia-smi output at line {}: '{line}'",
                line_idx + 1
            );
        }

        let index = columns[0].parse::<u32>().with_context(|| {
            format!(
                "invalid GPU index '{}' at line {}",
                columns[0],
                line_idx + 1
            )
        })?;
        let memory_total_mib = columns[columns.len() - 1].parse::<u64>().with_context(|| {
            format!(
                "invalid GPU memory value '{}' at line {}",
                columns[columns.len() - 1],
                line_idx + 1
            )
        })?;
        let name = columns[1..columns.len() - 1].join(",");
        let name = name.trim().to_string();
        if name.is_empty() {
            bail!("missing GPU name at line {}", line_idx + 1);
        }

        devices.push(NvidiaDeviceInfo {
            index,
            name,
            memory_total_mib,
        });
    }
    Ok(devices)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_template(work_id: u64) -> Arc<WorkTemplate> {
        Arc::new(WorkTemplate {
            work_id,
            epoch: 7,
            header_base: Arc::<[u8]>::from(vec![1u8; 92]),
            target: [0xff; 32],
            stop_at: Instant::now() + Duration::from_secs(30),
        })
    }

    #[test]
    fn parse_nvidia_smi_query_output_parses_multiple_rows() {
        let parsed = parse_nvidia_smi_query_output(
            "0, NVIDIA GeForce RTX 3080, 10240\n1, NVIDIA RTX A4000, 16384\n",
        )
        .expect("query output should parse");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].index, 0);
        assert_eq!(parsed[0].name, "NVIDIA GeForce RTX 3080");
        assert_eq!(parsed[0].memory_total_mib, 10_240);
        assert_eq!(parsed[1].index, 1);
        assert_eq!(parsed[1].name, "NVIDIA RTX A4000");
        assert_eq!(parsed[1].memory_total_mib, 16_384);
    }

    #[test]
    fn parse_nvidia_smi_query_output_rejects_invalid_rows() {
        let err =
            parse_nvidia_smi_query_output("abc, RTX, 8192").expect_err("invalid index should fail");
        assert!(format!("{err:#}").contains("invalid GPU index"));
    }

    #[test]
    fn recommend_worker_lanes_respects_memory_and_instances() {
        assert_eq!(
            recommend_worker_lanes(16, Some(4 * 1024 * 1024 * 1024), 1),
            1
        );
        assert_eq!(
            recommend_worker_lanes(16, Some(64 * 1024 * 1024 * 1024), 2),
            8
        );
        assert_eq!(
            recommend_worker_lanes(3, Some(64 * 1024 * 1024 * 1024), 1),
            3
        );
    }

    #[test]
    fn collapse_assignment_batch_merges_contiguous_chunks() {
        let template = test_template(11);
        let assignments = vec![
            WorkAssignment {
                template: Arc::clone(&template),
                nonce_chunk: NonceChunk {
                    start_nonce: 100,
                    nonce_count: 4,
                },
            },
            WorkAssignment {
                template,
                nonce_chunk: NonceChunk {
                    start_nonce: 104,
                    nonce_count: 6,
                },
            },
        ];

        let collapsed = collapse_assignment_batch(&assignments)
            .expect("batch collapse should succeed")
            .expect("batch should not be empty");
        assert_eq!(collapsed.nonce_chunk.start_nonce, 100);
        assert_eq!(collapsed.nonce_chunk.nonce_count, 10);
    }

    #[test]
    fn collapse_assignment_batch_rejects_non_contiguous_chunks() {
        let template = test_template(11);
        let assignments = vec![
            WorkAssignment {
                template: Arc::clone(&template),
                nonce_chunk: NonceChunk {
                    start_nonce: 100,
                    nonce_count: 4,
                },
            },
            WorkAssignment {
                template,
                nonce_chunk: NonceChunk {
                    start_nonce: 105,
                    nonce_count: 6,
                },
            },
        ];

        let err =
            collapse_assignment_batch(&assignments).expect_err("non-contiguous chunks should fail");
        assert!(format!("{err:#}").contains("not contiguous"));
    }
}
