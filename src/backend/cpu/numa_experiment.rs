//! Research-only controls for allocation locality, not a shipping policy.
//! Every allocation/reallocation restores the calling thread's memory policy.
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::kernel::PowArena;

const STRIDE: usize = 2 * 1024 * 1024;
const MPOL_DEFAULT: i32 = 0;
const MPOL_BIND: i32 = 2;

fn ids(text: &str) -> Result<BTreeSet<usize>, String> {
    let mut result = BTreeSet::new();
    for part in text.trim().split(',') {
        let numbers: Result<Vec<usize>, _> = part.split('-').map(str::parse).collect();
        let numbers = numbers.map_err(|_| format!("invalid node/CPU list: {text:?}"))?;
        let (start, end) = match numbers.as_slice() {
            [one] => (*one, *one),
            [start, end] if start <= end => (*start, *end),
            _ => return Err(format!("invalid node/CPU range: {part:?}")),
        };
        // Bound malformed inputs without limiting normal Linux CPU/node IDs.
        if end >= 1_048_576 {
            return Err("node/CPU ID exceeds experiment bound".into());
        }
        result.extend(start..=end);
    }
    if result.is_empty() {
        return Err("empty node/CPU list".into());
    }
    Ok(result)
}

fn status_ids(status: &str, key: &str) -> Result<BTreeSet<usize>, String> {
    ids(status
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .ok_or_else(|| format!("missing {key}"))?
        .trim())
}

fn os_error(context: &str) -> String {
    format!("{context}: {}", std::io::Error::last_os_error())
}

struct Policy {
    mode: i32,
    mask: Vec<usize>,
    changed: bool,
}

impl Policy {
    fn capture() -> Result<Self, String> {
        let possible = std::fs::read_to_string("/sys/devices/system/node/possible")
            .map_err(|e| e.to_string())?;
        let last = *ids(&possible)?.last().expect("nonempty list");
        let mut policy = Self {
            mode: 0,
            mask: vec![0; (last + 1).div_ceil(usize::BITS as usize).max(1)],
            changed: false,
        };
        let maxnode = policy.maxnode();
        let rc = unsafe {
            libc::syscall(
                libc::SYS_get_mempolicy,
                &mut policy.mode,
                policy.mask.as_mut_ptr(),
                maxnode,
                0usize,
                0usize,
            )
        };
        if rc != 0 {
            return Err(os_error("get_mempolicy"));
        }
        Ok(policy)
    }

    fn maxnode(&self) -> usize {
        self.mask.len() * usize::BITS as usize
    }

    fn bind(&mut self, node: usize) -> Result<(), String> {
        // Do not silently override numactl/cgroup/user policy in the candidate.
        // Explicit remote controls also require a default inherited policy.
        if self.mode != MPOL_DEFAULT {
            return Err(format!(
                "existing memory policy {} must be preserved",
                self.mode
            ));
        }
        if node >= self.maxnode() {
            return Err("target node exceeds kernel possible-node mask".into());
        }
        let mut mask = vec![0usize; self.mask.len()];
        mask[node / usize::BITS as usize] |= 1usize << (node % usize::BITS as usize);
        let rc = unsafe {
            libc::syscall(
                libc::SYS_set_mempolicy,
                MPOL_BIND,
                mask.as_ptr(),
                self.maxnode(),
            )
        };
        if rc != 0 {
            return Err(os_error("set_mempolicy(MPOL_BIND)"));
        }
        self.changed = true;
        Ok(())
    }

    fn restore(&mut self) -> Result<(), String> {
        if !self.changed {
            return Ok(());
        }
        let rc = unsafe {
            libc::syscall(
                libc::SYS_set_mempolicy,
                self.mode,
                self.mask.as_ptr(),
                self.maxnode(),
            )
        };
        if rc != 0 {
            return Err(os_error("restore set_mempolicy"));
        }
        self.changed = false;
        Ok(())
    }
}

impl Drop for Policy {
    fn drop(&mut self) {
        // Also restore on unwinding; do not double-panic if the syscall fails.
        if let Err(error) = self.restore() {
            eprintln!("NUMA_EXPERIMENT_RESTORE_ERROR {error}");
        }
    }
}

pub(super) fn allocate(
    worker: usize,
    allocate: impl FnOnce() -> Result<PowArena, String>,
) -> Result<PowArena, String> {
    let label = match std::env::var("SEINE_EXPERIMENT_ARENA_POLICY") {
        Ok(label) => label,
        Err(std::env::VarError::NotPresent) => return allocate(),
        Err(error) => return Err(error.to_string()),
    };
    allocate_for_policy(worker, &label, allocate)
}

fn allocate_for_policy(
    worker: usize,
    label: &str,
    allocate: impl FnOnce() -> Result<PowArena, String>,
) -> Result<PowArena, String> {
    if !matches!(label, "audit" | "local" | "remote") {
        return Err(format!("unknown experiment arena policy: {label}"));
    }
    let status = std::fs::read_to_string("/proc/thread-self/status").map_err(|e| e.to_string())?;
    let cpus = status_ids(&status, "Cpus_allowed_list:")?;
    let nodes = status_ids(&status, "Mems_allowed_list:")?;
    let cpu = unsafe { libc::sched_getcpu() };
    if cpu < 0 || cpus != BTreeSet::from([cpu as usize]) {
        return Err("NUMA experiment requires successful single-CPU worker pinning".into());
    }
    let entries = std::fs::read_dir(format!("/sys/devices/system/cpu/cpu{cpu}"))
        .map_err(|e| e.to_string())?;
    let cpu_nodes: Vec<usize> = entries
        .filter_map(|e| {
            e.ok()?
                .file_name()
                .to_str()?
                .strip_prefix("node")?
                .parse()
                .ok()
        })
        .collect();
    if cpu_nodes.len() != 1 || !nodes.contains(&cpu_nodes[0]) {
        return Err("worker CPU lacks one allowed local memory node".into());
    }
    let local = cpu_nodes[0];
    let target = match label {
        "local" => Some(local),
        "remote" => Some(
            *nodes
                .iter()
                .find(|&&n| n != local)
                .ok_or("remote needs two nodes")?,
        ),
        _ => None,
    };
    let mut policy = Policy::capture()?;
    if let Some(node) = target {
        policy.bind(node)?;
    }
    let started = std::time::Instant::now();
    let allocated = allocate();
    let allocation_seconds = started.elapsed().as_secs_f64();
    // Restore before propagating an allocation error or doing any observation.
    policy.restore()?;
    let mut arena = allocated?;
    let blocks = arena.as_mut_slice();
    let bytes = std::mem::size_of_val(blocks);
    let address = blocks.as_mut_ptr().cast::<u8>();
    let pages: Vec<*mut libc::c_void> = (0..bytes)
        .step_by(STRIDE)
        .map(|offset| unsafe { address.add(offset).cast() })
        .collect();
    let mut locations = vec![-999i32; pages.len()];
    let rc = unsafe {
        libc::syscall(
            libc::SYS_move_pages,
            0i32,
            pages.len(),
            pages.as_ptr(),
            std::ptr::null::<i32>(),
            locations.as_mut_ptr(),
            0i32,
        )
    };
    if rc != 0 || locations.iter().any(|&n| n < 0) {
        return Err(format!(
            "placement query failed: rc={rc}, locations={locations:?}, {}",
            os_error("move_pages")
        ));
    }
    if unsafe { libc::sched_getcpu() } != cpu {
        return Err("worker CPU changed during allocation observation".into());
    }
    let mut counts = BTreeMap::new();
    for node in locations {
        *counts.entry(node).or_insert(0usize) += 1;
    }
    let node_meminfo = |node| {
        std::fs::read_to_string(Path::new(&format!(
            "/sys/devices/system/node/node{node}/meminfo"
        )))
        .unwrap_or_default()
    };
    eprintln!(
        "NUMA_ARENA {}",
        serde_json::json!({
            "schema": 1, "worker": worker, "policy": label, "cpu": cpu,
            "local_node": local, "target_node": target, "bytes": bytes,
            "stride_bytes": STRIDE, "samples_by_node": counts,
            "sampled_placement_only": true, "inherited_policy": policy.mode,
            "policy_restored": true, "allocation_seconds": allocation_seconds,
            "local_node_meminfo": node_meminfo(local),
        })
    );
    Ok(arena)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_ids_and_masks_do_not_assume_dense_nodes() {
        assert_eq!(ids("0-2,7,65").unwrap(), BTreeSet::from([0, 1, 2, 7, 65]));
        assert_eq!(
            status_ids("Mems_allowed_list:\t2,65\n", "Mems_allowed_list:").unwrap(),
            BTreeSet::from([2, 65])
        );
        for bad in ["", "3-1", "1-2-3", "1,", "1048576"] {
            assert!(ids(bad).is_err(), "accepted {bad:?}");
        }
    }

    #[test]
    #[ignore = "native Linux NUMA syscalls required; runs on an isolated thread"]
    fn native_policy_restores_on_success_error_and_unwind() {
        std::thread::spawn(|| {
            let original = Policy::capture().unwrap();
            assert_eq!(original.mode, MPOL_DEFAULT);
            let status = std::fs::read_to_string("/proc/thread-self/status").unwrap();
            let node = *status_ids(&status, "Mems_allowed_list:")
                .unwrap()
                .first()
                .unwrap();
            for outcome in 0..3 {
                let result = std::panic::catch_unwind(|| -> Result<(), String> {
                    let mut guard = Policy::capture()?;
                    guard.bind(node)?;
                    assert_eq!(Policy::capture()?.mode, MPOL_BIND);
                    if outcome == 1 {
                        return Err("injected allocation failure".into());
                    }
                    if outcome == 2 {
                        panic!("injected unwinding");
                    }
                    guard.restore()
                });
                assert_eq!(result.is_err(), outcome == 2);
                if let Ok(value) = result {
                    assert_eq!(value.is_err(), outcome == 1);
                }
                let restored = Policy::capture().unwrap();
                assert_eq!(restored.mode, original.mode);
                assert_eq!(restored.mask, original.mask);
            }
        })
        .join()
        .unwrap();
    }

    #[test]
    #[ignore = "full 2 GiB parity; optimized isolated process, one pinned worker"]
    fn full_size_local_arena_reference_parity() {
        std::thread::spawn(|| {
            use argon2::{Algorithm, Argon2, Params, Version};
            use blocknet_pow_kernel::{CpuKernel, FixedArgon2id};
            use crate::config::CpuPageMode;

            const KIB: u32 = 2 * 1024 * 1024;
            let cpu = core_affinity::get_core_ids().unwrap()[0];
            assert!(core_affinity::set_for_current(cpu));
            let oracle = Argon2::new(Algorithm::Argon2id, Version::V0x13,
                Params::new(KIB, 1, 1, Some(32)).unwrap());
            let salt = b"numa-full-size-reference";
            let mut references = [[0u8; 32]; 3];
            for (nonce, output) in references.iter_mut().enumerate() {
                oracle.hash_password_into(&(nonce as u64).to_le_bytes(), salt, output).unwrap();
            }
            for policy in ["audit", "local"] {
                let mut arena = allocate_for_policy(0, policy, || {
                    PowArena::new(KIB as usize, CpuPageMode::Regular)
                }).unwrap();
                for kernel in CpuKernel::ALL {
                    if !kernel.is_supported() { continue; }
                    let hasher = FixedArgon2id::with_kernel(KIB, kernel).unwrap();
                    for nonce in [0, 1, 2, 0] {
                        let mut output = [0u8; 32];
                        hasher.hash_password_into_with_memory(&(nonce as u64).to_le_bytes(),
                            salt, &mut output, arena.as_mut_slice()).unwrap();
                        assert_eq!(output, references[nonce], "{policy} {kernel} nonce={nonce}");
                    }
                    println!("NUMA_PARITY policy={policy} kernel={kernel} full_2gib=true dirty_reuse=true PASS");
                }
            }
        }).join().unwrap();
    }
}
