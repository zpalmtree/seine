//! Temporary experiment instrumentation. Not a production allocation policy.
use std::collections::BTreeMap;

const MAX_NODES: usize = 1024;
const WORDS: usize = MAX_NODES / usize::BITS as usize;

pub(super) struct Policy {
    label: String,
    cpu: i32,
    local_node: usize,
    target: Option<usize>,
    old_mode: i32,
    old_mask: [usize; WORDS],
}

impl Policy {
    pub(super) fn begin() -> Option<Self> {
        let label = std::env::var("SEINE_EXPERIMENT_ARENA_POLICY").ok()?;
        assert!(matches!(label.as_str(), "audit" | "local" | "remote"));
        let cpu = unsafe { libc::sched_getcpu() };
        assert!(cpu >= 0);
        let local_node = std::fs::read_dir(format!("/sys/devices/system/cpu/cpu{cpu}"))
            .unwrap()
            .filter_map(|entry| {
                entry
                    .ok()?
                    .file_name()
                    .to_str()?
                    .strip_prefix("node")?
                    .parse::<usize>()
                    .ok()
            })
            .next()
            .expect("CPU must have a NUMA node");
        let status = std::fs::read_to_string("/proc/thread-self/status").unwrap();
        let allowed = status
            .lines()
            .find_map(|line| line.strip_prefix("Mems_allowed_list:\t"))
            .unwrap();
        let nodes: Vec<usize> = allowed
            .split(',')
            .flat_map(|part| {
                let mut range = part.trim().split('-').map(|n| n.parse::<usize>().unwrap());
                let start = range.next().unwrap();
                start..=range.next().unwrap_or(start)
            })
            .collect();
        assert!(nodes.contains(&local_node));
        let target = match label.as_str() {
            "local" => Some(local_node),
            "remote" => Some(
                *nodes
                    .iter()
                    .find(|&&n| n != local_node)
                    .expect("remote needs two allowed nodes"),
            ),
            _ => None,
        };
        let mut policy = Self {
            label,
            cpu,
            local_node,
            target,
            old_mode: 0,
            old_mask: [0; WORDS],
        };
        if let Some(node) = target {
            assert!(node < MAX_NODES);
            let rc = unsafe {
                libc::syscall(
                    libc::SYS_get_mempolicy,
                    &mut policy.old_mode,
                    policy.old_mask.as_mut_ptr(),
                    MAX_NODES,
                    0usize,
                    0usize,
                )
            };
            assert_eq!(rc, 0, "get_mempolicy: {}", std::io::Error::last_os_error());
            let mut mask = [0usize; WORDS];
            mask[node / usize::BITS as usize] |= 1 << (node % usize::BITS as usize);
            let rc =
                unsafe { libc::syscall(libc::SYS_set_mempolicy, 2i32, mask.as_ptr(), MAX_NODES) };
            assert_eq!(rc, 0, "set_mempolicy: {}", std::io::Error::last_os_error());
        }
        Some(policy)
    }

    pub(super) fn observe(&self, address: *mut u8, bytes: usize, worker: usize) {
        // Sample one resident address per 2 MiB. This reports sampled placement,
        // not an exhaustive count of individual 4 KiB pages.
        let pages: Vec<*mut libc::c_void> = (0..bytes)
            .step_by(2 * 1024 * 1024)
            .map(|offset| unsafe { address.add(offset).cast() })
            .collect();
        let mut status = vec![-999i32; pages.len()];
        let rc = unsafe {
            libc::syscall(
                libc::SYS_move_pages,
                0i32,
                pages.len(),
                pages.as_ptr(),
                std::ptr::null::<i32>(),
                status.as_mut_ptr(),
                0i32,
            )
        };
        let error = if rc < 0 {
            Some(std::io::Error::last_os_error().to_string())
        } else {
            None
        };
        let mut counts = BTreeMap::new();
        for node in status {
            *counts.entry(node).or_insert(0usize) += 1;
        }
        eprintln!(
            "EPYC_ARENA {}",
            serde_json::json!({
                "worker": worker, "policy": self.label, "cpu": self.cpu,
                "local_node": self.local_node, "target_node": self.target,
                "address": address as usize, "bytes": bytes, "stride": 2 * 1024 * 1024,
                "samples_by_node": counts, "query_result": rc, "query_error": error
            })
        );
    }
}

impl Drop for Policy {
    fn drop(&mut self) {
        if self.target.is_some() {
            let rc = unsafe {
                libc::syscall(
                    libc::SYS_set_mempolicy,
                    self.old_mode,
                    self.old_mask.as_ptr(),
                    MAX_NODES,
                )
            };
            assert_eq!(rc, 0, "restore policy: {}", std::io::Error::last_os_error());
        }
    }
}
