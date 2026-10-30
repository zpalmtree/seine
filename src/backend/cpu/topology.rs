//! Linux NUMA, L3 and physical-core placement, constrained to the process CPU/memory
//! masks. Incomplete cache data retains NUMA and physical-core placement; incomplete
//! NUMA/core data retains OS order.
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Topology {
    // Node ID -> physical cores (optionally L3-interleaved) -> allowed SMT siblings.
    nodes: BTreeMap<usize, Vec<Vec<usize>>>,
    cache_spread: bool,
    // Retain sharing domains in the autotune fingerprint even when ordering is
    // unchanged (for example, one eligible core per cache).
    l3_groups: BTreeSet<BTreeSet<usize>>,
}

impl Topology {
    pub(super) fn ordered_cpus(&self) -> Option<Vec<usize>> {
        let max_cores = self.nodes.values().map(Vec::len).max()?;
        let max_siblings = self.nodes.values().flatten().map(Vec::len).max()?;
        let mut order = Vec::new();
        for sibling in 0..max_siblings {
            for core in 0..max_cores {
                for cores in self.nodes.values() {
                    if let Some(cpu) = cores.get(core).and_then(|ids| ids.get(sibling)) {
                        order.push(*cpu);
                    }
                }
            }
        }
        Some(order)
    }

    fn spread_l3(&mut self, caches: &BTreeMap<usize, BTreeSet<usize>>) -> Option<()> {
        let allowed: BTreeSet<_> = self.nodes.values().flatten().flatten().copied().collect();
        if caches.keys().copied().collect::<BTreeSet<_>>() != allowed {
            return None;
        }
        for (&cpu, group) in caches {
            if !group.contains(&cpu) || group.iter().any(|member| caches.get(member) != Some(group))
            {
                return None;
            }
        }
        // Build separately so any inconsistent core/cache relationship leaves
        // every node in its original order. A cache can span NUMA nodes (SNC);
        // keep those node boundaries even when their cache masks are identical.
        let mut nodes = BTreeMap::new();
        let mut cache_spread = false;
        for (&node, cores) in &self.nodes {
            let mut groups = BTreeMap::<usize, Vec<Vec<usize>>>::new();
            for core in cores {
                let cache = caches.get(core.first()?)?;
                if core.iter().any(|cpu| caches.get(cpu) != Some(cache)) {
                    return None;
                }
                groups
                    .entry(*cache.first()?)
                    .or_default()
                    .push(core.clone());
            }
            cache_spread |= groups.len() > 1;
            let mut spread = Vec::with_capacity(cores.len());
            for index in 0..groups.values().map(Vec::len).max()? {
                for group in groups.values() {
                    if let Some(core) = group.get(index) {
                        spread.push(core.clone());
                    }
                }
            }
            nodes.insert(node, spread);
        }
        self.nodes = nodes;
        self.cache_spread = cache_spread;
        self.l3_groups = caches.values().cloned().collect();
        Some(())
    }
}

pub(super) fn detect(allowed: &BTreeSet<usize>) -> Option<Topology> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let mems = parse_cpu_list(
        status
            .lines()
            .find_map(|line| line.strip_prefix("Mems_allowed_list:"))?,
    )?;
    read_topology(Path::new("/sys/devices/system"), allowed, &mems)
}

/// Telemetry requires complete measured topology, unlike the placement/cache
/// fallback. Include the full memory mask even when it does not change ordering.
pub(super) fn measured_inputs(allowed: &BTreeSet<usize>) -> Option<String> {
    let before = current_mems()?;
    let result = measured_inputs_at(Path::new("/sys/devices/system"), allowed, &before)?;
    (current_mems()? == before).then_some(result)
}

fn current_mems() -> Option<BTreeSet<usize>> {
    let status = fs::read_to_string("/proc/thread-self/status").ok()?;
    parse_cpu_list(
        status
            .lines()
            .find_map(|line| line.strip_prefix("Mems_allowed_list:"))?,
    )
}

fn measured_inputs_at(
    root: &Path,
    allowed: &BTreeSet<usize>,
    mems: &BTreeSet<usize>,
) -> Option<String> {
    let topology = read_topology(root, allowed, mems)?;
    // Missing/contradictory L3 data still permits the existing runtime fallback,
    // but must not masquerade as a fully known telemetry cohort.
    (!topology.l3_groups.is_empty()).then(|| format!("{allowed:?}:{mems:?}:{topology:?}"))
}

fn read_topology(
    root: &Path,
    allowed: &BTreeSet<usize>,
    allowed_mems: &BTreeSet<usize>,
) -> Option<Topology> {
    let online = parse_cpu_list(&fs::read_to_string(root.join("node/online")).ok()?)?;
    let mut cpu_nodes = BTreeMap::new();
    for node in online {
        let path = root.join(format!("node/node{node}"));
        let cpus = parse_cpu_list(&fs::read_to_string(path.join("cpulist")).ok()?)?;
        let eligible = cpus.intersection(allowed).copied().collect::<Vec<_>>();
        if eligible.is_empty() {
            continue;
        }
        // CPU-only nodes and cpusets without local memory cannot use this policy.
        if !allowed_mems.contains(&node) {
            return None;
        }
        let meminfo = fs::read_to_string(path.join("meminfo")).ok()?;
        let memory_kib = meminfo.lines().find_map(|line| {
            let (_, value) = line.split_once("MemTotal:")?;
            value.split_whitespace().next()?.parse::<u64>().ok()
        })?;
        if memory_kib == 0 {
            return None;
        }
        for cpu in eligible {
            if cpu_nodes.insert(cpu, node).is_some() {
                return None;
            }
        }
    }
    if cpu_nodes.keys().copied().collect::<BTreeSet<_>>() != *allowed {
        return None;
    }
    let mut siblings = BTreeMap::new();
    for &cpu in allowed {
        let list =
            fs::read_to_string(root.join(format!("cpu/cpu{cpu}/topology/thread_siblings_list")))
                .ok()?;
        let group = parse_cpu_list(&list)?
            .intersection(allowed)
            .copied()
            .collect();
        siblings.insert(cpu, group);
    }
    let mut topology = build_topology(&cpu_nodes, &siblings)?;
    if let Some(caches) = read_l3_groups(root, allowed) {
        let _ = topology.spread_l3(&caches);
    }
    Some(topology)
}

fn read_l3_groups(
    root: &Path,
    allowed: &BTreeSet<usize>,
) -> Option<BTreeMap<usize, BTreeSet<usize>>> {
    let mut caches = BTreeMap::new();
    for &cpu in allowed {
        let mut group = None;
        for entry in fs::read_dir(root.join(format!("cpu/cpu{cpu}/cache"))).ok()? {
            let entry = entry.ok()?;
            if !entry.file_name().to_str()?.starts_with("index") {
                continue;
            }
            let path = entry.path();
            if fs::read_to_string(path.join("level")).ok()?.trim() != "3"
                || fs::read_to_string(path.join("type")).ok()?.trim() != "Unified"
            {
                continue;
            }
            let shared = parse_cpu_list(&fs::read_to_string(path.join("shared_cpu_list")).ok()?)?;
            // Cache IDs and index numbers are not globally unique. Use the
            // sharing mask and restrict it just like the physical-core mask.
            if group
                .replace(shared.intersection(allowed).copied().collect())
                .is_some()
            {
                return None;
            }
        }
        caches.insert(cpu, group?);
    }
    Some(caches)
}

fn build_topology(
    cpu_nodes: &BTreeMap<usize, usize>,
    siblings: &BTreeMap<usize, BTreeSet<usize>>,
) -> Option<Topology> {
    if cpu_nodes.is_empty() || cpu_nodes.keys().ne(siblings.keys()) {
        return None;
    }
    let mut nodes = BTreeMap::<usize, Vec<Vec<usize>>>::new();
    for (&cpu, &node) in cpu_nodes {
        let group = siblings.get(&cpu)?;
        if !group.contains(&cpu)
            || group.iter().any(|sibling| {
                cpu_nodes.get(sibling) != Some(&node) || siblings.get(sibling) != Some(group)
            })
        {
            return None;
        }
        if group.first() == Some(&cpu) {
            nodes
                .entry(node)
                .or_default()
                .push(group.iter().copied().collect());
        }
    }
    Some(Topology {
        nodes,
        cache_spread: false,
        l3_groups: BTreeSet::new(),
    })
}

fn parse_cpu_list(raw: &str) -> Option<BTreeSet<usize>> {
    let mut ids = BTreeSet::new();
    if raw.trim().is_empty() {
        return Some(ids);
    }
    for part in raw.trim().split(',') {
        let (first, last) = part.split_once('-').unwrap_or((part, part));
        let first = first.parse::<usize>().ok()?;
        let last = last.parse::<usize>().ok()?;
        // Bound expansion even if a damaged/mounted sysfs contains bogus data.
        if first > last || last >= 1_048_576 {
            return None;
        }
        for id in first..=last {
            if !ids.insert(id) {
                return None;
            }
        }
    }
    Some(ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Scratch(std::path::PathBuf);
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    impl Scratch {
        fn new() -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            Self(
                std::env::temp_dir()
                    .join(format!("seine-topology-{}-{unique}", std::process::id())),
            )
        }
        fn write(&self, path: &str, value: &str) {
            let path = self.0.join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, value).unwrap();
        }
    }

    fn fixture(groups: &[(usize, &[usize])]) -> Topology {
        let mut nodes = BTreeMap::new();
        let mut siblings = BTreeMap::new();
        for &(node, cpus) in groups {
            for &cpu in cpus {
                nodes.insert(cpu, node);
                siblings.insert(cpu, cpus.iter().copied().collect());
            }
        }
        build_topology(&nodes, &siblings).unwrap()
    }

    #[test]
    fn spreads_physical_cores_across_nodes_before_any_smt() {
        let topology = fixture(&[(0, &[0, 8]), (0, &[1, 9]), (1, &[2, 10]), (1, &[3, 11])]);
        assert_eq!(topology.ordered_cpus().unwrap(), [0, 2, 1, 3, 8, 10, 9, 11]);
    }

    #[test]
    fn uneven_sparse_nodes_and_restricted_siblings_keep_every_allowed_cpu_once() {
        let topology = fixture(&[(2, &[7]), (2, &[9, 19]), (2, &[11]), (8, &[30, 90])]);
        assert_eq!(topology.ordered_cpus().unwrap(), [7, 30, 9, 11, 90, 19]);
    }

    #[test]
    fn single_node_with_distant_siblings_keeps_os_order() {
        let topology = fixture(&[(0, &[0, 4]), (0, &[1, 5]), (0, &[2, 6]), (0, &[3, 7])]);
        assert_eq!(topology.ordered_cpus().unwrap(), [0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn single_node_places_physical_cores_before_smt() {
        // Adjacent SMT siblings, then single-thread cores (Intel hybrid desktops).
        let topology = fixture(&[(0, &[0, 1]), (0, &[2, 3]), (0, &[4]), (0, &[5])]);
        assert_eq!(topology.ordered_cpus().unwrap(), [0, 2, 4, 5, 1, 3]);
    }

    fn caches(groups: &[&[usize]]) -> BTreeMap<usize, BTreeSet<usize>> {
        groups
            .iter()
            .flat_map(|group| {
                group
                    .iter()
                    .map(|&cpu| (cpu, group.iter().copied().collect()))
            })
            .collect()
    }

    #[test]
    fn single_node_spreads_l3_groups_before_smt() {
        let mut topology = fixture(&[(0, &[0, 4]), (0, &[1, 5]), (0, &[2, 6]), (0, &[3, 7])]);
        topology
            .spread_l3(&caches(&[&[0, 1, 4, 5], &[2, 3, 6, 7]]))
            .unwrap();
        assert_eq!(topology.ordered_cpus().unwrap(), [0, 2, 1, 3, 4, 6, 5, 7]);
    }

    #[test]
    fn numa_balance_precedes_cache_balance_even_with_uneven_domains() {
        let mut topology = fixture(&[
            (0, &[0, 10]),
            (0, &[1, 11]),
            (0, &[2, 12]),
            (1, &[3, 13]),
            (1, &[4, 14]),
        ]);
        topology
            .spread_l3(&caches(&[&[0, 1, 10, 11], &[2, 12], &[3, 4, 13, 14]]))
            .unwrap();
        assert_eq!(
            topology.ordered_cpus().unwrap(),
            [0, 3, 2, 4, 1, 10, 13, 12, 14, 11]
        );
    }

    #[test]
    fn shared_l3_across_numa_nodes_does_not_merge_nodes() {
        let mut topology = fixture(&[(0, &[0]), (0, &[1]), (1, &[2]), (1, &[3])]);
        topology.spread_l3(&caches(&[&[0, 1, 2, 3]])).unwrap();
        assert_eq!(topology.ordered_cpus().unwrap(), [0, 2, 1, 3]);
    }

    #[test]
    fn malformed_cache_maps_do_not_partially_change_placement() {
        let new_topology = || fixture(&[(0, &[0, 4]), (0, &[1, 5]), (1, &[2, 6]), (1, &[3, 7])]);
        let valid = caches(&[&[0, 1, 4, 5], &[2, 3, 6, 7]]);
        let mut missing = valid.clone();
        missing.remove(&7);
        let mut asymmetric = valid.clone();
        asymmetric.insert(7, BTreeSet::from([7]));
        let mut unknown = valid.clone();
        unknown.get_mut(&0).unwrap().insert(99);
        let split_smt = caches(&[&[0, 1], &[4, 5], &[2, 3, 6, 7]]);
        for invalid in [missing, asymmetric, unknown, split_smt] {
            let mut topology = new_topology();
            assert!(topology.spread_l3(&invalid).is_none());
            assert_eq!(topology, new_topology());
        }
    }

    #[test]
    fn single_cache_keeps_core_order_and_cache_layout_is_in_identity() {
        let mut topology = fixture(&[(0, &[0]), (0, &[1]), (0, &[2]), (0, &[3])]);
        topology.spread_l3(&caches(&[&[0, 1, 2, 3]])).unwrap();
        assert_eq!(topology.ordered_cpus().unwrap(), [0, 1, 2, 3]);
        assert_eq!(
            topology.l3_groups,
            BTreeSet::from([BTreeSet::from([0, 1, 2, 3])])
        );
    }

    #[test]
    fn epyc_eight_cache_groups_spread_32_workers_without_hardcoded_cpu_ids() {
        let mut topology = fixture(
            &(0..64)
                .map(|cpu| (0, vec![cpu, cpu + 64]))
                .collect::<Vec<_>>()
                .iter()
                .map(|(node, ids)| (*node, ids.as_slice()))
                .collect::<Vec<_>>(),
        );
        let groups = (0..8)
            .map(|cache| {
                (cache * 8..cache * 8 + 8)
                    .flat_map(|cpu| [cpu, cpu + 64])
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        topology
            .spread_l3(&caches(
                &groups.iter().map(Vec::as_slice).collect::<Vec<_>>(),
            ))
            .unwrap();
        let order = topology.ordered_cpus().unwrap();
        for cache in 0..8 {
            assert_eq!(
                order[..32].iter().filter(|&&cpu| cpu / 8 == cache).count(),
                4
            );
        }
        assert_eq!(
            order[..64].iter().copied().collect::<BTreeSet<_>>(),
            (0..64).collect()
        );
        assert_eq!(
            order.iter().copied().collect::<BTreeSet<_>>(),
            (0..128).collect()
        );
    }

    #[test]
    fn rejects_inconsistent_or_cross_node_siblings() {
        let nodes = BTreeMap::from([(0, 0), (1, 1)]);
        let siblings = BTreeMap::from([(0, BTreeSet::from([0, 1])), (1, BTreeSet::from([0, 1]))]);
        assert!(build_topology(&nodes, &siblings).is_none());
        let nodes = BTreeMap::from([(0, 0), (1, 0)]);
        let siblings = BTreeMap::from([(0, BTreeSet::from([0, 1])), (1, BTreeSet::from([1]))]);
        assert!(build_topology(&nodes, &siblings).is_none());
        assert!(build_topology(&nodes, &BTreeMap::new()).is_none());
    }

    #[test]
    fn parses_kernel_lists_and_rejects_malformed_ranges() {
        assert_eq!(
            parse_cpu_list(" 0-2,8,10-11\n").unwrap(),
            BTreeSet::from([0, 1, 2, 8, 10, 11])
        );
        for invalid in ["3-1", "0-2,2", "1,", "-1", "x", "0-9999999999"] {
            assert!(parse_cpu_list(invalid).is_none(), "{invalid}");
        }
    }

    #[test]
    fn sysfs_reader_obeys_cpuset_and_falls_back_when_local_memory_is_unavailable() {
        let scratch = Scratch::new();
        let write = |path: &str, value: &str| scratch.write(path, value);
        write("node/online", "0,2");
        write("node/node0/cpulist", "0-1,4-5");
        write("node/node2/cpulist", "2-3,6-7");
        write("node/node0/meminfo", "Node 0 MemTotal: 100000 kB\n");
        write("node/node2/meminfo", "Node 2 MemTotal: 100000 kB\n");
        // Only CPU 4 from its sibling group is allowed; excluded CPUs need no files.
        for (cpu, siblings) in [(4, "0,4"), (2, "2,6"), (6, "2,6")] {
            write(
                &format!("cpu/cpu{cpu}/topology/thread_siblings_list"),
                siblings,
            );
        }
        let cpus = BTreeSet::from([2, 4, 6]);
        let mems = BTreeSet::from([0, 2]);
        assert_eq!(
            read_topology(&scratch.0, &cpus, &mems)
                .unwrap()
                .ordered_cpus()
                .unwrap(),
            [4, 2, 6]
        );
        assert!(read_topology(&scratch.0, &cpus, &BTreeSet::from([0])).is_none());
        assert!(read_topology(&scratch.0, &BTreeSet::from([2, 4, 6, 99]), &mems).is_none());
        write("node/node2/meminfo", "Node 2 MemTotal: 0 kB\n");
        assert!(read_topology(&scratch.0, &cpus, &mems).is_none());
        write("node/node2/meminfo", "Node 2 MemTotal: 100000 kB\n");
        fs::remove_file(scratch.0.join("cpu/cpu6/topology/thread_siblings_list")).unwrap();
        assert!(read_topology(&scratch.0, &cpus, &mems).is_none());
    }

    #[test]
    fn sysfs_l3_reader_restricts_masks_and_rejects_partial_or_duplicate_caches() {
        let scratch = Scratch::new();
        scratch.write("node/online", "0");
        scratch.write("node/node0/cpulist", "0-3,8-11");
        scratch.write("node/node0/meminfo", "Node 0 MemTotal: 100000 kB\n");
        let cpus = BTreeSet::from([1, 3, 8, 9, 11]);
        let mems = BTreeSet::from([0]);
        for &cpu in &cpus {
            let core = cpu % 8;
            scratch.write(
                &format!("cpu/cpu{cpu}/topology/thread_siblings_list"),
                &format!("{core},{}", core + 8),
            );
            let cache = format!("cpu/cpu{cpu}/cache/index7");
            scratch.write(&format!("{cache}/level"), "3");
            scratch.write(&format!("{cache}/type"), "Unified");
            scratch.write(
                &format!("{cache}/shared_cpu_list"),
                if core < 2 { "0-1,8-9" } else { "2-3,10-11" },
            );
        }
        let detected = || read_topology(&scratch.0, &cpus, &mems).unwrap();
        let measured = measured_inputs_at(&scratch.0, &cpus, &mems).unwrap();
        assert_ne!(
            measured,
            measured_inputs_at(&scratch.0, &cpus, &BTreeSet::from([0, 2])).unwrap()
        );
        assert_eq!(detected().ordered_cpus().unwrap(), [1, 3, 8, 9, 11]);
        assert_eq!(
            detected().l3_groups,
            BTreeSet::from([BTreeSet::from([1, 8, 9]), BTreeSet::from([3, 11])])
        );
        // Missing, contradictory, or duplicate cache descriptions keep the
        // usable NUMA/core topology, but cannot enable single-node cache spreading.
        scratch.write("cpu/cpu11/cache/index7/shared_cpu_list", "11");
        assert_eq!(detected().ordered_cpus().unwrap(), [1, 3, 8, 9, 11]);
        assert!(measured_inputs_at(&scratch.0, &cpus, &mems).is_none());
        scratch.write("cpu/cpu11/cache/index7/shared_cpu_list", "2-3,10-11");
        fs::remove_file(scratch.0.join("cpu/cpu11/cache/index7/level")).unwrap();
        assert_eq!(detected().ordered_cpus().unwrap(), [1, 3, 8, 9, 11]);
        assert!(measured_inputs_at(&scratch.0, &cpus, &mems).is_none());
        scratch.write("cpu/cpu11/cache/index7/level", "3");
        scratch.write("cpu/cpu11/cache/index8/level", "3");
        scratch.write("cpu/cpu11/cache/index8/type", "Unified");
        scratch.write("cpu/cpu11/cache/index8/shared_cpu_list", "2-3,10-11");
        assert_eq!(detected().ordered_cpus().unwrap(), [1, 3, 8, 9, 11]);
    }
}
