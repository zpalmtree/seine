#!/usr/bin/env python3
"""Bounded alternating NUMA-policy comparisons on a disposable Linux host.

The operator prepares HugeTLB pools and an independent rental deadline first.
This runner never provisions cloud resources or changes system memory pools.
"""
import argparse
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import statistics
import subprocess
import time

from probe import capture

GIB = 1024**3
TEST_PREFIX = 'backend::cpu::numa_experiment::tests::'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def health():
    vm = dict(line.split() for line in Path('/proc/vmstat').read_text().splitlines())
    cpu = [int(v) for v in Path('/proc/stat').read_text().splitlines()[0].split()[1:]]
    return {k: int(vm[k]) for k in ('pswpin', 'pswpout', 'oom_kill')} | {
        'ticks': sum(cpu[:8]), 'steal': cpu[7]}


def memory_headroom():
    values = {line.split(':')[0]: int(line.split()[1]) * 1024
              for line in Path('/proc/meminfo').read_text().splitlines()
              if line.split()[1].isdigit()}
    huge = int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines()
                    if line.startswith('HugePages_Free:')))
    available = min(values['MemAvailable'] + huge * values.get('Hugepagesize', 0),
                    values['MemTotal'] * 0.8)
    entries = Path('/proc/self/cgroup').read_text().splitlines()
    require(len(entries) == 1 and entries[0].startswith('0::'), 'runner requires unified cgroup v2')
    root = Path('/sys/fs/cgroup')
    relative = entries[0][3:].lstrip('/')
    require('..' not in Path(relative).parts, 'cgroup namespace path needs explicit host setup')
    current = root / relative
    require(current.is_dir(), 'cannot resolve current cgroup')
    while True:
        for name in ('memory.max', 'memory.high'):
            path = current / name
            if path.exists() and path.read_text().strip() != 'max':
                used = int((current / 'memory.current').read_text())
                available = min(available, max(0, int(path.read_text()) - used))
        if current == root:
            break
        current = current.parent
    return max(0, available - 2 * GIB)


def schedule(workers, policies, pairs):
    # Reverse within every fixed worker count, independent of outer-loop order.
    return [(count, pair, policy) for count in workers for pair in range(pairs)
            for policy in (policies if pair % 2 == 0 else list(reversed(policies)))]


def validate(report, log, workers, policy, mode, kernel, rounds):
    require(report['cpu_threads'] == workers, 'wrong worker count')
    cfg = report['config_fingerprint']
    for key, expected in [('cpu_kernel', kernel), ('cpu_page_mode', mode),
                          ('cpu_affinity', 'auto'), ('cpu_autotune_threads', False)]:
        require(cfg[key] == expected, f'wrong {key}: {cfg[key]}')
    require(len(report['runs']) == rounds, 'incomplete measured rounds')
    rates = []
    fields = ['memory_explicit_large', 'memory_explicit_large_1g',
              'memory_transparent_huge', 'memory_regular', 'memory_heap']
    for run in report['runs']:
        require(run['elapsed_secs'] > 0 and run['hashes'] > 0, 'nonproductive sample')
        rates.append(run['hashes'] / run['elapsed_secs'])
        backends = run['backend_runs']
        require(sum(b[f + '_workers'] for b in backends for f in fields) == workers, 'arena count mismatch')
        require(sum(b[f + '_bytes'] for b in backends for f in fields) == workers * 2 * GIB, 'arena byte mismatch')
        if mode != 'auto':
            field = 'memory_regular' if mode == 'regular' else 'memory_explicit_large'
            require(sum(b[field + '_bytes'] for b in backends) == workers * 2 * GIB, 'strict backing mismatch')
        for backend in backends:
            for key in ('dropped_events', 'assignment_enqueue_timeouts', 'assignment_execution_timeouts',
                        'control_enqueue_timeouts', 'control_execution_timeouts', 'memory_allocation_failures'):
                require(backend[key] == 0, f'timed backend error: {key}')
    require('NUMA_EXPERIMENT_RESTORE_ERROR' not in log, 'memory policy restoration failed')
    arenas = [json.loads(line.removeprefix('NUMA_ARENA ')) for line in log.splitlines()
              if line.startswith('NUMA_ARENA ')]
    require(len(arenas) == workers and {a['worker'] for a in arenas} == set(range(workers)), 'missing/duplicate arena audit')
    for arena in arenas:
        require(arena['policy'] == policy and arena['policy_restored'], 'policy identity/restoration mismatch')
        require(arena['bytes'] == 2 * GIB and arena['stride_bytes'] == 2 * 1024**2, 'unexpected sample geometry')
        nodes = arena['samples_by_node']
        require(sum(nodes.values()) == 1024 and all(int(n) >= 0 for n in nodes), 'incomplete placement query')
        if policy != 'audit':
            target = arena['target_node']
            require((target == arena['local_node']) == (policy == 'local'), 'wrong target node')
            require(nodes == {str(target): 1024}, 'requested locality was not established')
    return {'completed_hps': statistics.mean(rates), 'round_hps': rates, 'arenas': arenas,
            'backing': [{f: b[f + '_bytes'] for f in fields} for b in report['runs'][0]['backend_runs']]}


def execute(command, log, timeout, env):
    # Dedicated process group, bounded cleanup even if a child inherits the log.
    with log.open('w') as stream:
        proc = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT,
                                env=env, start_new_session=True)
        try:
            require(proc.wait(timeout=timeout) == 0, f'command failed; see {log}')
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--parity-binary', required=True, type=Path, help='optimized Rust test executable from same source')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--workers', nargs='+', type=int, default=[1, 8, 16, 24, 32])
    parser.add_argument('--policies', nargs='+', choices=['audit', 'local', 'remote'], default=['audit', 'local'])
    parser.add_argument('--page-mode', choices=['auto', 'regular', 'large'], default='auto')
    parser.add_argument('--kernel', choices=['avx2', 'avx512', 'avx512-256'], default='avx512')
    parser.add_argument('--pairs', type=int, default=4)
    parser.add_argument('--seconds', type=int, default=30)
    parser.add_argument('--rounds', type=int, default=2)
    parser.add_argument('--max-arena-gib', type=int, default=64)
    parser.add_argument('--deadline-utc', required=True, help='absolute ISO timestamp; separate rental deadline must also exist')
    parser.add_argument('--guest-numa', action='store_true', help='allow two guest NUMA nodes without two exposed sockets; no physical-socket claim')
    parser.add_argument('--socket', type=int, help='restrict this controller and children to one exposed socket')
    parser.add_argument('--plan', action='store_true', help='record preflight/schedule without running binaries')
    args = parser.parse_args()
    # Cooperative host lock for this runner; other sessions still require
    # explicit coordination because they may use a different benchmark tool.
    host_lock = open('/tmp/seine-numa-benchmark.lock', 'a')
    fcntl.flock(host_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    deadline = dt.datetime.fromisoformat(args.deadline_utc.replace('Z', '+00:00'))
    require(deadline.tzinfo is not None, 'deadline must include timezone')
    require(args.pairs >= 2 and args.pairs % 2 == 0, 'use an even number of alternating pairs')
    require(args.seconds >= 10 and args.rounds >= 1, 'sample window too short')
    require(args.workers and min(args.workers) > 0 and len(set(args.workers)) == len(args.workers), 'invalid worker sweep')
    require('audit' in args.policies and len(set(args.policies)) == len(args.policies) and len(args.policies) >= 2, 'need distinct audit/candidate policies')
    require(max(args.workers) * 2 <= args.max_arena_gib, 'explicit RAM ceiling exceeded')
    binary, parity = args.binary.resolve(), args.parity_binary.resolve()
    inventory = capture()
    all_sockets = {c['socket'] for c in inventory['cpus']}
    require(not inventory['wsl'], 'WSL is not native NUMA performance evidence; use probe.py for inventory')
    require(len(inventory['allowed_memory_nodes']) >= 2, 'need at least two allowed memory nodes')
    require(args.guest_numa or len(all_sockets) >= 2, 'two exposed sockets required unless --guest-numa is explicit')
    if args.socket is not None:
        selected = {c['cpu'] for c in inventory['cpus'] if c['socket'] == args.socket}
        require(selected, 'requested socket is unavailable')
        os.sched_setaffinity(0, selected)
    require(max(args.workers) <= len(os.sched_getaffinity(0)), 'worker sweep exceeds allowed logical CPUs')
    args.output.mkdir(parents=True, exist_ok=False)
    save(args.output / 'topology.json', inventory)
    identity = {'miner_sha256': sha(binary), 'parity_sha256': sha(parity)}
    jobs = schedule(args.workers, args.policies, args.pairs)
    # Includes startup/fence allowance per invocation; parity gets a separate cap.
    estimated_seconds = 600 + len(jobs) * (args.seconds * (args.rounds + 1) + 180)
    save(args.output / 'protocol.json', vars(args) | identity | {
        'binary': str(binary), 'parity_binary': str(parity), 'output': str(args.output),
        'actual_affinity': sorted(os.sched_getaffinity(0)), 'schedule': jobs,
        'estimated_upper_seconds': estimated_seconds, 'page_pool_mutations': False})
    if args.plan:
        return
    require((deadline - dt.datetime.now(dt.timezone.utc)).total_seconds() >= estimated_seconds,
            'matrix does not fit deadline; reduce sweep before starting')
    env = dict(os.environ)
    env.pop('SEINE_EXPERIMENT_ARENA_POLICY', None)
    require(memory_headroom() >= 3 * GIB, 'insufficient memory for full-size parity')
    execute([str(parity), TEST_PREFIX, '--include-ignored', '--nocapture', '--test-threads=1'],
            args.output / 'parity.log', 600, env)
    parity_log = (args.output / 'parity.log').read_text()
    for policy in ('audit', 'local'):
        require(f'NUMA_PARITY policy={policy} kernel={args.kernel} full_2gib=true dirty_reuse=true PASS' in parity_log,
                'required full-size parity evidence missing')
    completed = []
    try:
        for workers, pair, policy in jobs:
            timeout = args.seconds * (args.rounds + 1) + 180
            require((deadline - dt.datetime.now(dt.timezone.utc)).total_seconds() >= timeout + 30,
                    'deadline reserve reached')
            require(memory_headroom() >= workers * 2 * GIB, 'RAM headroom changed; stop matrix')
            stem = f't{workers}-pair{pair}-{policy}'
            report = args.output / (stem + '.json')
            log = args.output / (stem + '.log')
            command = [str(binary), '--bench', '--bench-kind', 'backend', '--backend', 'cpu',
                       '--threads', str(workers), '--disable-cpu-autotune-threads', '--cpu-affinity', 'auto',
                       '--cpu-page-mode', args.page_mode, '--bench-cpu-kernel', args.kernel,
                       '--bench-secs', str(args.seconds), '--bench-rounds', str(args.rounds),
                       '--bench-warmup-rounds', '1', '--bench-output', str(report), '--ui', 'plain',
                       '--data-dir', str(args.output / 'miner-data')]
            save(args.output / (stem + '-command.json'), {'argv': command, 'policy': policy})
            before = health()
            execute(command, log, timeout, env | {'SEINE_EXPERIMENT_ARENA_POLICY': policy})
            delta = {k: v - before[k] for k, v in health().items()}
            save(args.output / (stem + '-health.json'), delta)
            require(all(delta[k] == 0 for k in ('pswpin', 'pswpout', 'oom_kill')), 'swap/OOM activity invalidates sample')
            require(delta['ticks'] > 0 and 100 * delta['steal'] / delta['ticks'] < 2, 'guest steal invalidates sample')
            result = validate(json.loads(report.read_text()), log.read_text(), workers, policy,
                              args.page_mode, args.kernel, args.rounds)
            topology_by_cpu = {cpu['cpu']: cpu for cpu in inventory['cpus']}
            for arena in result['arenas']:
                require(arena['cpu'] in os.sched_getaffinity(0), 'worker escaped allowed CPU mask')
                require(topology_by_cpu[arena['cpu']]['nodes'] == [arena['local_node']], 'CPU/node mapping changed')
                require(all(int(node) in inventory['allowed_memory_nodes']
                            for node in arena['samples_by_node']), 'arena escaped allowed memory mask')
            completed.append({'workers': workers, 'pair': pair, 'policy': policy,
                              'report_sha256': sha(report), 'log_sha256': sha(log), **result})
            save(args.output / 'completed.json', completed)
            print(stem, result['completed_hps'], flush=True)
        require(identity == {'miner_sha256': sha(binary), 'parity_sha256': sha(parity)}, 'binary changed during run')
        require([(r['workers'], r['pair'], r['policy']) for r in completed] == jobs, 'actual run order differs')
        comparisons = []
        for workers in args.workers:
            for policy in args.policies:
                if policy == 'audit':
                    continue
                ratios = []
                for pair in range(args.pairs):
                    rates = {r['policy']: r['completed_hps'] for r in completed
                             if r['workers'] == workers and r['pair'] == pair}
                    ratios.append(rates[policy] / rates['audit'])
                comparisons.append({'workers': workers, 'policy': policy,
                                    'paired_pct': [100 * (r - 1) for r in ratios],
                                    'geomean_pct': 100 * (math.exp(statistics.mean(map(math.log, ratios))) - 1)})
        save(args.output / 'summary.json', {'comparisons': comparisons, 'native_performance': True,
                                          'guest_topology_only': args.guest_numa})
        (args.output / 'COMPLETE').write_text('All comparisons, placement, backing and order checks passed.\n')
    except BaseException as error:
        save(args.output / 'FAILED.json', {'error': repr(error), 'completed_invocations': len(completed),
                                          'at': dt.datetime.now(dt.timezone.utc).isoformat()})
        raise
    finally:
        save(args.output / 'final-topology.json', capture())


if __name__ == '__main__':
    def interrupted(*_):
        raise InterruptedError('SIGTERM')
    signal.signal(signal.SIGTERM, interrupted)
    main()
