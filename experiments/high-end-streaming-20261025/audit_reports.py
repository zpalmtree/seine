#!/usr/bin/env python3
"""Verify collected reports belong to the published release and fixed protocol."""
import argparse
import hashlib
import json
from pathlib import Path


def audit(root, platform, family, model):
    reports = []
    builds = set()
    for path in sorted(root.rglob('*.json')):
        report = json.loads(path.read_text(encoding='utf-8-sig'))
        if not isinstance(report, dict) or 'config_fingerprint' not in report:
            continue
        env, fp = report['environment'], report['config_fingerprint']
        assert env['seine_version'] == '0.2.22', path
        assert env['git_commit'] == '6e5411768d5d', path
        assert env['source_fingerprint'] == '7ad8df30888fb8f1', path
        assert env['runtime_environment'] == 'native', path
        assert env['target_triple'] == platform, path
        identity = env['cpu_identity']
        assert identity['vendor'] == 'AuthenticAMD', path
        assert (identity['family'], identity['model']) == (family, model), path
        assert report['backends'] == ['cpu#1'], path
        assert report['pow_fingerprint'] == {
            'memory_kb': 2097152, 'iterations': 1, 'parallelism': 1,
            'output_len': 32, 'header_base_len': 92}, path
        assert fp['cpu_kernel'] == fp['cpu_kernel_requested'], path
        assert not fp['cpu_autotune_threads'], path
        expected_mode = 'regular' if platform.endswith('windows-msvc') else 'large'
        assert fp['cpu_page_mode'] == expected_mode, path
        field = 'memory_regular' if expected_mode == 'regular' else 'memory_explicit_large'
        workers = report['cpu_threads']
        assert workers in ([8, 16] if expected_mode == 'regular' else [8, 24]), path
        for sample in report['runs']:
            assert sample['actual_elapsed_secs'] == sample['elapsed_secs'], path
            assert sample['hashes'] == sample['counted_hashes'] + sample['late_hashes'], path
            assert sum(b[field + '_bytes'] for b in sample['backend_runs']) == workers * 2 * 1024**3, path
            assert sum(b[field + '_workers'] for b in sample['backend_runs']) == workers, path
        builds.add(env['build_fingerprint'])
        reports.append({'path': str(path.relative_to(root)),
                        'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    assert len(reports) == 24, f'Expected 16 screen and 8 confirmation reports, got {len(reports)}'
    assert len(builds) == 1
    return {'reports_checked': len(reports), 'source_commit': '6e5411768d5d67365ecaff3abc30d280fb30cb0d',
            'build_fingerprint': builds.pop(), 'reports': reports}


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('host', choices=['genoa', 'windows'])
    ap.add_argument('directory', type=Path)
    args = ap.parse_args()
    identity = ('x86_64-unknown-linux-gnu', 25, 17) if args.host == 'genoa' else ('x86_64-pc-windows-msvc', 26, 68)
    print(json.dumps(audit(args.directory, *identity), indent=2))
