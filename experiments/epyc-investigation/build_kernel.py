#!/usr/bin/env python3
"""Build and verify a generated candidate before uploading it to a timing host."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('variant')
    ap.add_argument('--target', type=Path, required=True)
    args = ap.parse_args()
    root = Path.cwd()
    assert (root / '.git').is_file(), 'Use an isolated Git worktree, not the primary checkout'
    scripts = root / 'experiments/epyc-investigation'
    out = root / 'data/epyc-investigation/kernels' / args.variant
    out.mkdir(parents=True, exist_ok=True)
    assert not (out / 'seine').exists(), 'Do not overwrite a frozen binary'
    assert not os.environ.get('RUSTFLAGS'), 'Portable comparisons require no RUSTFLAGS'
    for path in ['Cargo.toml', 'Cargo.lock', 'pow-kernel/Cargo.toml']:
        assert (root / path).read_bytes() == subprocess.check_output(['git', 'show', f'785a06f:{path}']), (
            'Restore frozen build manifests before a kernel comparison', path)
    gate = subprocess.run(['gpuq', 'gaming', 'check', '--quiet'])
    assert gate.returncode in (0, 75)
    subprocess.run(['python3', str(scripts / 'prepare_kernel.py'), args.variant], check=True)
    sources = ['pow-kernel/src/fixed_argon.rs', 'pow-kernel/src/fixed_argon/avx512_256.rs']
    subprocess.run(['rustup', 'run', '1.93.0', 'rustfmt', '--edition', '2021', '--config',
                    'skip_children=true', *sources], check=True)
    for source in sources:
        shutil.copy2(root / source, out / Path(source).name)
    (out / 'candidate.patch').write_bytes(subprocess.check_output(['git', 'diff', '785a06f', '--', *sources]))
    env = dict(os.environ, CARGO_TARGET_DIR=str(args.target.resolve()))
    for label, command in [
        ('tests', ['cargo', '+1.93.0', 'test', '--locked', '-p', 'blocknet-pow-kernel']),
        ('build', ['cargo', '+1.93.0', 'build', '--locked', '--release', '--no-default-features',
                   '--bin', 'seine', '--example', 'verify_cpu_kernels', '-p', 'seine', '-p', 'blocknet-pow-kernel'])]:
        with (out / (label + '.log')).open('w') as log:
            subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    shutil.copy2(args.target / 'release/seine', out / 'seine')
    shutil.copy2(args.target / 'release/examples/verify_cpu_kernels', out / 'verify')
    with (out / 'verify-local.log').open('w') as log:
        subprocess.run([str(out / 'verify')], stdout=log, stderr=subprocess.STDOUT, check=True, timeout=300)
    with (out / 'assembly-summary.json').open('w') as log:
        subprocess.run(['python3', str(scripts / 'inspect_assembly.py'), str(out / 'seine'),
                        str(out / 'assembly')], stdout=log, check=True)
    metadata = {'variant': args.variant, 'base_commit': '785a06f',
                'selected_kernel_label': 'avx512' if args.variant in ('early-seed-512', 'dd-t1-512', 'dd-nta-512') else 'avx512-256',
                'label_scope': 'Candidate identity is the variant plus binary hash; no automatic preference changed',
                'files_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                 for p in out.iterdir() if p.is_file()},
                'rustc': subprocess.check_output(['rustup', 'run', '1.93.0', 'rustc', '-Vv'], text=True)}
    (out / 'source.json').write_text(json.dumps(metadata, indent=2) + '\n')
    print(args.variant, 'built; unit/integration and full-size reference checks passed', flush=True)


if __name__ == '__main__':
    main()
