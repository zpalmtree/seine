#!/usr/bin/env python3
"""One isolated candidate, measured sequentially at fixed worker counts."""
import argparse
from pathlib import Path
import json
from bench import paired

ap = argparse.ArgumentParser()
ap.add_argument('candidate')
ap.add_argument('--baseline', default='baseline')
ap.add_argument('--baseline-kernel', default='avx512-256')
ap.add_argument('--candidate-kernel', default='avx512-256')
ap.add_argument('--workers', nargs='+', type=int, default=[1, 8, 24])
ap.add_argument('--pairs', type=int, default=2)
ap.add_argument('--seconds', type=int, default=12)
ap.add_argument('--rounds', type=int, default=1)
ap.add_argument('--out', required=True)
args = ap.parse_args()
root = Path.cwd()
out = root / args.out
out.mkdir(exist_ok=False)
for workers in args.workers:
    summary = paired(root, out / f'w{workers}',
        {'binary': f'kernels/{args.baseline}/seine', 'kernel': args.baseline_kernel,
         'env': {'SEINE_EXPERIMENT_ARENA_POLICY': 'audit'}},
        {'binary': f'kernels/{args.candidate}/seine',
         'kernel': args.candidate_kernel,
         'env': {'SEINE_EXPERIMENT_ARENA_POLICY': 'audit'}}, workers,
        pairs=args.pairs, seconds=args.seconds, rounds=args.rounds)
    print(workers, json.dumps(summary), flush=True)
(out / 'COMPLETE').write_text('All requested worker counts passed.\n')
