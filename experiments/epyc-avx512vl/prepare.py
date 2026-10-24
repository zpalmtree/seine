#!/usr/bin/env python3
"""Generate the experimental 256-bit AVX-512 implementation from a frozen base.

This generates a source file for an isolated experimental checkout. It never
overwrites the checkout's kernel itself. It introduces no shipping dispatch
preference. The normal
and mid-prefetch compression paths retain the established paired AVX2 layout,
but use AVX-512VL native rotates and its larger register namespace.
"""
from pathlib import Path
import argparse
import re
import subprocess

BASE = '9c414716471b19970336539cf64d91d0de59635c'
FILE = 'pow-kernel/src/fixed_argon.rs'


def function(source, name):
    start = source.index('unsafe fn ' + name + '(')
    brace = source.index('{', start)
    depth = 1
    end = brace + 1
    while depth:
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    return source[start:end]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path,
                        default=Path('data/epyc-avx512vl/generated-fixed_argon.rs'))
    args = parser.parse_args()
    original = subprocess.check_output(['git', 'show', f'{BASE}:{FILE}'], text=True)
    source = original
    helpers = []
    for name in ('avx2_round', 'avx2_round_pair'):
        body = function(original, name).replace(name, name.replace('avx2', 'avx512vl'))
        body = body.replace('_mm256_permute4x64_epi64,', '_mm256_ror_epi64, _mm256_permute4x64_epi64,')
        body = re.sub(r'avx2_rotr_64_(\d+)\(', r'_mm256_ror_epi64::<\1>(', body)
        helpers.append('#[cfg(target_arch = "x86_64")]\n#[inline(always)]\n'
                       '#[allow(clippy::too_many_arguments)]\n' + body)
    for suffix in ('_into', '_into_mid_prefetch'):
        old = function(original, 'compress_avx512' + suffix)
        new = function(original, 'compress_avx2' + suffix)
        new = new.replace('compress_avx2', 'compress_avx512')
        new = new.replace('avx2_round', 'avx512vl_round')
        # Every scratch element is written by the input-XOR phase before use.
        new = new.replace('let mut q = PowBlock::default();',
                          'let mut q = std::mem::MaybeUninit::<PowBlock>::uninit();')
        new = new.replace('let q_ptr = q.0.as_mut_ptr();',
                          'let q_ptr = q.as_mut_ptr() as *mut u64;')
        source = source.replace(old, new)
    marker = '#[cfg(target_arch = "x86_64")]\n#[target_feature(enable = "avx512f,avx512vl")]\nunsafe fn compress_avx512('
    assert marker in source
    source = source.replace(marker, '\n\n'.join(helpers) + '\n\n' + marker)
    assert args.output.resolve() != Path(FILE).resolve(), 'use a generated output file'
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(source)


if __name__ == '__main__':
    main()
