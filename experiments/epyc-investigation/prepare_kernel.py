#!/usr/bin/env python3
"""Generate one isolated kernel candidate from frozen 785a06f sources.

Only two explicit kernel files in this experiment worktree are overwritten.
Backend instrumentation and normal preferred-kernel policy are unaffected.
"""
import argparse
from pathlib import Path
import subprocess

BASE = '785a06f'
PARENT = 'pow-kernel/src/fixed_argon.rs'
MODULE = 'pow-kernel/src/fixed_argon/avx512_256.rs'


def frozen(path):
    return subprocess.check_output(['git', 'show', f'{BASE}:{path}'], text=True)


def replace_once(text, old, new):
    assert text.count(old) == 1, (old[:90], text.count(old))
    return text.replace(old, new, 1)


def early(module):
    start = module.index('#[inline(always)]')
    end = module.index('\n#[inline(always)]', start + 1)
    helper = module[start:end]
    helper = replace_once(helper, 'unsafe fn avx512vl_round(', 'unsafe fn avx512vl_round_seed(')
    helper = replace_once(helper, ') {', '    backup_word: u64,\n    next_ref_area_size: usize,\n    memory_blocks_base: *const PowBlock,\n) {')
    trigger = '    *a = avx2_blamka(*a, bb);'
    pos = helper.rindex(trigger) + len(trigger)
    helper = helper[:pos] + '''
    // a[0] is final before the remaining d/c/b arithmetic and output stores.
    let word = std::arch::x86_64::_mm_cvtsi128_si64(
        std::arch::x86_64::_mm256_castsi256_si128(*a)) as u64 ^ backup_word;
    let reference = reference_index(next_ref_area_size, word);
    prefetch_pow_block(&*memory_blocks_base.add(reference));
''' + helper[pos:]
    middle = module.index('pub(super) unsafe fn compress_into_mid_prefetch(')
    head, tail = module[:middle], module[middle:]
    old = '        avx512vl_round(&mut a, &mut b, &mut c, &mut d);'
    assert tail.count(old) == 2
    tail = tail.replace(old, '        avx512vl_round_seed(&mut a, &mut b, &mut c, &mut d, *dst_ptr, next_ref_area_size, memory_blocks_base);', 1)
    tail = replace_once(tail, '''    {
        let rand = *dst_ptr;
        let next_ref = reference_index(next_ref_area_size, rand);
        prefetch_pow_block(&*memory_blocks_base.add(next_ref));
    }''', '    // Next-reference prefetch was issued inside column zero.')
    return head + tail + '\n' + helper


def hint(module, level):
    module = replace_once(module, 'use super::{avx2_blamka, prefetch_pow_block, reference_index, PowBlock};',
                          'use super::{avx2_blamka, reference_index, PowBlock};')
    calls = '\n'.join(f'        _mm_prefetch(ptr.add({i * 64}), _MM_HINT_{level});' for i in range(16))
    return module + f'''
#[inline(always)]
fn prefetch_pow_block(block: &PowBlock) {{
    use std::arch::x86_64::{{_mm_prefetch, _MM_HINT_{level}}};
    let ptr = block as *const PowBlock as *const i8;
    unsafe {{
{calls}
    }}
}}
'''


def early512(parent):
    start = parent.index('unsafe fn avx512_round(')
    end = parent.index('\n#[cfg(', start)
    helper = parent[start:end]
    helper = replace_once(helper, 'unsafe fn avx512_round(', 'unsafe fn avx512_round_seed(')
    helper = replace_once(helper, ') {',
        '    backup_word: u64,\n    next_ref_area_size: usize,\n    memory_blocks_base: *const PowBlock,\n) {')
    trigger = '    *a = avx512_blamka(*a, bb);'
    pos = helper.rindex(trigger) + len(trigger)
    helper = helper[:pos] + '''
    let word = std::arch::x86_64::_mm_cvtsi128_si64(
        std::arch::x86_64::_mm512_castsi512_si128(*a)) as u64 ^ backup_word;
    prefetch_pow_block(&*memory_blocks_base.add(reference_index(next_ref_area_size, word)));
''' + helper[pos:]
    mid = parent.index('unsafe fn compress_avx512_into_mid_prefetch(')
    phase = parent.index('    // Phase 3a: Column pair 0', mid)
    head, tail = parent[:phase], parent[phase:]
    tail = tail.replace('        avx512_round(&mut a, &mut b, &mut c, &mut d);',
        '        avx512_round_seed(&mut a, &mut b, &mut c, &mut d, *dst_ptr, next_ref_area_size, memory_blocks_base);', 1)
    old = '''    {
        let rand = *dst_ptr;
        let next_ref = reference_index(next_ref_area_size, rand);
        prefetch_pow_block(&*memory_blocks_base.add(next_ref));
    }'''
    assert old in tail
    tail = tail.replace(old, '    // Prefetch issued as soon as the seed word was final.', 1)
    return head + tail + '\n#[cfg(target_arch = "x86_64")]\n#[inline(always)]\n' + helper


def hint512(parent, level):
    with_hint = hint(frozen(MODULE), level)
    helper = with_hint[with_hint.rindex('#[inline(always)]\nfn prefetch_pow_block('):]
    helper = helper.replace('fn prefetch_pow_block(', 'fn prefetch_pow_block_dd_hint(', 1)
    start = parent.index('unsafe fn compress_avx512_into_mid_prefetch(')
    head, tail = parent[:start], parent[start:]
    tail = tail.replace('        prefetch_pow_block(&*memory_blocks_base.add(next_ref));',
                        '        prefetch_pow_block_dd_hint(&*memory_blocks_base.add(next_ref));', 1)
    return head + tail + '\n#[cfg(target_arch = "x86_64")]\n' + helper


def hybrid(parent, di, dd):
    for isa, x, y in [('ISA_AVX512', 'true', 'false'), ('ISA_AVX512_256', 'true', 'false'),
                      ('ISA_AVX2', 'true', 'false'), ('ISA_NEON', 'false', 'true'), ('ISA_SCALAR', 'false', 'false')]:
        parent = replace_once(parent, f'fill_blocks::<{isa}, {x}, {y}>(memory_blocks)',
                               f'fill_blocks::<{isa}, {x}, {y}, 0, SYNC_POINTS>(memory_blocks)')
    parent = replace_once(parent, '        const USE_AARCH64_MID_PREFETCH: bool,\n',
                           '        const USE_AARCH64_MID_PREFETCH: bool,\n        const START: usize,\n        const END: usize,\n')
    parent = replace_once(parent, '        for slice in 0..SYNC_POINTS {', '        for slice in START..END {')
    kernels = {'avx2': 'ISA_AVX2', '512': 'ISA_AVX512', '256': 'ISA_AVX512_256'}
    return replace_once(parent,
        '            ISA_AVX512_256 => self.fill_blocks::<ISA_AVX512_256, true, false, 0, SYNC_POINTS>(memory_blocks),',
        f'''            ISA_AVX512_256 => {{
                self.fill_blocks::<{kernels[di]}, true, false, 0, 2>(memory_blocks);
                self.fill_blocks::<{kernels[dd]}, true, false, 2, SYNC_POINTS>(memory_blocks);
            }}''')


def quad_rows(module):
    helper = '''
#[inline(always)]
#[allow(clippy::too_many_arguments)]
unsafe fn round_quad(
'''
    helper += ''.join(f'    {reg}{i}: &mut std::arch::x86_64::__m256i,\n' for i in range(4) for reg in 'abcd')
    helper += ') {\n    use std::arch::x86_64::{_mm256_permute4x64_epi64, _mm256_ror_epi64, _mm256_xor_si256};\n'
    steps = [
        '*a{i} = avx2_blamka(*a{i}, *b{i});', '*d{i} = _mm256_ror_epi64::<32>(_mm256_xor_si256(*d{i}, *a{i}));',
        '*c{i} = avx2_blamka(*c{i}, *d{i});', '*b{i} = _mm256_ror_epi64::<24>(_mm256_xor_si256(*b{i}, *c{i}));',
        '*a{i} = avx2_blamka(*a{i}, *b{i});', '*d{i} = _mm256_ror_epi64::<16>(_mm256_xor_si256(*d{i}, *a{i}));',
        '*c{i} = avx2_blamka(*c{i}, *d{i});', '*b{i} = _mm256_ror_epi64::<63>(_mm256_xor_si256(*b{i}, *c{i}));',
        'let mut bb{i} = _mm256_permute4x64_epi64::<0x39>(*b{i});',
        'let mut cc{i} = _mm256_permute4x64_epi64::<0x4E>(*c{i});',
        'let mut dd{i} = _mm256_permute4x64_epi64::<0x93>(*d{i});',
        '*a{i} = avx2_blamka(*a{i}, bb{i});', 'dd{i} = _mm256_ror_epi64::<32>(_mm256_xor_si256(dd{i}, *a{i}));',
        'cc{i} = avx2_blamka(cc{i}, dd{i});', 'bb{i} = _mm256_ror_epi64::<24>(_mm256_xor_si256(bb{i}, cc{i}));',
        '*a{i} = avx2_blamka(*a{i}, bb{i});', 'dd{i} = _mm256_ror_epi64::<16>(_mm256_xor_si256(dd{i}, *a{i}));',
        'cc{i} = avx2_blamka(cc{i}, dd{i});', 'bb{i} = _mm256_ror_epi64::<63>(_mm256_xor_si256(bb{i}, cc{i}));',
        '*b{i} = _mm256_permute4x64_epi64::<0x93>(bb{i});',
        '*c{i} = _mm256_permute4x64_epi64::<0x4E>(cc{i});', '*d{i} = _mm256_permute4x64_epi64::<0x39>(dd{i});']
    helper += ''.join('    ' + step.format(i=i) + '\n' for step in steps for i in range(4)) + '}\n'
    loop = '    for row in (0..8).step_by(4) {\n'
    for i in range(4):
        loop += f'        let base{i} = (row + {i}) * 16;\n'
        for reg, off in zip('abcd', (0, 4, 8, 12)):
            loop += f'        let mut {reg}{i} = _mm256_loadu_si256(q_ptr.add(base{i} + {off}) as *const __m256i);\n'
    loop += '        round_quad(' + ', '.join(f'&mut {reg}{i}' for i in range(4) for reg in 'abcd') + ');\n'
    for i in range(4):
        for reg, off in zip('abcd', (0, 4, 8, 12)):
            loop += f'        _mm256_storeu_si256(q_ptr.add(base{i} + {off}) as *mut __m256i, {reg}{i});\n'
    loop += '    }'
    marker = '    for row in (0..8).step_by(2) {'
    assert module.count(marker) == 2
    for _ in range(2):
        start = module.index(marker)
        pos = start + len(marker)
        depth = 1
        while depth:
            depth += (module[pos] == '{') - (module[pos] == '}')
            pos += 1
        module = module[:start] + loop + module[pos:]
    return module + helper


def streaming(parent, module):
    # Keep the original compressor for direct unit tests and unrelated paths.
    start = module.index('pub(super) unsafe fn compress_into_mid_prefetch(')
    body_start = module.index('{', start)
    pos, depth = body_start + 1, 1
    while depth:
        depth += (module[pos] == '{') - (module[pos] == '}')
        pos += 1
    clone = module[start:pos]
    clone = replace_once(clone, 'fn compress_into_mid_prefetch(', 'fn compress_into_mid_prefetch_ring(')
    clone = replace_once(clone, '    memory_blocks_base: *const PowBlock,',
                         '    memory_blocks_base: *const PowBlock,\n    ring_base: *const PowBlock,\n    next_index: usize,')
    clone = replace_once(clone, '        prefetch_pow_block(&*memory_blocks_base.add(next_ref));',
                         '''        if next_index - next_ref < 64 {
            prefetch_pow_block(&*ring_base.add(next_ref & 63));
        } else {
            prefetch_pow_block(&*memory_blocks_base.add(next_ref));
        }''')
    module += '\n#[target_feature(enable = "avx2,avx512f,avx512vl")]\n' + clone + '\n'
    parent = replace_once(parent,
        '            ISA_AVX512_256 => self.fill_blocks::<ISA_AVX512_256, true, false>(memory_blocks),',
        '''            #[cfg(target_arch = "x86_64")]
            ISA_AVX512_256 => unsafe { self.fill_streaming_ring(memory_blocks) },''')
    fragment = Path(__file__).with_name('streaming_ring.rs').read_text()
    parent = replace_once(parent, '    fn fill_blocks<', fragment + '\n    fn fill_blocks<')
    return parent, module


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('variant', choices=['baseline', 'early-seed', 'early-seed-512', 'dd-t1', 'dd-nta',
                                      'dd-t1-512', 'dd-nta-512', 'di512-dd256',
                                      'di256-dd512', 'diavx2-dd512', 'di512-ddavx2', 'quad-rows', 'streaming-ring'])
    args = ap.parse_args()
    parent, module = frozen(PARENT), frozen(MODULE)
    if args.variant == 'early-seed':
        module = early(module)
    elif args.variant == 'early-seed-512':
        parent = early512(parent)
    elif args.variant in ('dd-t1-512', 'dd-nta-512'):
        parent = hint512(parent, {'dd-t1-512': 'T1', 'dd-nta-512': 'NTA'}[args.variant])
    elif args.variant.startswith('dd-'):
        module = hint(module, {'dd-t1': 'T1', 'dd-nta': 'NTA'}[args.variant])
    elif args.variant.startswith('di'):
        di, dd = args.variant.removeprefix('di').split('-dd')
        parent = hybrid(parent, di, dd)
    elif args.variant == 'quad-rows':
        module = quad_rows(module)
    elif args.variant == 'streaming-ring':
        parent, module = streaming(parent, module)
    Path(PARENT).write_text(parent)
    Path(MODULE).write_text(module)
    print('Prepared', args.variant, 'from', BASE)


if __name__ == '__main__':
    main()
