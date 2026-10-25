#!/usr/bin/env python3
"""Generate a normal hash-loop PMC probe from the pinned arena/barrier helper."""
from pathlib import Path

source = Path(__file__).with_name('epyc_phase.rs').read_text()
source = source.replace('Experimental phase timing', 'Experimental normal-hash PMC probe')
source = source.replace('epyc_phase KERNEL', 'epyc_loop KERNEL')
source = source.replace('.profile_split_hash_with_memory(', '.hash_password_into_with_memory(')
source = source.replace('        let timing = hasher', '        hasher')
for phase in ['seed', 'di', 'dd', 'finalize']:
    source = source.replace(f'    let mut {phase} = Duration::ZERO;\n', f'    let {phase} = Duration::ZERO;\n')
for line in ['        seed += timing.seed;\n', '        di += timing.data_independent;\n',
             '        dd += timing.data_dependent;\n', '        finalize += timing.finalize;\n']:
    assert line in source
    source = source.replace(line, '')
source = source.replace('mode={mode} arena_bytes=', 'mode={mode} kind=normal arena_bytes=')
assert 'profile_split_hash' not in source and 'timing.' not in source
Path('pow-kernel/examples/epyc_loop.rs').write_text(source)
