#!/usr/bin/env python3
"""Compare ELF instruction shapes and entry alignment; never execute miners.

Usage: inspect_layout.py baseline_elf ungated_elf gated_elf
Requires GNU nm/objdump. Normalization is not semantic equivalence: relocated
addresses and referenced data/callees are not recursively verified.
"""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


def inspect(path):
    symbols = {}
    for line in subprocess.check_output(['nm', '-S', '-C', str(path)], text=True).splitlines():
        match = re.fullmatch(r'([0-9a-f]+) ([0-9a-f]+) (\w) (.*)', line)
        if match:
            symbols[int(match[1], 16)] = (int(match[2], 16), match[4])
    assembly = subprocess.check_output(
        ['objdump', '-d', '-C', '--no-show-raw-insn', str(path)], text=True)
    chunks = re.split(r'\n([0-9a-f]+) <([^\n]+)>:\n', assembly)
    functions = {}
    for index in range(1, len(chunks), 3):
        start, name, body = int(chunks[index], 16), chunks[index + 1], chunks[index + 2]
        if start not in symbols:
            continue
        size = symbols[start][0]
        if 'fixed_argon::FixedArgon2id::fill_blocks' in name:
            key = 'fill_scalar'
            for label, needle in [('avx2', 'compress_avx2_into>'),
                                  ('avx512', 'compress_avx512_into>'),
                                  ('avx512_256', 'avx512_256::compress_into>')]:
                if needle in body:
                    key = 'fill_' + label
        elif 'fixed_argon::FixedArgon2id::hash_password_into_with_memory' in name:
            key = 'hash_dispatch'
        elif any(needle in name for needle in ['fixed_argon::compress_avx', 'fixed_argon::avx512_256::compress_']):
            key = name.split('fixed_argon::')[1]
        elif name.endswith('__rust_begin_short_backtrace'):
            calls = len(re.findall(r'call\s+[^\n]+FixedArgon2id::hash_password_into_with_memory', body))
            if not calls:
                continue
            key = f'hash_caller_{calls}_calls'
        else:
            continue
        normalized = []
        for line in body.splitlines():
            match = re.fullmatch(r'\s*([0-9a-f]+):\s+(.*)', line)
            if not match or int(match[1], 16) >= start + size:
                continue
            instruction = match[2].split(' #')[0].rstrip()
            def target(match):
                address = int(match[1], 16)
                return f'LOCAL+{address-start:#x}' if start <= address < start + size else match[2]
            instruction = re.sub(r'\b([0-9a-f]+) <([^>]*)>', target, instruction)
            instruction = re.sub(r'-?0x[0-9a-f]+\(%rip\)', 'RIP_RELOCATION(%rip)', instruction)
            normalized.append(instruction)
        assert key not in functions, ('ambiguous function', key)
        functions[key] = {'address': hex(start), 'entry_mod64': start % 64, 'size': size,
                          'instruction_count': len(normalized),
                          'shape_sha256': hashlib.sha256(('\n'.join(normalized)+'\n').encode()).hexdigest()}
    assert {'fill_scalar', 'fill_avx2', 'fill_avx512', 'fill_avx512_256',
            'hash_dispatch', 'hash_caller_1_calls', 'hash_caller_2_calls'} <= functions.keys()
    return {'binary_sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'functions': functions}


def main():
    if len(sys.argv) != 4:
        raise SystemExit(__doc__)
    results = {label: inspect(Path(path)) for label, path in
               zip(['baseline', 'ungated', 'gated'], sys.argv[1:])}
    baseline = results['baseline']['functions']
    for label in ['ungated', 'gated']:
        for key, function in results[label]['functions'].items():
            function['same_shape_as_baseline'] = function['shape_sha256'] == baseline[key]['shape_sha256']
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
