#!/usr/bin/env python3
"""Archive relevant disassembly and summarize register/prefetch placement."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('binary', type=Path)
    ap.add_argument('out', type=Path)
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    symbols = subprocess.check_output(['nm', '-S', '--defined-only', '-C', str(args.binary)], text=True)
    result = {}
    for line in symbols.splitlines():
        parts = line.split(maxsplit=3)
        if len(parts) != 4:
            continue
        address, size, kind, name = parts
        if kind.lower() != 't' or not any(x in name for x in (
                'fixed_argon::avx512_256::compress_into', 'fixed_argon::compress_avx2_into',
                'fixed_argon::compress_avx512_into', 'fixed_argon::FixedArgon2id::fill_streaming_ring')):
            continue
        start, length = int(address, 16), int(size, 16)
        assembly = subprocess.check_output(['objdump', '-d', '-C', '-Mintel', '--no-show-raw-insn',
            f'--start-address={start}', f'--stop-address={start+length}', str(args.binary)], text=True)
        slug = name.replace('::', '_').replace(' ', '_')
        (args.out / (slug + '.asm')).write_text(assembly)
        instructions = []
        for row in assembly.splitlines():
            m = re.match(r'\s*[0-9a-f]+:\s+(.*)', row)
            if m:
                instruction = m.group(1).split('#')[0].strip()
                if not re.match(r'(?:data16 |cs |nop|xchg\s+ax,ax)', instruction):
                    instructions.append(instruction)
        normalized = [re.sub(r'0x[0-9a-f]+', 'OFFSET', re.sub(r'\b[0-9a-f]+ <[^>]+>', 'TARGET', x))
                      for x in instructions]
        prefetch = [i for i, inst in enumerate(instructions) if inst.startswith('prefetch')]
        regs = sorted({int(n) for n in re.findall(r'ymm(\d+)', assembly)})
        result[name] = {'bytes': length, 'instruction_count': len(instructions), 'ymm_registers': regs,
             'zmm_references': len(re.findall(r'zmm\d+', assembly)), 'prefetch_indexes': prefetch,
             'multiply_count_after_first_prefetch': sum('vpmuludq' in inst for inst in instructions[prefetch[0]:]) if prefetch else None,
             'normalized_sha256': hashlib.sha256('\n'.join(normalized).encode()).hexdigest()}
    assert result
    (args.out / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
