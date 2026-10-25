#!/usr/bin/env python3
"""Inspect frozen miner ELFs; never execute, rebuild, or modify the binaries.

Pass the retained campaign data directory with --data. An optional --output
must be a new file inside that data directory.

Requires Python 3, GNU nm and objdump. With no --output, JSON goes to stdout.
The output reproduces the original normalized hashes AND adds conservative
reference checks. Instruction-shape equality is NOT semantic equivalence:
callee implementations, relocation-bearing metadata and data references can
differ. Unverified references are explicitly reported, never silently certified.
"""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import re
import struct
import subprocess


def sha(data):
    return hashlib.sha256(data).hexdigest()


def command(args):
    return subprocess.check_output(args, text=True)


class Elf:
    def __init__(self, path):
        self.data = path.read_bytes()
        assert self.data[:6] == b'\x7fELF\x02\x01', 'requires ELF64 little endian'
        offset = struct.unpack_from('<Q', self.data, 32)[0]
        size, count = struct.unpack_from('<HH', self.data, 54)
        self.segments = [struct.unpack_from('<IIQQQQQQ', self.data, offset + i * size)
                         for i in range(count)]

    def read(self, address, size):
        for kind, flags, offset, base, physical, filesz, memsz, align in self.segments:
            if kind == 1 and base <= address and address + size <= base + filesz:
                return self.data[offset + address - base:offset + address - base + size]
        raise ValueError(f'Unmapped ELF bytes {address:#x}+{size:#x}')


def inspect(path):
    elf = Elf(path)
    identity = json.loads((path.parent / 'identities.json').read_text())
    assert sha(elf.data) == identity['sha256'][path.name], 'frozen binary identity mismatch'
    symbols = []
    for line in command(['nm', '-S', '-C', str(path)]).splitlines():
        match = re.fullmatch(r'([0-9a-f]+) ([0-9a-f]+) (\w) (.*)', line)
        if match:
            symbols.append((int(match[1], 16), int(match[2], 16), match[4]))
    symbol_counts = collections.Counter(name for address, size, name in symbols)
    functions = {}
    for start, size, name in symbols:
        if not any(part in name for part in (
                'fixed_argon::FixedArgon2id::fill_blocks',
                'fixed_argon::FixedArgon2id::hash_password_into_with_memory',
                'fixed_argon::compress_avx512_into')):
            continue
        assembly = command(['objdump', '-d', '-C', '--no-show-raw-insn',
                            f'--start-address={start}', f'--stop-address={start + size}', str(path)])
        instructions = []
        for line in assembly.splitlines():
            match = re.fullmatch(r'\s*([0-9a-f]+):\s+(.*)', line)
            if match:
                instructions.append((int(match[1], 16), match[2]))
        key = name.rsplit('::', 1)[1]
        if key == 'fill_blocks':
            if 'compress_avx512_into>' in assembly:
                key = 'fill_avx512'
            elif 'compress_avx2_into>' in assembly:
                key = 'fill_avx2'
            elif 'avx512_256::compress_into>' in assembly:
                key = 'fill_avx512_256'
            else:
                key = 'fill_scalar'
        assert key not in functions, ('ambiguous function classification', key)
        functions[key] = dict(name=name, address=start, size=size, instructions=instructions)
    known = {f['address']: key for key, f in functions.items()}
    for key, function in functions.items():
        start, size = function['address'], function['size']
        legacy, strict, references, external_targets = [], [], [], []
        reference_ids = {}
        unresolved = []
        for address, instruction in function['instructions']:
            # Keep every mnemonic/register/immediate/addressing mode. Only
            # direct target addresses and RIP displacements are normalized.
            body = instruction.split(' #')[0].rstrip()
            rip = re.search(r'%rip\).*?# ([0-9a-f]+)(?: <(.*)>)?', instruction)

            def direct_target(match):
                target, label = int(match[1], 16), match[2]
                if start <= target < start + size:
                    return f'LOCAL+{target - start:#x}'
                semantic = known.get(target, label)
                external_targets.append(semantic)
                if target not in known and symbol_counts[label] > 1:
                    unresolved.append(f'ambiguous external symbol {label} at {target:#x}')
                return semantic

            body = re.sub(r'\b([0-9a-f]+) <([^>]*)>', direct_target, body)
            legacy.append(re.sub(r'-?0x[0-9a-f]+\(%rip\)', 'RIP_RELOCATION(%rip)', body))
            if rip:
                target = int(rip[1], 16)
                reference_id = reference_ids.setdefault(target, len(reference_ids))
                # A stable ordinal preserves reference aliasing; the record
                # additionally retains the actual label/address and payload.
                body = re.sub(r'-?0x[0-9a-f]+\(%rip\)',
                              f'RIP_REF_{reference_id}(%rip)', body)
                record = {'instruction_offset': address - start, 'id': reference_id,
                          'address': hex(target), 'label': rip[2],
                          'instruction': instruction}
                # Only these known vector loads are certified as data reads.
                # LEA/pointers/GOT/jump tables/panic metadata remain unverified.
                vector_load = (key.startswith('compress_avx512')
                               and re.match(r'vmov\w*\s+', instruction)
                               and '%zmm' in instruction)
                record['verified_read_width'] = 64 if vector_load else None
                record['payload_64_bytes_hex'] = elf.read(target, 64).hex()
                if not vector_load:
                    unresolved.append(f'uncertified RIP reference {reference_id} at +{address-start:#x}')
                references.append(record)
            elif '%rip' in instruction:
                unresolved.append(f'RIP operand without resolved target at +{address-start:#x}')
            strict.append(body)
        function.update(
            raw_function_sha256=sha(elf.read(start, size)),
            legacy_normalized_instruction_sha256=sha(('\n'.join(legacy) + '\n').encode()),
            instruction_shape_sha256=sha(('\n'.join(strict) + '\n').encode()),
            normalized_instructions=strict, rip_references=references,
            external_targets=external_targets, unresolved_references=unresolved,
            instruction_count=len(function['instructions']), entry_mod64=start % 64)
        del function['instructions']
    return {'path': str(path.resolve()), 'sha256': sha(elf.data),
            'identity_sha256': sha((path.parent / 'identities.json').read_bytes()),
            'identity': identity, 'functions': functions}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    root = args.data.resolve()
    paths = {'control': root / 'portable-final-x86/seine-portable-control',
             'candidate_636': root / 'portable-final-x86/seine-portable-candidate',
             'fixed_f20': root / 'portable-codegen-x86/seine-portable-candidate'}
    binaries = {key: inspect(path) for key, path in paths.items()}
    comparisons = {}
    for variant in ('candidate_636', 'fixed_f20'):
        rows = {}
        for key, baseline in binaries['control']['functions'].items():
            candidate = binaries[variant]['functions'].get(key)
            if candidate is None:
                rows[key] = {'present': False}
                continue
            shape_equal = baseline['normalized_instructions'] == candidate['normalized_instructions']
            references_equal = ([r['payload_64_bytes_hex'] for r in baseline['rip_references']]
                                == [r['payload_64_bytes_hex'] for r in candidate['rip_references']])
            unresolved = baseline['unresolved_references'] + candidate['unresolved_references']
            rows[key] = {'present': True, 'instruction_shape_equal': shape_equal,
                         'reference_payload_windows_equal': references_equal,
                         'fully_verified_local_instruction_and_reference_equality':
                             shape_equal and references_equal and not unresolved,
                         'unverified_reference_count': len(unresolved),
                         'size_delta': candidate['size'] - baseline['size'],
                         'instruction_count_delta': candidate['instruction_count'] - baseline['instruction_count']}
        comparisons[variant + '_versus_control'] = rows
    original = json.loads((root / 'codegen-disassembly-review.json').read_text())
    reproduced = {}
    for variant, binary in binaries.items():
        reproduced[variant] = {}
        for key, function in binary['functions'].items():
            previous = original['binaries'][variant]['functions'][key]
            equal = function['legacy_normalized_instruction_sha256'] == previous['normalized_instruction_sha256']
            reproduced[variant][key] = equal
            assert equal, ('original normalized hash did not reproduce', variant, key)
    report = {'scope': 'Static ELF inspection only; binaries are never executed.',
              'script_sha256': sha(Path(__file__).read_bytes()),
              'original_artifact_sha256': sha((root / 'codegen-disassembly-review.json').read_bytes()),
              'tools': {name: command([name, '--version']).splitlines()[0] for name in ('nm', 'objdump')},
              'original_hashes_reproduced': reproduced, 'binaries': binaries,
              'comparisons': comparisons,
              'limitations': ['Instruction shape equality is not whole-program semantic equivalence.',
                              'All branch targets and operand structure are retained. Direct internal targets use function-relative offsets; external targets retain semantic labels.',
                              'RIP references preserve alias identity, target labels, addresses and payload windows. Only recognized 64-byte compression vector loads are certified; other references stay explicitly unverified.',
                              'External callee bodies, dynamic relocations and jump-table semantics are not recursively certified.',
                              'Code layout and alignment still differ; no throughput recovery or causality is established by disassembly.']}
    text = json.dumps(report, indent=2) + '\n'
    if args.output:
        output = args.output.resolve()
        assert output.parent == root, 'data artifacts must remain in this campaign data directory'
        assert not output.exists(), 'do not overwrite an existing evidence artifact'
        output.write_text(text)
        print(json.dumps({'output': str(output), 'sha256': sha(output.read_bytes()),
                          'script_sha256': report['script_sha256'], 'all_original_hashes_reproduced': True,
                          'fixed_comparisons': comparisons['fixed_f20_versus_control']}))
    else:
        print(text, end='')


if __name__ == '__main__':
    main()
