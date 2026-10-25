#!/usr/bin/env python3
"""Create an owned-guest input archive with an exact per-file digest manifest."""
import argparse
import hashlib
import io
from pathlib import Path
import tarfile

HERE = Path(__file__).resolve().parent
DATA = HERE.parents[1] / 'data/cpu-campaign-20261024'
PUBLISHED_X86_SHA256 = 'e2574ed0867d8f645b6f4b39daeac7fdb047a957d048b7c7c85a91d82754d908'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('architecture', choices=['arm64', 'amd64'])
    args = parser.parse_args()
    sources = {name: DATA / name for name in ['baseline.tar.gz', 'neon-auto.tar.gz', 'cpu-recovery.tar.gz']}
    for name in ['bootstrap.sh', 'screen.py', 'matrix.py', 'resource_lifecycle.py',
                 'arm_generation.sh', 'arm-prefetch64.patch', 'cpu_functional_validate.sh', 'recovery_validate.sh']:
        sources[name] = HERE / name
    if args.architecture == 'amd64':
        binary = DATA / 'published-linux-seine'
        if not binary.exists():
            # The original input archive holds the release executable whose
            # digest was checked against the published v0.2.21 SHA256SUMS.
            with tarfile.open(DATA / 'payload.tar.gz') as archive:
                binary.write_bytes(archive.extractfile('bin/seine-generic').read())
            binary.chmod(0o755)
        assert hashlib.sha256(binary.read_bytes()).hexdigest() == PUBLISHED_X86_SHA256
        sources['bin/seine-generic'] = binary
    manifest = ''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {name}\n'
                       for name, path in sources.items()).encode()
    output = DATA / f'payload-generation-{args.architecture}.tar.gz'
    with tarfile.open(output, 'w:gz') as archive:
        for name, path in sources.items():
            archive.add(path, arcname=name, recursive=False)
        info = tarfile.TarInfo('INPUT-SHA256SUMS')
        info.size = len(manifest)
        archive.addfile(info, io.BytesIO(manifest))
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    output.with_suffix(output.suffix + '.sha256').write_text(digest + '\n')
    print(digest, output)


if __name__ == '__main__':
    main()
