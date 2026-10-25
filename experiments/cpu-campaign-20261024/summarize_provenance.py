#!/usr/bin/env python3
"""Publish binary identities from verified archives without private guest paths."""
import argparse
import hashlib
import json
from pathlib import Path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    bundles = {}
    known_sources = {}
    for name in ['portable-final-arm', 'portable-codegen-arm',
                 'portable-final-x86', 'portable-codegen-x86', 'portable-joint-x86',
                 'portable-accepted-x86', 'portable-retry-only-x86']:
        path = args.directory / name / 'identities.json'
        identity = json.loads(path.read_text())
        bundles[name] = {key: identity[key] for key in
                         ['source_control', 'source_candidate', 'profile', 'sha256']}
        bundles[name]['identity_file_sha256'] = digest(path)
        for binary, sha in identity['sha256'].items():
            assert digest(path.parent / binary) == sha, binary
            role = 'control' if 'control' in binary else 'candidate'
            known_sources.setdefault(sha, set()).add(identity['source_' + role])

    matrices = []
    for verification in sorted(args.directory.glob('*/*-verification.json')):
        verified = json.loads(verification.read_text())
        if not verified.get('every_file_verified'):
            continue
        stage = verification.name.removesuffix('-verification.json')
        root = verification.parent / stage
        manifest_path = root / (stage + '-results.tar.gz.manifest.json')
        manifest = json.loads(manifest_path.read_text())

        def read(relative):
            path = root / relative
            assert digest(path) == manifest[relative], relative
            return json.loads(path.read_text())

        for relative in sorted(manifest):
            if not relative.endswith('/experiment.json'):
                continue
            experiment = read(relative)
            if not isinstance(experiment.get('variants'), dict):
                continue
            parent = str(Path(relative).parent)
            records_path = parent + '/results.json'
            records = read(records_path) if records_path in manifest else []
            variants = {}
            for label, identity in experiment['variants'].items():
                fingerprints, annotations = set(), set()
                for record in records:
                    if record.get('variant') != label:
                        continue
                    report = read(parent + '/' + record['label'] + '.json')
                    environment = report['environment']
                    fingerprints.add(environment.get('source_fingerprint'))
                    annotations.add(environment.get('git_commit'))
                variants[label] = {
                    'binary_name': Path(identity['binary']).name,
                    'binary_sha256': identity['sha256'],
                    'kernel': identity['kernel'],
                    'compiled_source_fingerprints': sorted(fingerprints, key=str),
                    'reported_git_annotations': sorted(annotations, key=str),
                    'matching_frozen_sources': sorted(known_sources.get(identity['sha256'], set()))}
            matrices.append({
                'hardware': verification.parent.name,
                'stage': stage,
                'experiment_relative_path': relative,
                'experiment_sha256': manifest[relative],
                'archive_sha256': verified['sha256'],
                'completion_marker_present': parent + '/COMPLETE' in manifest,
                'workers': experiment['workers'],
                'passes': experiment['pairs'],
                'variants': variants})
    result = {
        'scope': 'Provenance only, not acceptance. Includes incomplete, excluded and order-limited matrices; consult each normalized evidence status and RESULTS.md.',
        'git_annotation_caveat': 'Benchmark git_commit may be overridden by inherited SEINE_GIT_COMMIT or BNMINER_GIT_COMMIT. It is not sufficient executable identity. Raw annotations are preserved; binary SHA256 and compiled source fingerprints distinguish the actual builds.',
        'frozen_bundles': bundles,
        'matrices': matrices}
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'matrices': len(matrices), 'frozen_bundles': len(bundles)}))


if __name__ == '__main__':
    main()
