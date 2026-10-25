#!/usr/bin/env python3
"""Archive a completed guest stage, verify its digest, and retain original files.

Run between timing phases, before terminating a lease. Paths are relative to the
owned guest campaign directory. Never use this to archive a running stage.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import tarfile

import cloud


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("instance")
    parser.add_argument("hardware")
    parser.add_argument("stage")
    parser.add_argument("paths", nargs="+")
    args = parser.parse_args()
    assert all(re.fullmatch(r"[a-zA-Z0-9_-]+", value) for value in (args.hardware, args.stage))
    assert all(not Path(p).is_absolute() and ".." not in Path(p).parts for p in args.paths)
    name = args.stage + "-results.tar.gz"
    remote = "/home/ubuntu/campaign/" + name
    code = """import hashlib,json,pathlib,tarfile,sys
root=pathlib.Path('/home/ubuntu/campaign')
name=sys.argv[1]
paths=sys.argv[2:]
manifest={}
for relative in paths:
 path=root/relative
 assert path.exists(), relative
 for file in sorted(path.rglob('*')) if path.is_dir() else [path]:
  if file.is_file():
   assert not file.is_symlink()
   manifest[str(file.relative_to(root))]=hashlib.sha256(file.read_bytes()).hexdigest()
manifest_name=name+'.manifest.json'
(root/manifest_name).write_text(json.dumps(manifest,indent=2)+'\\n')
with tarfile.open(root/name,'w:gz') as archive:
 for relative in paths+[manifest_name]: archive.add(root/relative,arcname=relative)
print(json.dumps({'sha256':hashlib.sha256((root/name).read_bytes()).hexdigest(),'files':len(manifest)}))
"""
    expected = json.loads(cloud.ssh(args.instance, shlex.join(["python3", "-c", code, name, *args.paths]), timeout=120))
    destination = cloud.ROOT / args.hardware
    destination.mkdir(parents=True, exist_ok=True)
    archive = destination / name
    assert not archive.exists(), "do not overwrite an earlier evidence archive"
    cloud.copy(args.instance, remote, str(archive), download=True)
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == expected["sha256"]
    extracted = destination / args.stage
    extracted.mkdir(exist_ok=False)
    with tarfile.open(archive) as bundle:
        # Explicit validation also supports Python versions before data filters.
        assert all((m.isfile() or m.isdir()) and not Path(m.name).is_absolute()
                   and ".." not in Path(m.name).parts for m in bundle.getmembers())
        bundle.extractall(extracted)
    manifest = json.loads((extracted / (name + ".manifest.json")).read_text())
    for file, digest in manifest.items():
        assert hashlib.sha256((extracted / file).read_bytes()).hexdigest() == digest, file
    cloud.save(destination / (args.stage + "-verification.json"),
               expected | {"instance": args.instance, "verified_at": cloud.now().isoformat(),
                           "paths": args.paths, "every_file_verified": True})
    print(json.dumps(expected | {"archive": str(archive), "every_file_verified": True}))


if __name__ == "__main__":
    main()
