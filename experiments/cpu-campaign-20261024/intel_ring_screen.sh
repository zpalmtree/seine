#!/usr/bin/env bash
# Borrowed EPYC research path: manual identity only, no automatic selection.
set -euo pipefail
cd /home/ubuntu/campaign
test -f results/intel-isa-confirm/COMPLETE
mkdir -p results/intel-ring-build
sha256sum --check intel-ring-binaries.sha256 > results/intel-ring-build/identities.txt
timeout 600 bin/verify-ring-control > results/intel-ring-build/control-full-size-parity.log
timeout 600 bin/verify-ring-candidate > results/intel-ring-build/candidate-full-size-parity.log
touch results/intel-ring-build/COMPLETE
python3 matrix.py --out results/intel-ring-screen --workers 8 12 --pairs 2 --seconds 12 \
  --variant avx2:bin/seine-ring-control:avx2 \
  --variant avx512:bin/seine-ring-control:avx512 \
  --variant streaming:bin/seine-ring-candidate:avx512-256
