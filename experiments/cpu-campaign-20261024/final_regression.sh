#!/usr/bin/env bash
# Run only after native functional checks; no compiler or lifecycle contention.
set -euo pipefail
cd /home/ubuntu/campaign
test -f results/hardened-build/COMPLETE
test -f results/pid-pool/COMPLETE
test -f results/lifecycle/COMPLETE
if [[ $(uname -m) == aarch64 ]]; then
  python3 matrix.py --out results/final-arm-regression --workers 1 16 --pairs 2 --seconds 20 \
    --variant baseline:bin/seine-generic:neon \
    --variant earlier:bin/seine-generation-auto:neon-sha3 \
    --variant final:bin/seine-hardened:neon-sha3
else
  sha256sum --check portable-control-input.sha256
  cp seine-portable-control bin/seine-portable-control
  cp verify-portable-control bin/verify-portable-control
  cp bin/seine-hardened bin/seine-portable-candidate
  cp bin/verify-hardened bin/verify-portable-candidate
  sha256sum bin/seine-portable-control bin/verify-portable-control \
    bin/seine-portable-candidate bin/verify-portable-candidate > portable-x86-binaries.sha256
  bash portable_regression.sh
fi
