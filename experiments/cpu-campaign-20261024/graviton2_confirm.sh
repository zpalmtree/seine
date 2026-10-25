#!/usr/bin/env bash
# No concurrent timing: invoke only after the complete generation screen.
set -euo pipefail
cd /home/ubuntu/campaign
test -f results/arm-generation-prefetch-screen/COMPLETE
python3 matrix.py --out results/graviton2-fallback-confirm --workers 1 16 --pairs 4 --seconds 30 \
  --variant baseline:bin/seine-generic:neon --variant fallback:bin/seine-generation-auto:neon
# This exact generic verifier was built on Graviton3; no rebuild on Graviton2.
mkdir -p results/portable-arm
sha256sum --check portable-arm-sha256.txt > results/portable-arm/identities.txt
timeout 600 bin/verify-portable-neon > results/portable-arm/full-size-parity.log
touch results/portable-arm/COMPLETE
bash recovery_validate.sh
