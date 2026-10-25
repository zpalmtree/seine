#!/usr/bin/env bash
# Same published binary and backing for all ISA paths, with longer pair windows.
set -euo pipefail
cd /home/ubuntu/campaign
test -f results/lifecycle/COMPLETE
python3 matrix.py --out results/intel-isa-confirm --workers 1 12 --pairs 4 --seconds 30 \
  --variant avx2:bin/seine-generic:avx2 --variant avx512:bin/seine-generic:avx512 \
  --variant avx512256:bin/seine-generic:avx512-256
