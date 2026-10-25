#!/usr/bin/env bash
set -euo pipefail
cd /home/ubuntu/campaign
test -f results/arm-followup-screen/COMPLETE
source "$HOME/.cargo/env"
export CARGO_BUILD_JOBS=8
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/combined-8280a66
export SEINE_GIT_TAG=v0.2.21-neon-combined-experiment
export SEINE_GIT_COMMIT=8280a66
mkdir -p src-combined results/arm-combined-build
tar xzf neon-combined.tar.gz -C src-combined
cd src-combined
cargo +1.93.0 test --locked --release -p blocknet-pow-kernel -- --test-threads=1 > ../results/arm-combined-build/kernel-tests.log 2>&1
cargo +1.93.0 build --locked --release --no-default-features --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-combined
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_neon_dispatch
cp "$CARGO_TARGET_DIR/release/examples/verify_neon_dispatch" ../bin/verify-combined
timeout 300 ../bin/verify-combined > ../results/arm-combined-build/full-size-parity.log
cargo +1.93.0 test --locked --release --no-default-features --bin seine adaptive_kernel -- --test-threads=1 > ../results/arm-combined-build/adaptive-tests.log 2>&1
cargo +1.93.0 test --locked --release --no-default-features --bin seine adaptive_kernel_real_arena_smoke -- --ignored --test-threads=1 > ../results/arm-combined-build/real-arena-switches.log 2>&1
cargo +1.93.0 clippy --locked -p blocknet-pow-kernel --all-targets -- -D warnings > ../results/arm-combined-build/clippy.log 2>&1
cd ..
sha256sum bin/* neon-combined.tar.gz > results/arm-combined-build/identities.txt
touch results/arm-combined-build/COMPLETE
# First reject a combined-path codegen regression against the independently
# tested globally enabled SHA3 + prefetch64 control, and check old NEON remains.
python3 matrix.py --out results/arm-combined-screen --workers 1 16 --pairs 2 --seconds 12 \
  --variant baseline:bin/seine-generic:neon \
  --variant fallback:bin/seine-combined:neon \
  --variant combined:bin/seine-combined:neon-sha3 \
  --variant build-prefetch64:bin/seine-prefetch64:neon
python3 matrix.py --out results/arm-combined-confirm --workers 1 8 16 --pairs 4 --seconds 30 \
  --variant baseline:bin/seine-generic:neon \
  --variant combined:bin/seine-combined:neon-sha3
