#!/usr/bin/env bash
set -euo pipefail
cd /home/ubuntu/campaign
test -f results/arm-combined-screen/COMPLETE
source "$HOME/.cargo/env"
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
export CARGO_BUILD_JOBS=8
mkdir -p src-arm-clean-baseline src-arm-clean-combined results/arm-clean-build
tar xzf baseline.tar.gz -C src-arm-clean-baseline
tar xzf neon-combined.tar.gz -C src-arm-clean-combined
cd src-arm-clean-baseline
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/arm-clean-baseline
export SEINE_GIT_TAG=v0.2.21
export SEINE_GIT_COMMIT=359265e325d85c9c7b6e3eed7b5b126fcec63f9c
cargo +1.93.0 build --locked --release --no-default-features --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-clean-baseline
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_cpu_kernels
timeout 300 "$CARGO_TARGET_DIR/release/examples/verify_cpu_kernels" > ../results/arm-clean-build/baseline-parity.log
cd ../src-arm-clean-combined
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/arm-clean-combined
export SEINE_GIT_TAG=v0.2.21-neon-combined-experiment
export SEINE_GIT_COMMIT=8280a66
cargo +1.93.0 build --locked --release --no-default-features --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-clean-combined
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_neon_dispatch
timeout 300 "$CARGO_TARGET_DIR/release/examples/verify_neon_dispatch" > ../results/arm-clean-build/combined-parity.log
cd ..
rustc +1.93.0 -vV > results/arm-clean-build/compiler.txt
sha256sum bin/seine-clean-baseline bin/seine-clean-combined baseline.tar.gz neon-combined.tar.gz > results/arm-clean-build/identities.txt
touch results/arm-clean-build/COMPLETE
python3 matrix.py --out results/arm-clean-confirm --workers 1 16 --pairs 4 --seconds 30 \
  --variant baseline:bin/seine-clean-baseline:neon \
  --variant combined:bin/seine-clean-combined:neon-sha3
python3 matrix.py --out results/arm-clean-regular --workers 1 8 --pairs 2 --seconds 20 --page regular \
  --variant baseline:bin/seine-clean-baseline:neon \
  --variant combined:bin/seine-clean-combined:neon-sha3
