#!/usr/bin/env bash
set -euo pipefail
cd /home/ubuntu/campaign
test -f results/control-screen/COMPLETE
source "$HOME/.cargo/env"
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
export CARGO_BUILD_JOBS=8
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/neon-x86-regression
export SEINE_GIT_TAG=v0.2.21-neon-x86-regression
export SEINE_GIT_COMMIT=2ce28ae
mkdir -p src-neon-regression results/x86-regression-build
tar xzf neon-regression.tar.gz -C src-neon-regression
cd src-neon-regression
cargo +1.93.0 test --locked --release --workspace --no-default-features -- --test-threads=1 > ../results/x86-regression-build/workspace-tests.log 2>&1
cargo +1.93.0 build --locked --release --no-default-features --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-neon-regression
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_cpu_kernels
timeout 300 "$CARGO_TARGET_DIR/release/examples/verify_cpu_kernels" > ../results/x86-regression-build/full-size-parity.log
cargo +1.93.0 test --locked --release --no-default-features --bin seine adaptive_kernel_real_arena_smoke -- --ignored --test-threads=1 > ../results/x86-regression-build/real-arena-switches.log 2>&1
rustup component add clippy --toolchain 1.93.0
cargo +1.93.0 clippy --locked --release --workspace --no-default-features --all-targets -- -D warnings > ../results/x86-regression-build/clippy.log 2>&1
cd ..
sha256sum bin/seine-clean-cpu bin/seine-neon-regression neon-regression.tar.gz > results/x86-regression-build/identities.txt
touch results/x86-regression-build/COMPLETE
python3 matrix.py --out results/x86-regression --workers 1 12 --pairs 2 --seconds 30 \
  --variant baseline:bin/seine-clean-cpu:avx512 \
  --variant candidate:bin/seine-neon-regression:avx512
python3 resource_lifecycle.py --binary bin/seine-neon-regression --cpus 0-3
