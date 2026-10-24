#!/usr/bin/env bash
# Native follow-up after bootstrap/screen, with isolated source/profile targets.
set -euo pipefail
cd /home/ubuntu/campaign
test -f results/SCREEN-COMPLETE
source "$HOME/.cargo/env"
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
export CARGO_BUILD_JOBS=8
export SEINE_GIT_TAG=v0.2.21-neon-auto
export SEINE_GIT_COMMIT=3122893
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/arm-generation-auto
mkdir -p src-generation-auto results/arm-generation-build
tar xzf neon-auto.tar.gz -C src-generation-auto
cd src-generation-auto
cargo +1.93.0 test --locked --release --workspace --no-default-features -- --test-threads=1 > ../results/arm-generation-build/workspace-tests.log 2>&1
cargo +1.93.0 build --locked --release --no-default-features --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-generation-auto
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_neon_dispatch
timeout 600 "$CARGO_TARGET_DIR/release/examples/verify_neon_dispatch" > ../results/arm-generation-build/full-size-parity.log
cargo +1.93.0 test --locked --release --no-default-features --bin seine adaptive_kernel_real_arena_smoke -- --ignored --test-threads=1 > ../results/arm-generation-build/real-arena-switches.log 2>&1
cargo +1.93.0 clippy --locked --release -p blocknet-pow-kernel --all-targets -- -D warnings > ../results/arm-generation-build/kernel-clippy.log 2>&1
cd ..
sha256sum bin/seine-generic bin/seine-generation-auto baseline.tar.gz neon-auto.tar.gz > results/arm-generation-build/identities.txt
touch results/arm-generation-build/COMPLETE
if grep -qw sha3 /proc/cpuinfo; then
  python3 matrix.py --out results/arm-generation-fallback --workers 1 16 --pairs 2 --seconds 20 \
    --variant baseline:bin/seine-generic:neon --variant fallback:bin/seine-generation-auto:neon
  python3 matrix.py --out results/arm-generation-confirm --workers 1 16 --pairs 4 --seconds 30 \
    --variant baseline:bin/seine-generic:neon --variant candidate:bin/seine-generation-auto:neon-sha3
else
  mkdir src-generation-prefetch64
  tar xzf baseline.tar.gz -C src-generation-prefetch64
  cd src-generation-prefetch64
  patch -p1 < ../arm-prefetch64.patch
  export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/arm-generation-prefetch64
  export SEINE_GIT_TAG=v0.2.21-neon-prefetch64-experiment
  export SEINE_GIT_COMMIT=359265e325d85c9c7b6e3eed7b5b126fcec63f9c
  cargo +1.93.0 build --locked --release --no-default-features --bin seine
  cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-generation-prefetch64
  cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_cpu_kernels
  timeout 600 "$CARGO_TARGET_DIR/release/examples/verify_cpu_kernels" > ../results/arm-generation-build/prefetch64-full-size-parity.log
  cd ..
  sha256sum bin/seine-generation-prefetch64 arm-prefetch64.patch >> results/arm-generation-build/identities.txt
  python3 matrix.py --out results/arm-generation-prefetch-screen --workers 1 8 16 --pairs 2 --seconds 12 \
    --variant baseline:bin/seine-generic:neon --variant fallback:bin/seine-generation-auto:neon \
    --variant prefetch64:bin/seine-generation-prefetch64:neon
fi
