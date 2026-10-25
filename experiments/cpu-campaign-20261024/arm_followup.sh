#!/usr/bin/env bash
set -euo pipefail
cd /home/ubuntu/campaign
test -f results/SCREEN-COMPLETE
source "$HOME/.cargo/env"
export CARGO_BUILD_JOBS=8
export SEINE_GIT_TAG=v0.2.21-neon-runtime-experiment
export SEINE_GIT_COMMIT=9ce67f6
mkdir -p src-runtime results/arm-build
tar xzf neon-runtime.tar.gz -C src-runtime
cd src-runtime
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/followup-runtime
cargo +1.93.0 test --locked --release -p blocknet-pow-kernel -- --test-threads=1 > ../results/arm-build/runtime-tests.log 2>&1
cargo +1.93.0 build --locked --release --no-default-features --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-runtime
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_neon_dispatch
cp "$CARGO_TARGET_DIR/release/examples/verify_neon_dispatch" ../bin/verify-runtime
timeout 300 ../bin/verify-runtime > ../results/arm-build/runtime-full-size-parity.log
rustup component add clippy --toolchain 1.93.0
cargo +1.93.0 clippy --locked -p blocknet-pow-kernel --all-targets -- -D warnings > ../results/arm-build/runtime-clippy.log 2>&1
cd ..
mkdir -p src-prefetch64
tar xzf baseline.tar.gz -C src-prefetch64
cd src-prefetch64
patch -p1 < ../arm-prefetch64.patch
export SEINE_GIT_TAG=v0.2.21-prefetch64-experiment
export SEINE_GIT_COMMIT=359265e325d8
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/followup-prefetch64
export RUSTFLAGS='-C target-feature=+sha3'
cargo +1.93.0 build --locked --release --no-default-features --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-prefetch64
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_cpu_kernels
cp "$CARGO_TARGET_DIR/release/examples/verify_cpu_kernels" ../bin/verify-prefetch64
timeout 300 ../bin/verify-prefetch64 > ../results/arm-build/prefetch64-full-size-parity.log
cd ..
sha256sum bin/* arm-prefetch64.patch neon-runtime.tar.gz > results/arm-build/identities.txt
touch results/arm-build/COMPLETE
python3 matrix.py --out results/arm-followup-screen --workers 1 8 16 --pairs 2 --seconds 12 \
  --variant generic:bin/seine-generic:neon \
  --variant runtime-generic:bin/seine-runtime:neon \
  --variant runtime-sha3:bin/seine-runtime:neon-sha3 \
  --variant build-sha3:bin/seine-sha3:neon \
  --variant prefetch64:bin/seine-prefetch64:neon
