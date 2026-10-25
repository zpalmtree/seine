#!/usr/bin/env bash
set -euo pipefail
cd /home/ubuntu/campaign
source "$HOME/.cargo/env"
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
export CARGO_BUILD_JOBS=8
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/arm-auto-3122893
export SEINE_GIT_TAG=v0.2.21-neon-auto
export SEINE_GIT_COMMIT=3122893
mkdir -p src-arm-auto results/arm-auto-build
tar xzf neon-auto.tar.gz -C src-arm-auto
cd src-arm-auto
cargo +1.93.0 test --locked --release --workspace --no-default-features -- --test-threads=1 > ../results/arm-auto-build/workspace-tests.log 2>&1
cargo +1.93.0 build --locked --release --no-default-features --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-neon-auto
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_neon_dispatch
timeout 300 "$CARGO_TARGET_DIR/release/examples/verify_neon_dispatch" > ../results/arm-auto-build/full-size-parity.log
cargo +1.93.0 test --locked --release --no-default-features --bin seine adaptive_kernel_real_arena_smoke -- --ignored --test-threads=1 > ../results/arm-auto-build/real-arena-switches.log 2>&1
cargo +1.93.0 clippy --locked --release -p blocknet-pow-kernel --all-targets -- -D warnings > ../results/arm-auto-build/kernel-clippy.log 2>&1
cd ..
sha256sum bin/seine-neon-auto neon-auto.tar.gz > results/arm-auto-build/identities.txt
touch results/arm-auto-build/COMPLETE
python3 resource_lifecycle.py --binary bin/seine-neon-auto --cpus 0-3 --initial-seconds 360 --expect-kernel neon-sha3
