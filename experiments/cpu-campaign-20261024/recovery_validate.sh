#!/usr/bin/env bash
# Rerun the previously failing resource lifecycle on a frozen fixed candidate.
set -euo pipefail
cd /home/ubuntu/campaign
test -f results/SCREEN-COMPLETE
source "$HOME/.cargo/env"
rustup component add --toolchain 1.93.0 clippy
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
export CARGO_BUILD_JOBS=8
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/recovery-1246967
export SEINE_GIT_TAG=v0.2.21-cpu-recovery
export SEINE_GIT_COMMIT=1246967
mkdir -p src-recovery results/recovery-build
tar xzf cpu-recovery.tar.gz -C src-recovery
cd src-recovery
cargo +1.93.0 test --locked --release --workspace --no-default-features -- --test-threads=1 > ../results/recovery-build/workspace-tests.log 2>&1
cargo +1.93.0 build --locked --release --no-default-features --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-recovery
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_cpu_kernels
timeout 600 "$CARGO_TARGET_DIR/release/examples/verify_cpu_kernels" > ../results/recovery-build/full-size-parity.log
cargo +1.93.0 test --locked --release --no-default-features --bin seine real_arena_smoke -- --ignored --test-threads=1 > ../results/recovery-build/real-arena-switches-and-growth.log 2>&1
cargo +1.93.0 clippy --locked --release -p blocknet-pow-kernel --all-targets -- -D warnings > ../results/recovery-build/kernel-clippy.log 2>&1
if [[ $(uname -m) == aarch64 ]]; then
  cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_neon_dispatch
  timeout 600 "$CARGO_TARGET_DIR/release/examples/verify_neon_dispatch" > ../results/recovery-build/arm-runtime-dispatch-parity.log
fi
cd ..
sha256sum bin/seine-recovery cpu-recovery.tar.gz > results/recovery-build/identities.txt
touch results/recovery-build/COMPLETE
if [[ $(uname -m) == aarch64 ]] && grep -qw sha3 /proc/cpuinfo; then
  python3 resource_lifecycle.py --binary bin/seine-recovery --cpus 0-3 --initial-seconds 360 --expect-kernel neon-sha3
else
  python3 resource_lifecycle.py --binary bin/seine-recovery --cpus 0-3
fi
