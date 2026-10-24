#!/usr/bin/env bash
# Candidate correctness and lifecycle checks on a fresh x86 architecture.
set -euo pipefail
cd /home/ubuntu/campaign
test -f results/SCREEN-COMPLETE
source "$HOME/.cargo/env"
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
export CARGO_BUILD_JOBS=8
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/generation-functional-3122893
export SEINE_GIT_TAG=v0.2.21-cpu-functional
export SEINE_GIT_COMMIT=3122893
mkdir -p src-functional results/functional-build
tar xzf neon-auto.tar.gz -C src-functional
cd src-functional
cargo +1.93.0 test --locked --release --workspace --no-default-features -- --test-threads=1 > ../results/functional-build/workspace-tests.log 2>&1
cargo +1.93.0 build --locked --release --no-default-features --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-functional
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_cpu_kernels
timeout 600 "$CARGO_TARGET_DIR/release/examples/verify_cpu_kernels" > ../results/functional-build/full-size-parity.log
cargo +1.93.0 test --locked --release --no-default-features --bin seine adaptive_kernel_real_arena_smoke -- --ignored --test-threads=1 > ../results/functional-build/real-arena-switches.log 2>&1
cargo +1.93.0 clippy --locked --release -p blocknet-pow-kernel --all-targets -- -D warnings > ../results/functional-build/kernel-clippy.log 2>&1
cd ..
sha256sum bin/seine-functional neon-auto.tar.gz > results/functional-build/identities.txt
touch results/functional-build/COMPLETE
python3 resource_lifecycle.py --binary bin/seine-functional --cpus 0-3
