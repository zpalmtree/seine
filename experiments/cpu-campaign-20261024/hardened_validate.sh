#!/usr/bin/env bash
# Frozen final candidate: memory recovery, bounded parked workers and ARM dispatch.
set -euo pipefail
cd /home/ubuntu/campaign
test -f results/SCREEN-COMPLETE
source "$HOME/.cargo/env"
rustup component add --toolchain 1.93.0 clippy
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
export CARGO_BUILD_JOBS=8
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/hardened-636e688
export SEINE_GIT_TAG=v0.2.21-cpu-hardened
export SEINE_GIT_COMMIT=636e6887c405c41d732433b1eeb018cefcdc962d
echo '2fb4e195164ea19af1e73da06616d7462f0f746ba3db6390239b81ce953f53b8  cpu-hardened-636e688.tar.gz' | sha256sum --check
mkdir src-hardened results/hardened-build
tar xzf cpu-hardened-636e688.tar.gz -C src-hardened
cd src-hardened
cargo +1.93.0 test --locked --release --workspace --no-default-features -- --test-threads=1 > ../results/hardened-build/workspace-tests.log 2>&1
cargo +1.93.0 build --locked --release --no-default-features --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-hardened
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_cpu_kernels
cp "$CARGO_TARGET_DIR/release/examples/verify_cpu_kernels" ../bin/verify-hardened
timeout 600 ../bin/verify-hardened > ../results/hardened-build/full-size-parity.log
cargo +1.93.0 test --locked --release --no-default-features --bin seine real_arena_smoke -- --ignored --test-threads=1 > ../results/hardened-build/real-arena-switches-and-growth.log 2>&1
cargo +1.93.0 clippy --locked --release -p blocknet-pow-kernel --all-targets -- -D warnings > ../results/hardened-build/kernel-clippy.log 2>&1
if [[ $(uname -m) == aarch64 ]]; then
  cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_neon_dispatch
  timeout 600 "$CARGO_TARGET_DIR/release/examples/verify_neon_dispatch" > ../results/hardened-build/arm-runtime-dispatch-parity.log
fi
cd ..
sha256sum bin/seine-hardened bin/verify-hardened cpu-hardened-636e688.tar.gz > results/hardened-build/identities.txt
touch results/hardened-build/COMPLETE
python3 pid_pool_probe.py --binary bin/seine-hardened
if [[ $(uname -m) == aarch64 ]] && grep -qw sha3 /proc/cpuinfo; then
  python3 resource_lifecycle.py --binary bin/seine-hardened --cpus 0-3 --initial-seconds 360 --expect-kernel neon-sha3
else
  python3 resource_lifecycle.py --binary bin/seine-hardened --cpus 0-3
fi
