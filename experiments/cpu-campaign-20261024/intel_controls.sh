#!/usr/bin/env bash
set -euo pipefail
cd /home/ubuntu/campaign
test -f results/streaming-screen/COMPLETE
source "$HOME/.cargo/env"
export CARGO_BUILD_JOBS=8
export SEINE_GIT_TAG=v0.2.21
export SEINE_GIT_COMMIT=359265e325d85c9c7b6e3eed7b5b126fcec63f9c
mkdir -p results/control-build src-clean-control
tar xzf baseline.tar.gz -C src-clean-control
# No target directory is shared across source revisions or feature profiles.
# Preserve the earlier controls and record symbol provenance before rebuilding.
for binary in bin/seine-generic bin/seine-cpu-baseline bin/seine-streaming; do
  nm -C "$binary" > "results/control-build/$(basename "$binary").symbols.txt"
done
cd src-clean-control
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/control-default
cargo +1.93.0 build --locked --release --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-clean-default
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_cpu_kernels
timeout 300 "$CARGO_TARGET_DIR/release/examples/verify_cpu_kernels" > ../results/control-build/default-full-size-parity.log
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/control-cpu
cargo +1.93.0 build --locked --release --no-default-features --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-clean-cpu
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_cpu_kernels
timeout 300 "$CARGO_TARGET_DIR/release/examples/verify_cpu_kernels" > ../results/control-build/cpu-full-size-parity.log
cd ..
mkdir -p src-streaming-clean
tar xzf streaming-verify-v2.tar.gz -C src-streaming-clean
cd src-streaming-clean
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/streaming-clean
export SEINE_GIT_TAG=v0.2.21-streaming-experiment
export SEINE_GIT_COMMIT=d283877
cargo +1.93.0 build --locked --release --no-default-features --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-clean-streaming
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_streaming_dispatch
timeout 300 "$CARGO_TARGET_DIR/release/examples/verify_streaming_dispatch" > ../results/control-build/streaming-full-size-parity.log
cd ..
for binary in bin/seine-clean-default bin/seine-clean-cpu bin/seine-clean-streaming; do
  nm -C "$binary" > "results/control-build/$(basename "$binary").symbols.txt"
done
if grep -q fill_streaming_ring results/control-build/seine-clean-default.symbols.txt results/control-build/seine-clean-cpu.symbols.txt; then
  echo 'Unexpected streaming code in a clean baseline' >&2
  exit 1
fi
grep -q fill_streaming_ring results/control-build/seine-clean-streaming.symbols.txt
sha256sum bin/* baseline.tar.gz > results/control-build/identities.txt
touch results/control-build/COMPLETE
python3 matrix.py --out results/control-screen --workers 1 12 --pairs 2 --seconds 12 \
  --variant release512:bin/seine-generic:avx512 \
  --variant release256:bin/seine-generic:avx512-256 \
  --variant rebuilt512:bin/seine-clean-default:avx512 \
  --variant rebuilt256:bin/seine-clean-default:avx512-256 \
  --variant cpu256:bin/seine-clean-cpu:avx512-256 \
  --variant ring:bin/seine-clean-streaming:avx512-256
