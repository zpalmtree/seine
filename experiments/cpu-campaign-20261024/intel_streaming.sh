#!/usr/bin/env bash
set -euo pipefail
cd /home/ubuntu/campaign
test -f results/SCREEN-COMPLETE
test -f results/interhash/COMPLETE
source "$HOME/.cargo/env"
export CARGO_BUILD_JOBS=8
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/streaming-baseline
mkdir -p results/streaming-build src-streaming
export SEINE_GIT_TAG=v0.2.21-cpu-only
export SEINE_GIT_COMMIT=359265e325d8
cd src
cargo +1.93.0 build --locked --release --no-default-features --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-cpu-baseline
cd ..
tar xzf streaming-verify-v2.tar.gz -C src-streaming
cd src-streaming
export CARGO_TARGET_DIR=/home/ubuntu/campaign/targets/streaming-d283877
export SEINE_GIT_TAG=v0.2.21-streaming-experiment
export SEINE_GIT_COMMIT=d283877
cargo +1.93.0 test --locked --release -p blocknet-pow-kernel -- --test-threads=1 > ../results/streaming-build/tests.log 2>&1
cargo +1.93.0 build --locked --release --no-default-features --bin seine
cp "$CARGO_TARGET_DIR/release/seine" ../bin/seine-streaming
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_streaming_dispatch
cp "$CARGO_TARGET_DIR/release/examples/verify_streaming_dispatch" ../bin/verify-streaming
timeout 300 ../bin/verify-streaming > ../results/streaming-build/full-size-parity.log
cd ..
sha256sum bin/* streaming-verify-v2.tar.gz baseline.tar.gz > results/streaming-build/identities.txt
touch results/streaming-build/COMPLETE
python3 matrix.py --out results/streaming-screen --workers 1 4 8 12 --pairs 2 --seconds 12 \
  --variant shipping512:bin/seine-cpu-baseline:avx512 \
  --variant shipping256:bin/seine-cpu-baseline:avx512-256 \
  --variant ring:bin/seine-streaming:avx512-256
