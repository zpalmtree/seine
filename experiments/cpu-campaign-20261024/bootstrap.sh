#!/usr/bin/env bash
# Run only on a campaign-owned, disposable Linux guest after timer verification.
set -euo pipefail
cd /home/ubuntu/campaign
mkdir -p results bin src
date -u --iso-8601=seconds > results/bootstrap-start.txt
sudo cat /run/systemd/shutdown/scheduled > results/shutdown-timer.txt
test -s results/shutdown-timer.txt
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq build-essential pkg-config curl python3
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o rustup-init.sh
sh rustup-init.sh -y --profile minimal --default-toolchain 1.93.0
source "$HOME/.cargo/env"
rustup component add --toolchain 1.93.0 clippy
tar xzf baseline.tar.gz -C src
export SEINE_GIT_TAG=v0.2.21
export SEINE_GIT_COMMIT=359265e325d8
export CARGO_BUILD_JOBS=8
cd src
cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_cpu_kernels
cp target/release/examples/verify_cpu_kernels ../bin/verify-generic
if [[ $(uname -m) == aarch64 ]]; then
  cargo +1.93.0 build --locked --release --no-default-features --bin seine
  cp target/release/seine ../bin/seine-generic
  if [[ ${SEINE_SKIP_GLOBAL_SHA3:-0} != 1 ]] && grep -qw sha3 /proc/cpuinfo; then
    RUSTFLAGS='-C target-feature=+sha3' CARGO_TARGET_DIR=target-sha3 cargo +1.93.0 build --locked --release --no-default-features --bin seine
    RUSTFLAGS='-C target-feature=+sha3' CARGO_TARGET_DIR=target-sha3 cargo +1.93.0 build --locked --release -p blocknet-pow-kernel --example verify_cpu_kernels
    cp target-sha3/release/seine ../bin/seine-sha3
    cp target-sha3/release/examples/verify_cpu_kernels ../bin/verify-sha3
  fi
else
  # Supplied published binary, verified against the release SHA256SUMS locally.
  test -x ../bin/seine-generic
fi
cd ..
sha256sum bin/* > results/binary-sha256.txt
rustc +1.93.0 --version > results/rustc.txt
date -u --iso-8601=seconds > results/bootstrap-end.txt
python3 screen.py
