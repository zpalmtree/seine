#!/usr/bin/env bash
# Tests the HiveOS custom miner package (packaging/hiveos) without mining. It
# runs shellcheck, h-config.sh against sample flight sheets, h-stats.sh against
# a fake control API, h-run.sh with a stand-in miner, and checks the layout.
#
#   scripts/test_hiveos_package.sh
#
# Optional:
#   SHELLCHECK=/path/to/shellcheck   shellcheck binary (skipped when missing)
#   HIVEOS_SANITIZE=/path/to/sanitize  HiveOS's /hive/bin/sanitize, to run the
#                                       stats through HiveOS's own sanitizer
#   HIVEOS_NVRTC_ARCHIVE=/path/to/cuda_nvrtc-linux-x86_64-12.8.61-archive.tar.xz
#                                     also build with NVRTC and load the library
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
pkg="$repo/packaging/hiveos"
data="$pkg/testdata"
work=$(mktemp -d)
server_pid=""
cleanup() {
	if [[ -n $server_pid ]]; then kill "$server_pid" 2>/dev/null || true; fi
	rm -rf "$work"
}
trap cleanup EXIT

passed=0
fail() {
	echo "FAIL: $*" >&2
	exit 1
}
ok() {
	passed=$((passed + 1))
	echo "ok - $*"
}
for tool in jq curl python3 xargs; do
	command -v "$tool" >/dev/null || fail "$tool is required"
done

address=8yAUM9aprE1ffiEm5KugyGH1TUqXQTuF9j5Mt2ZabBJmM9HAb5bfPkpa5JjgGQHU8bAmsuuJrcJVSXNWt5KLVn7Tv1bVm

# --- shellcheck -------------------------------------------------------------
shellcheck_bin=${SHELLCHECK:-shellcheck}
if command -v "$shellcheck_bin" >/dev/null 2>&1; then
	(cd "$pkg" && "$shellcheck_bin" -x -P SCRIPTDIR h-config.sh h-run.sh h-stats.sh build.sh h-manifest.conf ../lib/package.sh) ||
		fail "shellcheck packaging/hiveos"
	"$shellcheck_bin" "$repo/scripts/test_hiveos_package.sh" || fail "shellcheck test script"
	ok "shellcheck"
else
	echo "skip - shellcheck not installed (set SHELLCHECK=)"
fi

# --- fake HiveOS commands -----------------------------------------------------
mkdir -p "$work/bin"
cat >"$work/bin/gpu-detect" <<'EOF'
#!/usr/bin/env bash
case $1 in
NVIDIA) echo "${FAKE_NVIDIA:-0}" ;;
AMD) echo "${FAKE_AMD:-0}" ;;
*) echo 0 ;;
esac
EOF
cat >"$work/bin/cpu-temp" <<'EOF'
#!/usr/bin/env bash
echo "${FAKE_CPU_TEMP:-47}"
EOF
chmod +x "$work/bin/gpu-detect" "$work/bin/cpu-temp"
export PATH="$work/bin:$PATH"

home="$work/home"
mkdir -p "$home"
export SEINE_HIVEOS_HOME="$home"
export SEINE_HIVEOS_LOG_DIR="$work/log"
export SEINE_HIVEOS_DATA_DIR=/var/lib/seine

# --- h-config.sh ----------------------------------------------------------------
# Sources the manifest and h-config.sh the way HiveOS does (inside a function,
# from another directory) with only the given flight sheet variables set.
run_config() {
	rm -f "$home/seine.args"
	(
		cd "$work"
		unset CUSTOM_TEMPLATE CUSTOM_URL CUSTOM_PASS CUSTOM_USER_CONFIG WORKER_NAME
		for assignment in "$@"; do
			export "${assignment?}"
		done
		miner_config_gen() {
			# shellcheck source=/dev/null
			. "$pkg/h-manifest.conf"
			# shellcheck source=/dev/null
			. "$pkg/h-config.sh"
		}
		miner_config_gen
	) >"$work/config.out" 2>&1
}
# Arguments as HiveOS would write them, with the test home mapped back.
generated_args() {
	sed "s#$home#/hive/miners/custom/seine-hiveos#" "$home/seine.args"
}

run_config "CUSTOM_TEMPLATE=$address.rig01" \
	$'CUSTOM_URL=stratum+tcp://bntpool.com:3333\nstratum+ssl://backup.example.com:3443' \
	CUSTOM_PASS=x WORKER_NAME=rig01 FAKE_NVIDIA=2 FAKE_AMD=0 || fail "nvidia rig: $(cat "$work/config.out")"
diff -u "$data/nvidia-rig.args" <(generated_args) || fail "nvidia rig arguments"
ok "h-config: NVIDIA rig, failover pools, GPU-only default"

run_config "CUSTOM_TEMPLATE=$address" WORKER_NAME=rig02 FAKE_NVIDIA=1 \
	$'CUSTOM_USER_CONFIG=--backend nvidia,cpu\r\n--threads 2' || fail "extra config: $(cat "$work/config.out")"
diff -u "$data/extra-config.args" <(generated_args) || fail "extra config arguments"
ok "h-config: extra config overrides backends, rig name as worker, default pool, CRLF"

run_config "CUSTOM_TEMPLATE=$address.%WORKER_NAME%" WORKER_NAME=cpu-box || fail "cpu rig"
grep -qx -- "$address.cpu-box" "$home/seine.args" || fail "worker placeholder not expanded"
generated_args | grep -A1 -x -- --backend | grep -qx cpu || fail "rig without GPUs should mine on the CPU"
ok "h-config: no GPUs mines on CPU, %WORKER_NAME% expanded"

run_config "CUSTOM_TEMPLATE=@miner.one" WORKER_NAME=rig03 FAKE_NVIDIA=1 || fail "shortname"
generated_args | grep -A1 -x -- --pool-worker | grep -qx rig03 || fail "shortname login needs the rig name as worker"
grep -qx -- "@miner.one" "$home/seine.args" || fail "shortname changed"
run_config "CUSTOM_TEMPLATE=@miner.one.rig03" WORKER_NAME=rig03 FAKE_NVIDIA=1 || fail "shortname.worker"
generated_args | grep -A1 -x -- --address | grep -qx @miner.one || fail "shortname template keeps the rig name"
generated_args | grep -A1 -x -- --pool-worker | grep -qx rig03 || fail "shortname template worker"
ok "h-config: shortname wallet keeps its dots and mines as the rig name"
run_config "CUSTOM_TEMPLATE=solo:@miner.one.rig03" WORKER_NAME=rig03 FAKE_NVIDIA=1 || fail "solo shortname"
generated_args | grep -A1 -x -- --address | grep -qx solo:@miner.one || fail "solo shortname keeps the rig name"
generated_args | grep -A1 -x -- --pool-worker | grep -qx rig03 || fail "solo shortname worker"
run_config "CUSTOM_TEMPLATE=@miner.one" WORKER_NAME=rig03 FAKE_NVIDIA=1 "CUSTOM_USER_CONFIG=--rig-id mine" || fail "rig-id"
generated_args | grep -qx -- --pool-worker && fail "--rig-id in extra config should replace the rig name"
ok "h-config: solo: shortname splits off the rig name, --rig-id wins"

run_config "CUSTOM_TEMPLATE=$address.r" FAKE_NVIDIA=1 FAKE_AMD=2 || fail "mixed rig"
generated_args | grep -qx -- --gpu-only || fail "mixed rig should let seine pick the GPUs"
generated_args | grep -qx -- --backend && fail "mixed rig should not list backends"
ok "h-config: NVIDIA plus AMD"

run_config "CUSTOM_TEMPLATE=$address.r" FAKE_NVIDIA=1 \
	"CUSTOM_USER_CONFIG=--data-dir '/mnt/my data' --ui=plain --no-auto-update -o stratum+tcp://mine.example.com:3333" ||
	fail "quoted extra config"
[[ $(grep -c -x -- --data-dir "$home/seine.args") -eq 1 ]] || fail "--data-dir duplicated"
grep -qx -- "/mnt/my data" "$home/seine.args" || fail "quoted value with a space was split"
! grep -qx -- "--pool-url" "$home/seine.args" || fail "flight sheet pool added despite -o"
! grep -qx -- "--ui" "$home/seine.args" || fail "--ui duplicated"
[[ $(grep -c -x -- --no-auto-update "$home/seine.args") -eq 1 ]] || fail "--no-auto-update duplicated"
ok "h-config: user flags win and quoted values survive"

run_config WORKER_NAME=rig01 FAKE_NVIDIA=1 && fail "missing template should fail"
grep -q "Wallet and worker template" "$work/config.out" || fail "missing template message"
run_config "CUSTOM_TEMPLATE=%WAL%.rig01" && fail "unexpanded %WAL% should fail"
run_config "CUSTOM_TEMPLATE=$address" "CUSTOM_USER_CONFIG=--threads '2" && fail "unbalanced quote should fail"
grep -q "check the quotes" "$work/config.out" || fail "unbalanced quote message"
ok "h-config: rejects a missing wallet and broken extra config"

# --- h-stats.sh -------------------------------------------------------------------
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
export SEINE_HIVEOS_API_PORT=$port
token=test-token-123
cp "$data/miner-stats.json" "$work/body.json"
python3 -I "$repo/scripts/package_test_api.py" "$port" "$token" "$work/body.json" &
server_pid=$!
for _ in $(seq 50); do
	curl -s -o /dev/null "http://127.0.0.1:$port/" && break
	sleep 0.1
done

# Sources h-stats.sh the way the HiveOS agent does and prints khs and stats.
run_stats() {
	(
		cd "$work"
		miner_stats() {
			local khs stats
			# shellcheck source=/dev/null
			. "$pkg/h-stats.sh"
			printf '%s\n%s\n' "$khs" "$stats"
		}
		miner_stats
	) 2>"$work/stats.err"
}
expect_stats() {
	local label=$1 out=$2 expected_khs=$3 filter=$4
	local khs stats
	khs=$(sed -n 1p <<<"$out")
	stats=$(sed -n 2p <<<"$out")
	[[ $khs == "$expected_khs" ]] || fail "$label: khs=$khs, want $expected_khs ($(cat "$work/stats.err"))"
	[[ $stats != *\\* ]] || fail "$label: stats contain a backslash, which the agent's read mangles"
	jq -e "$filter" <<<"$stats" >/dev/null || fail "$label: unexpected stats $stats"
}

out=$(run_stats)
expect_stats "no cookie" "$out" 0 '. == null'
ok "h-stats: miner not running reports khs=0 and null stats"

echo "$token" >"$home/control-api.cookie"
full_filter='
	.hs == [600.25, 150.12, 300, 200.12]
	and .hs_units == "hs"
	and .temp == ["61", "58", "66", 47]
	and .fan == ["45", "50", "70", null]
	and .bus_numbers == [1, 3, 10, null]
	and .ar == [50, 2]
	and .uptime == 3605
	and .ver == "0.2.46"
	and .algo == "blocknet"
	and .total_khs == 1.2505
	and (.hs | length) == (.temp | length)'
out=$(gpu_stats=$(<"$data/gpu-stats.json") run_stats)
expect_stats "gpu_stats variable" "$out" 1.2505 "$full_filter"
ok "h-stats: per-device rates, PCI bus mapping, temps, fans, shares"

out=$(GPU_STATS_JSON="$data/gpu-stats.json" run_stats)
expect_stats "GPU_STATS_JSON file" "$out" 1.2505 "$full_filter"
ok "h-stats: reads gpu-stats.json when \$gpu_stats is unset"

out=$(FAKE_CPU_TEMP=null run_stats)
expect_stats "no GPU stats" "$out" 1.2505 '.temp == [null, null, null, null] and .bus_numbers == [null, null, null, null] and .hs == [600.25, 150.12, 300, 200.12]'
ok "h-stats: missing HiveOS GPU data leaves nulls"

if [[ -n ${HIVEOS_SANITIZE:-} ]]; then
	sanitized=$(
		stats=$(gpu_stats=$(<"$data/gpu-stats.json") run_stats | sed -n 2p)
		# HiveOS's scripts don't run under set -eu.
		set +eu
		# shellcheck disable=SC1090
		. "$HIVEOS_SANITIZE"
		sanitize_miner_stats "$stats"
	)
	jq -e '.hs == [600.25, 150.12, 300, 200.12] and .fan == [45, 50, 70, null] and .bus_numbers == [1, 3, 10, null] and .ar == [50, 2] and .temp == [61, 58, 66, 47]' <<<"$sanitized" >/dev/null ||
		fail "HiveOS sanitizer output: $sanitized"
	ok "h-stats: output passes HiveOS's sanitize_miner_stats"
fi

jq '.mining = null | .lifecycle = "starting" | .running = false' "$data/miner-stats.json" >"$work/body.json"
out=$(run_stats)
expect_stats "starting" "$out" 0 '.hs == [] and .ar == [0, 0] and .uptime == 0 and .ver == "0.2.46"'
ok "h-stats: miner starting reports zero hashrate with its version"

echo wrong-token >"$home/control-api.cookie"
out=$(run_stats)
expect_stats "bad token" "$out" 0 '. == null'
kill "$server_pid"
wait "$server_pid" 2>/dev/null || true
server_pid=""
echo "$token" >"$home/control-api.cookie"
out=$(run_stats)
expect_stats "API down" "$out" 0 '. == null'
ok "h-stats: rejected token or stopped API reports the miner down"

# --- build.sh ---------------------------------------------------------------------
release="$work/release"
mkdir -p "$release"
cat >"$release/seine" <<'EOF'
#!/usr/bin/env bash
# Stand-in miner: records its arguments and environment, then logs until Ctrl+C.
printf '%s\n' "$@" >"$SEINE_TEST_ARGS"
echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH CUDA_DEVICE_ORDER=$CUDA_DEVICE_ORDER"
echo "mining"
if [[ -n ${SEINE_TEST_WAIT_FOR_INT:-} ]]; then
	trap 'sleep 0.3; echo "stopped cleanly"; exit 0' INT
	while :; do sleep 0.1; done
fi
exit 3
EOF
chmod +x "$release/seine"
cp "$repo/LICENSE" "$release/SEINE-LICENSE.txt"
tar -C "$release" -czf "$work/seine-linux-x86_64.tar.gz" .
mkdir -p "$work/out"
package=$("$pkg/build.sh" --archive "$work/seine-linux-x86_64.tar.gz" --version v0.2.46-rc1 \
	--out-dir "$work/out" --no-nvrtc)
[[ $package == "$work/out/seine-hiveos-0.2.46_rc1.tar.gz" ]] || fail "package name $package"

# HiveOS's custom-get derives the miner name and version from the file name.
url="https://github.com/zpalmtree/seine/releases/download/v0.2.46-rc1/$(basename "$package")"
basename=$(basename -s .tar.gz "$url")
version=$(echo "$basename" | awk -F '-' '{ print $NF }')
# shellcheck disable=SC2001 # the same commands HiveOS runs
miner=$(echo "$basename" | sed 's/'-"$version"'$//')
[[ $miner == seine-hiveos && $version == 0.2.46_rc1 ]] || fail "HiveOS parses $miner / $version"

entries=$(tar -tzf "$package")
[[ $(cut -d/ -f1 <<<"$entries" | sort -u) == seine-hiveos ]] || fail "archive must hold only seine-hiveos/: $entries"
for file in seine h-manifest.conf h-config.sh h-run.sh h-stats.sh SEINE-LICENSE.txt PRIVACY.md README.md SHA256SUMS.txt; do
	grep -qx "seine-hiveos/$file" <<<"$entries" || fail "package is missing $file"
done
mkdir -p "$work/hive/miners/custom"
tar -xzf "$package" -C "$work/hive/miners/custom"
installed="$work/hive/miners/custom/seine-hiveos"
for file in seine h-config.sh h-run.sh h-stats.sh; do
	[[ -x $installed/$file ]] || fail "$file is not executable"
done
grep -qx 'CUSTOM_VERSION=0.2.46_rc1' "$installed/h-manifest.conf" || fail "manifest version"
! grep -lq $'\r' "$installed"/h-* || fail "CRLF in package scripts"
(cd "$installed" && sha256sum --check --quiet SHA256SUMS.txt) || fail "package checksums"
ok "build.sh: HiveOS name/version, single top-level folder, executable scripts"

# --- h-run.sh -----------------------------------------------------------------------
export SEINE_HIVEOS_HOME="$installed"
export SEINE_TEST_ARGS="$work/run.args"
printf '%s\n' --address "$address.rig01" --data-dir "/mnt/my data" >"$installed/seine.args"
set +e
"$installed/h-run.sh" >"$work/run.out" 2>&1
status=$?
set -e
[[ $status -eq 3 ]] || fail "h-run.sh should return seine's exit status, got $status"
diff -u "$installed/seine.args" "$work/run.args" || fail "h-run.sh changed the arguments"
grep -q "LD_LIBRARY_PATH=$installed/lib" "$work/log/seine-hiveos.log" || fail "bundled lib dir not on LD_LIBRARY_PATH"
grep -q "CUDA_DEVICE_ORDER=PCI_BUS_ID" "$work/log/seine-hiveos.log" || fail "CUDA_DEVICE_ORDER"
ok "h-run.sh: passes arguments verbatim, logs, returns the miner's status"

: >"$work/log/seine-hiveos.log"
# Background jobs start with SIGINT ignored; give h-run.sh its own process
# group with the default handler, like HiveOS's screen window.
SEINE_TEST_WAIT_FOR_INT=1 python3 -I -c '
import os, signal, sys
signal.signal(signal.SIGINT, signal.SIG_DFL)
os.setsid()
os.execv(sys.argv[1], sys.argv[1:])' "$installed/h-run.sh" >/dev/null 2>&1 &
run_pid=$!
for _ in $(seq 50); do
	grep -q mining "$work/log/seine-hiveos.log" 2>/dev/null && break
	sleep 0.1
done
# Ctrl+C in HiveOS's screen window reaches the whole foreground process group.
kill -INT -- "-$run_pid"
wait "$run_pid" || true
grep -q "stopped cleanly" "$work/log/seine-hiveos.log" || fail "output after Ctrl+C was lost"
ok "h-run.sh: Ctrl+C lets the miner log its shutdown"

if [[ -n ${HIVEOS_NVRTC_ARCHIVE:-} ]]; then
	package=$("$pkg/build.sh" --archive "$work/seine-linux-x86_64.tar.gz" --version 0.2.46 \
		--out-dir "$work/out" --nvrtc-archive "$HIVEOS_NVRTC_ARCHIVE")
	rm -rf "$installed"
	tar -xzf "$package" -C "$work/hive/miners/custom"
	[[ -L $installed/lib/libnvrtc.so.12 && -L $installed/lib/libnvrtc-builtins.so.12.8 ]] ||
		fail "NVRTC symlinks"
	[[ -f $installed/NVIDIA-CUDA-NVRTC-LICENSE.txt ]] || fail "NVRTC license"
	nvrtc_version=$(LD_LIBRARY_PATH="$installed/lib" python3 -I -c '
import ctypes
lib = ctypes.CDLL("libnvrtc.so.12")
major, minor = ctypes.c_int(), ctypes.c_int()
assert lib.nvrtcVersion(ctypes.byref(major), ctypes.byref(minor)) == 0
print(f"{major.value}.{minor.value}")')
	[[ $nvrtc_version == 12.8 ]] || fail "bundled NVRTC reports $nvrtc_version"
	ok "build.sh: bundled NVRTC 12.8 loads from the package lib dir"
fi

echo "all $passed HiveOS package checks passed"
