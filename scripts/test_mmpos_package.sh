#!/usr/bin/env bash
# Tests the mmpOS custom miner package (packaging/mmpos) without mining. It
# builds the package around a stand-in miner, checks the archive layout, runs
# mmp-launch.sh with sample miner profile arguments and mmp-stats.sh against a
# fake control API.
#
#   scripts/test_mmpos_package.sh
#
# Optional:
#   SHELLCHECK=/path/to/shellcheck   shellcheck binary (skipped when missing)
#   MMPOS_JQ=/path/to/jq             jq to bundle (default: the jq on PATH)
#   MMPOS_NVRTC_ARCHIVE=/path/to/cuda_nvrtc-linux-x86_64-12.8.61-archive.tar.xz
#                                    also build with NVRTC and load the library
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
pkg="$repo/packaging/mmpos"
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
for tool in jq curl python3 tar; do
	command -v "$tool" >/dev/null || fail "$tool is required"
done

address=8yAUM9aprE1ffiEm5KugyGH1TUqXQTuF9j5Mt2ZabBJmM9HAb5bfPkpa5JjgGQHU8bAmsuuJrcJVSXNWt5KLVn7Tv1bVm

# --- shellcheck -------------------------------------------------------------
shellcheck_bin=${SHELLCHECK:-shellcheck}
if command -v "$shellcheck_bin" >/dev/null 2>&1; then
	(cd "$pkg" && "$shellcheck_bin" -x -P SCRIPTDIR mmp-launch.sh mmp-stats.sh build.sh mmp-external.conf) ||
		fail "shellcheck packaging/mmpos"
	"$shellcheck_bin" "$repo/scripts/test_mmpos_package.sh" || fail "shellcheck test script"
	ok "shellcheck"
else
	echo "skip - shellcheck not installed (set SHELLCHECK=)"
fi

# --- build.sh ---------------------------------------------------------------------
release="$work/release"
mkdir -p "$release"
cat >"$release/seine" <<'EOF'
#!/usr/bin/env bash
# Stand-in miner: records its arguments, working directory and environment,
# then logs until Ctrl+C when asked to.
printf '%s\n' "$@" >"$SEINE_TEST_ARGS"
echo "cwd=$PWD LD_LIBRARY_PATH=$LD_LIBRARY_PATH CUDA_DEVICE_ORDER=$CUDA_DEVICE_ORDER"
if [[ -t 1 ]]; then echo "stdout=terminal"; else echo "stdout=pipe"; fi
if [[ -n ${SEINE_TEST_WAIT_FOR_INT:-} ]]; then
	trap 'sleep 0.3; echo "stopped cleanly"; exit 0' INT
	echo mining
	while :; do sleep 0.1; done
fi
exit 3
EOF
chmod +x "$release/seine"
cp "$repo/LICENSE" "$release/SEINE-LICENSE.txt"
tar -C "$release" -czf "$work/seine-linux-x86_64.tar.gz" .
mkdir -p "$work/out"
jq_binary=${MMPOS_JQ:-$(command -v jq)}
package=$("$pkg/build.sh" --archive "$work/seine-linux-x86_64.tar.gz" --version v0.2.46-rc1 \
	--out-dir "$work/out" --no-nvrtc --jq "$jq_binary")
[[ $package == "$work/out/seine-mmpos-0.2.46-rc1.tar.gz" ]] || fail "package name $package"

# mmpOS unpacks a custom miner into /opt/mmp/miners/custom-<hash> and runs
# ./mmp-launch.sh and ./mmp-stats.sh from there, so every file sits under ./
# whether or not the agent strips a top-level directory.
entries=$(tar -tzf "$package")
! grep -qv '^\./' <<<"$entries" || fail "entries outside ./: $entries"
for file in seine mmp-launch.sh mmp-stats.sh mmp-external.conf bin/jq SEINE-LICENSE.txt \
	JQ-LICENSE.txt PRIVACY.md README.md SHA256SUMS.txt; do
	grep -qx "\./$file" <<<"$entries" || fail "package is missing $file"
done
root="$work/root"
installed="$root/opt/mmp/miners/custom-1a2b3"
mkdir -p "$installed" "$work/stripped"
tar -xzf "$package" -C "$installed"
tar -xzf "$package" -C "$work/stripped" --strip-components=1
diff -r "$installed" "$work/stripped" >/dev/null || fail "layout differs with the top-level directory stripped"
for file in seine mmp-launch.sh mmp-stats.sh bin/jq; do
	[[ -x $installed/$file ]] || fail "$file is not executable"
done
grep -qx 'EXTERNAL_VERSION=0.2.46-rc1' "$installed/mmp-external.conf" || fail "external conf version"
! grep -lq $'\r' "$installed"/mmp-* || fail "CRLF in package scripts"
(cd "$installed" && sha256sum --check --quiet SHA256SUMS.txt) || fail "package checksums"
ok "build.sh: files at the archive root, executable scripts, version, checksums"

# --- mmp-launch.sh --------------------------------------------------------------
export SEINE_MMPOS_ROOT="$root"
export SEINE_TEST_ARGS="$work/run.args"
mkdir -p "$root/run"

# Runs the profile command line the way the mmpOS agent does: a bash script
# in the miner directory. Prints the arguments seine received, with the test
# root mapped back to /.
run_launch() {
	rm -f "$SEINE_TEST_ARGS"
	printf 'cd %q\n%s\n' "$installed" "$1" >"$work/miner.sh"
	set +e
	env -u SEINE_MMPOS_GLIBC "${@:2}" bash "$work/miner.sh" >"$work/launch.out" 2>&1
	launch_status=$?
	set -e
}
launched_args() {
	sed "s#$root##" "$SEINE_TEST_ARGS"
}
default_line="./mmp-launch.sh --coin BNT tcp --pool bntpool.com:3333 --user $address.rig01 --password x --api-port 0"

cp "$data/gpu-info.json" "$root/run/gpu-info.json"
run_launch "./mmp-launch.sh --coin BNT tls --pool pool.example.com:3443 --user $address.rig01 --password x --api-port 4044"
[[ $launch_status -eq 3 ]] || fail "launch should exit with seine's status: $(cat "$work/launch.out")"
diff -u "$data/gpu-rig.args" <(launched_args) || fail "GPU rig arguments"
diff -u "$installed/seine.args" "$SEINE_TEST_ARGS" || fail "seine.args differs from what seine got"
grep -q "cwd=$installed " "$work/launch.out" || fail "seine should run from the package directory"
grep -q "LD_LIBRARY_PATH=$installed/lib" "$work/launch.out" || fail "bundled lib dir not on LD_LIBRARY_PATH"
grep -q "CUDA_DEVICE_ORDER=PCI_BUS_ID" "$work/launch.out" || fail "CUDA_DEVICE_ORDER"
[[ -d $root/opt/mmp/miners/seine-data ]] || fail "data dir beside the package not created"
ok "mmp-launch: TLS pool, NVIDIA plus AMD from gpu-info.json, API port, GPU-only default"

run_launch "./mmp-launch.sh --coin BNT tcp --pool bntpool.com:3333 --user @miner.one.rig02 --password x --api-port 0 --backend nvidia,cpu --threads 2"
diff -u "$data/extra-args.args" <(launched_args) || fail "extra arguments"
ok "mmp-launch: shortname keeps its dots and mines as the rig name, user backends win"
run_launch "./mmp-launch.sh --coin BNT tcp --pool bntpool.com:3333 --user solo:@miner.one.rig02 --password x --api-port 0"
launched_args | grep -A1 -x -- --address | grep -qx solo:@miner.one || fail "solo shortname keeps the rig name"
launched_args | grep -A1 -x -- --pool-worker | grep -qx rig02 || fail "solo shortname worker"
run_launch "./mmp-launch.sh --coin BNT tcp --pool bntpool.com:3333 --user @miner.one.rig02 --password x --api-port 0 --rig-id mine"
launched_args | grep -qx -- --pool-worker && fail "--rig-id should replace the rig name"
ok "mmp-launch: solo: shortname splits off the rig name, --rig-id wins"

if command -v script >/dev/null; then
	# mmpOS runs the miner in screen, so its output is a terminal.
	script -qec "bash $(printf %q "$work/miner.sh")" /dev/null >"$work/launch.out" 2>&1 || true
	grep -q "stdout=pipe" "$work/launch.out" || fail "seine should not log to a terminal: $(cat "$work/launch.out")"
	ok "mmp-launch: seine logs plain lines through a pipe inside a terminal"
fi

# Ctrl+C in the screen window reaches the whole foreground process group.
# Background jobs start with SIGINT ignored; give the launcher its own
# process group with the default handler, like the screen window.
: >"$work/int.out"
SEINE_TEST_WAIT_FOR_INT=1 python3 -I -c '
import os, signal, sys
signal.signal(signal.SIGINT, signal.SIG_DFL)
os.setsid()
os.execv(sys.argv[1], sys.argv[1:])' /bin/bash "$work/miner.sh" >"$work/int.out" 2>&1 &
run_pid=$!
for _ in $(seq 50); do
	grep -q mining "$work/int.out" 2>/dev/null && break
	sleep 0.1
done
kill -INT -- "-$run_pid"
wait "$run_pid" || true
grep -q "stopped cleanly" "$work/int.out" || fail "output after Ctrl+C was lost: $(cat "$work/int.out")"
ok "mmp-launch: Ctrl+C lets the miner log its shutdown"

rm "$root/run/gpu-info.json"
run_launch "$default_line"
launched_args | grep -A1 -x -- --backend | grep -qx cpu || fail "rig without GPUs should mine on the CPU"
mkdir -p "$root/dev" && touch "$root/dev/nvidia0" "$root/dev/nvidia1" "$root/dev/nvidiactl"
run_launch "$default_line"
launched_args | grep -A1 -x -- --backend | grep -qx nvidia || fail "NVIDIA device nodes without gpu-info.json"
rm -rf "${root:?}/dev"
ok "mmp-launch: no GPUs mines on the CPU; finds NVIDIA GPUs without gpu-info.json"

run_launch "./mmp-launch.sh --coin BNT --pool : --user $address --password --api-port %api_port%" HOSTNAME=box7
launched_args | grep -A1 -x -- --pool-url | grep -qx stratum+tcp://bntpool.com:3333 || fail "empty pool should use bntpool.com"
launched_args | grep -A1 -x -- --pool-worker | grep -qx box7 || fail "wallet without worker should use the host name"
launched_args | grep -A1 -x -- --api-bind | grep -qx 127.0.0.1:9977 || fail "unexpanded API port"
ok "mmp-launch: empty fields fall back to bntpool.com, the host name and port 9977"

for form in "stratum+ssl:// --pool pool.example.com:3443" "--pool stratum+ssl://pool.example.com:3443" \
	"--pool ssl://pool.example.com:3443" "--pool tls://pool.example.com:3443"; do
	run_launch "./mmp-launch.sh --coin BNT $form --user $address.rig01 --password x --api-port 4444"
	launched_args | grep -A1 -x -- --pool-url | grep -qx stratum+ssl://pool.example.com:3443 ||
		fail "TLS pool from '$form': $(cat "$work/launch.out")"
done
ok "mmp-launch: every form of %pool_protocol% selects a TLS pool"

run_launch "$default_line --pool-url stratum+tcp://a.example.com:3333,stratum+ssl://b.example.com:3443 --ui=tui --api-bind 127.0.0.1:5555 --data-dir '/mnt/my data'"
[[ $(grep -c -x -- --pool-url "$SEINE_TEST_ARGS") -eq 1 ]] || fail "--pool-url duplicated"
grep -qx -- "stratum+tcp://a.example.com:3333,stratum+ssl://b.example.com:3443" "$SEINE_TEST_ARGS" || fail "backup pools"
! grep -qx -- --ui "$SEINE_TEST_ARGS" || fail "--ui duplicated"
[[ $(grep -c -x -- --api-bind "$SEINE_TEST_ARGS") -eq 1 ]] || fail "--api-bind duplicated"
grep -qx -- "/mnt/my data" "$SEINE_TEST_ARGS" || fail "quoted value with a space was split"
ok "mmp-launch: user flags win, backup pools, quoted values"

# The guide's import profile: mmpOS substitutes the placeholders and runs the
# arguments from the package directory.
profile=$(awk '/^   ```json$/ { take = 1; next } /^   ```$/ { take = 0 } take' "$repo/docs/guides/mmpos.md")
jq -e '.miner_profile.miner == "custom" and .miner_profile.coin == "BNT"
	and (.miner_profile.custom_url | test("/vX.Y.Z/seine-mmpos-X.Y.Z.tar.gz$"))
	and (.pools | length) == 1 and .pools[0].coin == "BNT" and .pools[0].url == "bntpool.com"
	and .wallets[0].coin == "BNT"' <<<"$profile" >/dev/null || fail "guide import profile: $profile"
line=$(jq -r .miner_profile.commandline <<<"$profile")
line=${line//%coin%/BNT}
line=${line//%pool_server%/bntpool.com}
line=${line//%pool_port%/3333}
line=${line//%user%/$address.rig01}
line=${line//%password%/x}
line=${line//%api_port%/$(jq -r .miner_profile.api_port <<<"$profile")}
run_launch "$line"
[[ $launch_status -eq 3 ]] || fail "guide arguments: $(cat "$work/launch.out")"
launched_args | grep -A1 -x -- --pool-url | grep -qx stratum+tcp://bntpool.com:3333 || fail "guide pool"
launched_args | grep -A1 -x -- --api-bind | grep -qx 127.0.0.1:4444 || fail "guide API port"
ok "guide: the import profile parses and its arguments start seine"

run_launch "./mmp-launch.sh --coin BNT tcp --pool bntpool.com:3333 --user .rig01 --password x"
[[ $launch_status -ne 0 && ! -e $SEINE_TEST_ARGS ]] || fail "missing wallet should fail without starting seine"
grep -q "no wallet" "$work/launch.out" || fail "missing wallet message"
run_launch "./mmp-launch.sh --user %wallet_address%.rig01"
grep -q "no wallet" "$work/launch.out" || fail "unexpanded wallet placeholder"
run_launch "$default_line" SEINE_MMPOS_GLIBC="glibc 2.27"
[[ $launch_status -ne 0 && ! -e $SEINE_TEST_ARGS ]] || fail "old glibc should fail without starting seine"
grep -q "needs 2.31" "$work/launch.out" || fail "old glibc message"
ok "mmp-launch: rejects a missing wallet and an old glibc with a clear message"

# --- mmp-stats.sh ---------------------------------------------------------------
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
token=test-token-123
# The miner stats fixture is shared with the HiveOS tests; a Rust test keeps
# it in the shape seine serializes.
cp "$repo/packaging/hiveos/testdata/miner-stats.json" "$work/body.json"
python3 -I "$repo/scripts/package_test_api.py" "$port" "$token" "$work/body.json" &
server_pid=$!
for _ in $(seq 50); do
	curl -s -o /dev/null "http://127.0.0.1:$port/" && break
	sleep 0.1
done

printf '%s\n' --address "$address" --api-server --api-bind "127.0.0.1:$port" \
	--api-cookie "$installed/control-api.cookie" >"$installed/seine.args"
cp "$data/gpu-info.json" "$root/run/gpu-info.json"
# The agent runs mmp-stats.sh DEVICE_COUNT LOG_FILE. mmpOS's Docker agent has
# no jq, so a broken jq on PATH shows the script uses the bundled one.
mkdir -p "$work/badjq"
printf '#!/bin/sh\nexit 1\n' >"$work/badjq/jq"
chmod +x "$work/badjq/jq"
run_stats() {
	(cd / && PATH="$work/badjq:/usr/bin:/bin" "$installed/mmp-stats.sh" 4 /var/log/miner.log) 2>"$work/stats.err"
}
expect_stats() {
	local label=$1 out=$2 filter=$3
	jq -e "$filter" <<<"$out" >/dev/null 2>&1 || fail "$label: unexpected stats $out ($(cat "$work/stats.err"))"
}

out=$(run_stats)
[[ $out == "Miner API connection failed" ]] || fail "no cookie: $out"
ok "mmp-stats: miner not running reports the API as down"

echo "$token" >"$installed/control-api.cookie"
full_filter='
	.busid == ["cpu", 1, 3, 10]
	and .hash == [200.12, 600.25, 150.12, 300]
	and .units == "hs"
	and .air == ["50", "0", "2"]
	and .miner_name == "seine"
	and .miner_version == "0.2.46"
	and (.busid | length) == (.hash | length)'
out=$(run_stats)
expect_stats "full" "$out" "$full_filter"
ok "mmp-stats: CPU first, GPUs by PCI bus, gpu-info.json for a GPU without one, shares"

rm "$root/run/gpu-info.json"
out=$(run_stats)
expect_stats "no gpu-info" "$out" '.busid == ["cpu", 1, null, 10] and .hash == [200.12, 600.25, 150.12, 300]'
ok "mmp-stats: works without gpu-info.json"

jq '.mining.devices |= map(select(.kind != "cpu"))' "$repo/packaging/hiveos/testdata/miner-stats.json" >"$work/body.json"
out=$(run_stats)
expect_stats "GPU only" "$out" '.busid == [1, null, 10] and (.hash | length) == 3'
jq '.mining = null | .lifecycle = "starting" | .running = false' "$repo/packaging/hiveos/testdata/miner-stats.json" >"$work/body.json"
out=$(run_stats)
expect_stats "starting" "$out" '.busid == [] and .hash == [] and .air == ["0", "0", "0"] and .miner_version == "0.2.46"'
ok "mmp-stats: GPU-only rig, and a starting miner reports no devices with its version"

echo wrong-token >"$installed/control-api.cookie"
out=$(run_stats)
[[ $out == "Miner API connection failed" ]] || fail "bad token: $out"
kill "$server_pid"
wait "$server_pid" 2>/dev/null || true
server_pid=""
echo "$token" >"$installed/control-api.cookie"
out=$(run_stats)
[[ $out == "Miner API connection failed" ]] || fail "API down: $out"
ok "mmp-stats: rejected token or stopped API reports the API as down"

if [[ -n ${MMPOS_NVRTC_ARCHIVE:-} ]]; then
	package=$("$pkg/build.sh" --archive "$work/seine-linux-x86_64.tar.gz" --version 0.2.46 \
		--out-dir "$work/out" --nvrtc-archive "$MMPOS_NVRTC_ARCHIVE" --jq "$jq_binary")
	rm -rf "$installed" && mkdir -p "$installed"
	tar -xzf "$package" -C "$installed"
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

echo "all $passed mmpOS package checks passed"
