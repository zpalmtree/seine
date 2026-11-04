#!/usr/bin/env bash
# mmpOS runs the miner profile's command line from this directory inside its
# miner screen session. The default command line for a custom miner is:
#
#   ./mmp-launch.sh --coin %coin% --pool %pool_server%:%pool_port% \
#     --user %user% --password %password% --api-port %api_port%
#
# A %pool_protocol% placeholder (tcp or tls) may also appear on its own.
# Anything else goes to seine as is and wins over the package defaults. This
# script writes the seine command line to seine.args, one argument per line,
# and runs seine.

cd "$(dirname "$0")" || exit 1
# shellcheck source=mmp-external.conf
. ./mmp-external.conf

seine_mmpos_extra=()

# Flags this script sets unless the user's own arguments already set them.
# Seine rejects most flags given twice, so the user's value always wins.
seine_mmpos_has_flag() {
	local arg flag
	for arg in "${seine_mmpos_extra[@]}"; do
		for flag in "$@"; do
			[[ $arg == "$flag" || $arg == "$flag="* ]] && return 0
		done
	done
	return 1
}

seine_mmpos_jq() {
	if [[ -x $SEINE_HOME/bin/jq ]]; then
		"$SEINE_HOME/bin/jq" "$@"
	else
		jq "$@"
	fi
}

# Counts the rig's NVIDIA or AMD GPUs from mmpOS's gpu-info.json, or from the
# device nodes when that file is missing.
seine_mmpos_gpu_count() {
	local vendor=$1 root=${SEINE_MMPOS_ROOT:-} count=0 dev
	if [[ -r $SEINE_GPU_INFO ]]; then
		# shellcheck disable=SC2016
		count=$(seine_mmpos_jq -r --arg key "$vendor" \
			'.device.GPU[$key].busid // [] | length' "$SEINE_GPU_INFO" 2>/dev/null)
	elif [[ $vendor == nvidia_details ]]; then
		for dev in "$root"/dev/nvidia[0-9]*; do
			[[ -e $dev ]] && count=$((count + 1))
		done
	elif [[ -e $root/dev/kfd ]]; then
		for dev in "$root"/sys/bus/pci/devices/*; do
			[[ $(cat "$dev/vendor" 2>/dev/null) == 0x1002 &&
				$(cat "$dev/class" 2>/dev/null) == 0x03* ]] && count=$((count + 1))
		done
	fi
	[[ $count =~ ^[0-9]+$ ]] || count=0
	echo "$count"
}

# An mmpOS field the rig left empty can arrive as the placeholder itself.
seine_mmpos_value() {
	local value=${1//$'\r'/}
	value=${value#"${value%%[![:space:]]*}"}
	value=${value%"${value##*[![:space:]]}"}
	[[ $value == *%*%* ]] && value=""
	printf '%s' "$value"
}

seine_mmpos_launch() {
	local pool="" protocol="" user="" api_port="" worker="" glibc
	local nvidia amd backends=""
	local -a args=()

	while [[ $# -gt 0 ]]; do
		case $1 in
		--coin | --password | --pool | --user | --api-port)
			local flag=$1 value=""
			shift
			# A field mmpOS left empty can leave the flag without a value.
			if [[ $# -gt 0 && $1 != --* ]]; then
				value=$(seine_mmpos_value "$1")
				shift
			fi
			case $flag in
			--pool) pool=$value ;;
			--user) user=$value ;;
			--api-port) api_port=$value ;;
			esac
			;;
		tcp | tls | ssl | tcp:// | tls:// | ssl:// | stratum+tcp:// | stratum+ssl://)
			protocol=${1%://}
			protocol=${protocol#stratum+}
			shift
			;;
		*) seine_mmpos_extra+=("${1//$'\r'/}"); shift ;;
		esac
	done

	# The Linux release needs glibc 2.31 (Ubuntu 20.04) or newer.
	glibc=${SEINE_MMPOS_GLIBC:-$(getconf GNU_LIBC_VERSION 2>/dev/null)}
	glibc=${glibc##* }
	if [[ $glibc =~ ^[0-9]+\.[0-9]+ ]] &&
		[[ $(printf '%s\n' "$glibc" 2.31 | sort -V | head -n 1) != 2.31 ]]; then
		echo "seine: this rig has glibc $glibc; seine needs 2.31 or newer. Update mmpOS to a current image." >&2
		return 1
	fi

	if ! seine_mmpos_has_flag --address -u --user; then
		if [[ -z $user || $user == .* ]]; then
			echo "seine: no wallet. Select a pool and wallet with your Blocknet address in the miner profile." >&2
			return 1
		fi
		# %user% is %wallet_address%.%rig_name%%miner_id%. Seine splits a plain
		# address at the first dot itself, but reads a shortname (@name, also
		# after solo:) whole, dots included, so split the rig name off here.
		local bare=$user
		[[ ${bare:0:5} == [Ss][Oo][Ll][Oo]: ]] && bare=${bare:5}
		if [[ $bare == [@\$]*.* ]]; then
			worker=${user##*.}
			user=${user%.*}
		elif [[ $user != *.* ]]; then
			worker=${HOSTNAME:-$(uname -n)}
		fi
		args+=(--address "$user")
		if [[ -n $worker ]] && ! seine_mmpos_has_flag --pool-worker --worker --rig-id; then
			args+=(--pool-worker "$worker")
		fi
	fi

	if ! seine_mmpos_has_flag --pool-url -o --url --mode; then
		if [[ -z $pool || $pool == :* ]]; then
			pool=$SEINE_DEFAULT_POOL
		else
			# %pool_protocol%%pool_server%:%pool_port% gives the scheme inline.
			case $pool in
			ssl://* | tls://*) protocol=ssl pool=${pool#*://} ;;
			tcp://*) protocol=tcp pool=${pool#*://} ;;
			esac
			if [[ $pool != *://* ]]; then
				case $protocol in
				tls | ssl) pool="stratum+ssl://$pool" ;;
				*) pool="stratum+tcp://$pool" ;;
				esac
			fi
		fi
		args+=(--pool-url "$pool")
	fi

	# GPU rigs often have little system RAM and every CPU worker needs 2 GiB,
	# so mine on the GPUs only unless the user lists backends. A rig without
	# GPUs mines on the CPU, sized to the free RAM. With AMD cards, seine picks
	# the GPUs itself so integrated GPUs and its AMD start checks still apply.
	if ! seine_mmpos_has_flag --backend; then
		nvidia=$(seine_mmpos_gpu_count nvidia_details)
		amd=$(seine_mmpos_gpu_count amd_sysfs_details)
		[[ $nvidia -gt 0 ]] && backends=nvidia
		if [[ $amd -gt 0 ]]; then
			args+=(--gpu-only)
		else
			args+=(--backend "${backends:-cpu}")
		fi
	fi

	[[ $api_port =~ ^[0-9]+$ && $api_port -gt 0 && $api_port -lt 65536 ]] || api_port=$SEINE_API_PORT
	seine_mmpos_has_flag --data-dir || args+=(--data-dir "$SEINE_DATA_DIR")
	# mmpOS runs the miner in screen, a terminal, so ask for plain log lines.
	seine_mmpos_has_flag --ui || args+=(--ui plain)
	# mmpOS installs miner versions through the package; don't replace the binary.
	seine_mmpos_has_flag --auto-update --no-auto-update || args+=(--no-auto-update)
	seine_mmpos_has_flag --api-server --service || args+=(--api-server)
	seine_mmpos_has_flag --api-bind || args+=(--api-bind "127.0.0.1:$api_port")
	seine_mmpos_has_flag --api-cookie --api-token || args+=(--api-cookie "$SEINE_API_COOKIE")

	args+=("${seine_mmpos_extra[@]}")
	printf '%s\n' "${args[@]}" >"$SEINE_ARGS_FILE" || return 1

	seine_mmpos_has_flag --data-dir || mkdir -p "$SEINE_DATA_DIR"
	# The package ships the CUDA NVRTC libraries that NVIDIA mining needs.
	export LD_LIBRARY_PATH="$SEINE_HOME/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
	# NVIDIA GPU indexes follow PCI bus order, matching nvidia-smi and mmpOS.
	export CUDA_DEVICE_ORDER=PCI_BUS_ID

	echo "seine ${args[*]}"
	# mmpOS's screen session is a terminal, where seine would colour and
	# shorten its log lines; screen logs what cat passes on instead. cat
	# ignores Ctrl+C so seine can still log while it shuts down.
	./seine "${args[@]}" 2>&1 | (
		trap '' INT
		exec cat
	)
	return "${PIPESTATUS[0]}"
}

seine_mmpos_launch "$@"
