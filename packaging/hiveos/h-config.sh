#!/usr/bin/env bash
# shellcheck disable=SC2154
# HiveOS sources this file (after h-manifest.conf) from /hive/miners/custom with
# the flight sheet loaded: CUSTOM_TEMPLATE, CUSTOM_URL, CUSTOM_PASS,
# CUSTOM_USER_CONFIG, WORKER_NAME. It writes the seine command line to
# $CUSTOM_CONFIG_FILENAME, one argument per line, for h-run.sh.

# Flags this script sets unless "Extra config arguments" already sets them.
# Seine rejects most flags given twice, so the user's value always wins.
seine_hiveos_has_flag() {
	local arg flag
	for arg in "${seine_hiveos_user_args[@]}"; do
		for flag in "$@"; do
			[[ $arg == "$flag" || $arg == "$flag="* ]] && return 0
		done
	done
	return 1
}

seine_hiveos_detect_gpus() {
	local vendor=$1 count=0
	if command -v gpu-detect >/dev/null 2>&1; then
		count=$(gpu-detect "$vendor" 2>/dev/null)
	fi
	[[ $count =~ ^[0-9]+$ ]] || count=0
	echo "$count"
}

seine_hiveos_config() {
	local template=${CUSTOM_TEMPLATE:-} urls=${CUSTOM_URL:-}
	local extra=${CUSTOM_USER_CONFIG:-} worker=${WORKER_NAME:-}
	local -a args=() pools=()
	local parsed nvidia amd backends=""

	# Flight sheets edited on Windows can carry CRLF line endings.
	template=${template//$'\r'/}
	urls=${urls//$'\r'/}
	extra=${extra//$'\r'/}
	worker=${worker//$'\r'/}
	# HiveOS normally expands %WAL% and %WORKER_NAME% before this runs.
	template=${template//%WORKER_NAME%/$worker}
	extra=${extra//%WORKER_NAME%/$worker}
	template=${template#"${template%%[![:space:]]*}"}
	template=${template%"${template##*[![:space:]]}"}

	seine_hiveos_user_args=()
	if [[ -n ${extra//[[:space:]]/} ]]; then
		# xargs splits on whitespace and honours quotes without running anything.
		if ! parsed=$(printf '%s\n' "$extra" | xargs -n 1 printf '%s\n'); then
			echo "seine: could not parse Extra config arguments (check the quotes): $extra" >&2
			return 1
		fi
		mapfile -t seine_hiveos_user_args <<<"$parsed"
	fi

	if ! seine_hiveos_has_flag --address -u --user; then
		if [[ -z $template || $template == *%WAL%* ]]; then
			echo "seine: set \"Wallet and worker template\" in the flight sheet to your Blocknet address, e.g. %WAL%.%WORKER_NAME%" >&2
			return 1
		fi
		# Seine reads a shortname login (@name, $name, also after solo:) whole,
		# dots included, so split off the rig name that %WAL%.%WORKER_NAME% appends.
		local bare=$template
		[[ ${bare:0:5} == [Ss][Oo][Ll][Oo]: ]] && bare=${bare:5}
		if [[ $bare == [@\$]* && -n $worker && $template == *."$worker" ]]; then
			template=${template%."$worker"}
		fi
		args+=(--address "$template")
		# ADDRESS.WORKER already names the worker; otherwise use the rig name.
		if [[ ($template != *.* || $bare == [@\$]*) && -n $worker ]] &&
			! seine_hiveos_has_flag --pool-worker --worker --rig-id; then
			args+=(--pool-worker "$worker")
		fi
	fi

	if ! seine_hiveos_has_flag --pool-url -o --url --mode; then
		# One URL per line or space-separated; seine tries them in order.
		read -r -d '' -a pools <<<"$urls"
		[[ ${#pools[@]} -eq 0 ]] && pools=("$SEINE_DEFAULT_POOL")
		args+=(--pool-url "$(
			IFS=,
			echo "${pools[*]}"
		)")
	fi

	# GPU rigs often have little system RAM and every CPU worker needs 2 GiB,
	# so mine on the GPUs only unless the user lists backends. A rig without
	# GPUs mines on the CPU, sized to the free RAM. With AMD cards, seine picks
	# the GPUs itself so integrated GPUs and its AMD start checks still apply.
	if ! seine_hiveos_has_flag --backend; then
		nvidia=$(seine_hiveos_detect_gpus NVIDIA)
		amd=$(seine_hiveos_detect_gpus AMD)
		[[ $nvidia -gt 0 ]] && backends=nvidia
		if [[ $amd -gt 0 ]]; then
			args+=(--gpu-only)
		else
			args+=(--backend "${backends:-cpu}")
		fi
	fi

	seine_hiveos_has_flag --data-dir || args+=(--data-dir "$SEINE_DATA_DIR")
	seine_hiveos_has_flag --ui || args+=(--ui plain)
	# HiveOS installs miner versions through the package; don't replace the binary.
	seine_hiveos_has_flag --auto-update --no-auto-update || args+=(--no-auto-update)
	seine_hiveos_has_flag --api-server --service || args+=(--api-server)
	seine_hiveos_has_flag --api-bind || args+=(--api-bind "127.0.0.1:$SEINE_API_PORT")
	seine_hiveos_has_flag --api-cookie --api-token || args+=(--api-cookie "$SEINE_API_COOKIE")

	args+=("${seine_hiveos_user_args[@]}")
	printf '%s\n' "${args[@]}" >"$CUSTOM_CONFIG_FILENAME" || return 1
	echo "seine arguments written to $CUSTOM_CONFIG_FILENAME"
}

seine_hiveos_config
