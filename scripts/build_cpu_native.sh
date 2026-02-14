#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  scripts/build_cpu_native.sh [--profile <name>] [--cpu-only] [-- <extra cargo args>]

Examples:
  scripts/build_cpu_native.sh --cpu-only
  scripts/build_cpu_native.sh --profile release-native --cpu-only -- --locked

Notes:
  - Adds: RUSTFLAGS += "-C target-cpu=native"
  - Default profile: release-native
EOF
}

profile="release-native"
cpu_only=0
extra_args=()

while (($#)); do
    case "$1" in
        --profile)
            if (($# < 2)); then
                echo "error: --profile requires a value" >&2
                usage
                exit 1
            fi
            profile="$2"
            shift 2
            ;;
        --cpu-only)
            cpu_only=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            extra_args=("$@")
            break
            ;;
        *)
            echo "error: unknown argument: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if [[ -n "${RUSTFLAGS:-}" ]]; then
    export RUSTFLAGS="${RUSTFLAGS} -C target-cpu=native"
else
    export RUSTFLAGS="-C target-cpu=native"
fi

cmd=(cargo build --profile "$profile")
if ((cpu_only)); then
    cmd+=(--no-default-features)
fi
if ((${#extra_args[@]})); then
    cmd+=("${extra_args[@]}")
fi

echo "Building with profile=$profile cpu_only=$cpu_only"
echo "RUSTFLAGS=$RUSTFLAGS"
"${cmd[@]}"
