#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  scripts/bench_nvidia_ab.sh \
    --baseline-dir <path> \
    --candidate-dir <path> \
    [--bench-kind kernel|kernel-effective|backend|end-to-end] \
    [--bench-secs <n>] \
    [--bench-rounds <n>] \
    [--bench-warmup-rounds <n>] \
    [--pairs <n>] \
    [--cooldown-secs <n>] \
    [--profile <cargo-profile>] \
    [--baseline-profile <cargo-profile>] \
    [--candidate-profile <cargo-profile>] \
    [--native] \
    [--baseline-native] \
    [--candidate-native] \
    [--nvidia-devices <csv>] \
    [--nvidia-max-rregcount <n>] \
    [--nvidia-hashes-per-launch-per-lane <n>] \
    [--nvidia-fused-target-check] \
    [--nvidia-no-adaptive-launch-depth] \
    [--output-dir <path>] \
    [-- <extra miner args>]

Example:
  scripts/bench_nvidia_ab.sh \
    --baseline-dir ../seine-baseline \
    --candidate-dir . \
    --bench-kind backend \
    --pairs 4 \
    --bench-secs 20 \
    --bench-rounds 3 \
    --bench-warmup-rounds 1 \
    --cooldown-secs 20 \
    --nvidia-devices 0 \
    --nvidia-max-rregcount 208 \
    --profile release
USAGE
}

baseline_dir=""
candidate_dir=""
bench_kind="backend"
bench_secs=20
bench_rounds=3
bench_warmup_rounds=1
pairs=3
cooldown_secs=20
profile="release"
baseline_profile=""
candidate_profile=""
native=0
baseline_native=0
candidate_native=0
native_override=0
output_dir=""

nvidia_devices="0"
nvidia_max_rregcount=""
nvidia_hashes_per_launch_per_lane=""
nvidia_fused_target_check=0
nvidia_no_adaptive_launch_depth=0

extra_args=()

while (($#)); do
    case "$1" in
        --baseline-dir)
            baseline_dir="${2:-}"
            shift 2
            ;;
        --candidate-dir)
            candidate_dir="${2:-}"
            shift 2
            ;;
        --bench-kind)
            bench_kind="${2:-}"
            shift 2
            ;;
        --bench-secs)
            bench_secs="${2:-}"
            shift 2
            ;;
        --bench-rounds)
            bench_rounds="${2:-}"
            shift 2
            ;;
        --bench-warmup-rounds)
            bench_warmup_rounds="${2:-}"
            shift 2
            ;;
        --pairs)
            pairs="${2:-}"
            shift 2
            ;;
        --cooldown-secs)
            cooldown_secs="${2:-}"
            shift 2
            ;;
        --profile)
            profile="${2:-}"
            shift 2
            ;;
        --baseline-profile)
            baseline_profile="${2:-}"
            shift 2
            ;;
        --candidate-profile)
            candidate_profile="${2:-}"
            shift 2
            ;;
        --native)
            native=1
            shift
            ;;
        --baseline-native)
            baseline_native=1
            native_override=1
            shift
            ;;
        --candidate-native)
            candidate_native=1
            native_override=1
            shift
            ;;
        --nvidia-devices)
            nvidia_devices="${2:-}"
            shift 2
            ;;
        --nvidia-max-rregcount)
            nvidia_max_rregcount="${2:-}"
            shift 2
            ;;
        --nvidia-hashes-per-launch-per-lane)
            nvidia_hashes_per_launch_per_lane="${2:-}"
            shift 2
            ;;
        --nvidia-fused-target-check)
            nvidia_fused_target_check=1
            shift
            ;;
        --nvidia-no-adaptive-launch-depth)
            nvidia_no_adaptive_launch_depth=1
            shift
            ;;
        --output-dir)
            output_dir="${2:-}"
            shift 2
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

if [[ -z "$baseline_dir" || -z "$candidate_dir" ]]; then
    echo "error: --baseline-dir and --candidate-dir are required" >&2
    usage
    exit 1
fi

if [[ -z "$baseline_profile" ]]; then
    baseline_profile="$profile"
fi
if [[ -z "$candidate_profile" ]]; then
    candidate_profile="$profile"
fi
if ((native_override == 0)); then
    baseline_native="$native"
    candidate_native="$native"
fi

if ! [[ "$pairs" =~ ^[0-9]+$ ]] || ((pairs < 1)); then
    echo "error: --pairs must be an integer >= 1" >&2
    exit 1
fi
if ! [[ "$bench_secs" =~ ^[0-9]+$ ]] || ((bench_secs < 1)); then
    echo "error: --bench-secs must be an integer >= 1" >&2
    exit 1
fi
if ! [[ "$bench_rounds" =~ ^[0-9]+$ ]] || ((bench_rounds < 1)); then
    echo "error: --bench-rounds must be an integer >= 1" >&2
    exit 1
fi
if ! [[ "$bench_warmup_rounds" =~ ^[0-9]+$ ]]; then
    echo "error: --bench-warmup-rounds must be an integer >= 0" >&2
    exit 1
fi
if ! [[ "$cooldown_secs" =~ ^[0-9]+$ ]]; then
    echo "error: --cooldown-secs must be an integer >= 0" >&2
    exit 1
fi
if [[ -n "$nvidia_hashes_per_launch_per_lane" ]]; then
    if ! [[ "$nvidia_hashes_per_launch_per_lane" =~ ^[0-9]+$ ]] || ((nvidia_hashes_per_launch_per_lane < 1)); then
        echo "error: --nvidia-hashes-per-launch-per-lane must be an integer >= 1" >&2
        exit 1
    fi
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "error: jq is required to parse benchmark reports" >&2
    exit 1
fi
if [[ -n "$nvidia_max_rregcount" ]]; then
    if ! [[ "$nvidia_max_rregcount" =~ ^[0-9]+$ ]] || ((nvidia_max_rregcount < 1)); then
        echo "error: --nvidia-max-rregcount must be an integer >= 1" >&2
        exit 1
    fi
fi

if [[ -z "$output_dir" ]]; then
    stamp="$(date +%Y%m%d_%H%M%S)"
    output_dir="data/bench_nvidia_ab_${bench_kind}_${stamp}"
fi
mkdir -p "$output_dir"
output_dir="$(cd "$output_dir" && pwd)"

raw_tsv="$output_dir/results.tsv"
summary_txt="$output_dir/summary.txt"
printf "variant\tpair\torder\tavg_hps\tmedian_hps\tcounted_hashes\tlate_hashes\tlate_hash_pct\tgpu_start\tgpu_end\treport\n" > "$raw_tsv"

gpu_query_id="${nvidia_devices%%,*}"

extract_json_number() {
    local key="$1"
    local file="$2"
    local value
    if ! value="$(jq -er --arg key "$key" \
        'if (.[$key] | type) == "number" then .[$key] else error("missing or non-numeric field: \($key)") end' \
        "$file")"; then
        echo "error: numeric key '${key}' not found in ${file}" >&2
        exit 1
    fi
    printf "%s" "$value"
}

gpu_snapshot() {
    local gpu_id="$1"
    if ! command -v nvidia-smi >/dev/null 2>&1; then
        printf "na"
        return
    fi
    local raw
    raw="$(nvidia-smi --id="$gpu_id" --query-gpu=temperature.gpu,clocks.sm,clocks.mem,power.draw,utilization.gpu --format=csv,noheader,nounits 2>/dev/null | head -n 1 || true)"
    if [[ -z "$raw" ]]; then
        printf "na"
        return
    fi
    raw="${raw//,/;}"
    raw="${raw// /}"
    printf "%s" "$raw"
}

run_single() {
    local variant="$1"
    local repo_dir="$2"
    local pair="$3"
    local order="$4"
    local run_profile="$5"
    local run_native="$6"
    local report_file="$output_dir/${variant}_pair${pair}_${order}.json"

    local cmd=(
        cargo run
        --profile "$run_profile"
        --
        --bench
        --bench-kind "$bench_kind"
        --backend nvidia
        --nvidia-devices "$nvidia_devices"
        --bench-secs "$bench_secs"
        --bench-rounds "$bench_rounds"
        --bench-warmup-rounds "$bench_warmup_rounds"
        --ui plain
        --bench-output "$report_file"
    )

    if [[ -n "$nvidia_max_rregcount" ]]; then
        cmd+=(--nvidia-max-rregcount "$nvidia_max_rregcount")
    fi
    if [[ -n "$nvidia_hashes_per_launch_per_lane" ]]; then
        cmd+=(--nvidia-hashes-per-launch-per-lane "$nvidia_hashes_per_launch_per_lane")
    fi
    if ((nvidia_fused_target_check)); then
        cmd+=(--nvidia-fused-target-check)
    fi
    if ((nvidia_no_adaptive_launch_depth)); then
        cmd+=(--nvidia-no-adaptive-launch-depth)
    fi
    if ((${#extra_args[@]})); then
        cmd+=("${extra_args[@]}")
    fi

    local gpu_start
    local gpu_end
    gpu_start="$(gpu_snapshot "$gpu_query_id")"

    echo "[pair ${pair}/${pairs}] ${variant}:${order} | repo=${repo_dir} profile=${run_profile} native=${run_native} devices=${nvidia_devices} rreg=${nvidia_max_rregcount:-auto} depth=${nvidia_hashes_per_launch_per_lane:-shipping-default}"
    if ((run_native)); then
        (
            cd "$repo_dir"
            if [[ -n "${RUSTFLAGS:-}" ]]; then
                RUSTFLAGS="${RUSTFLAGS} -C target-cpu=native" "${cmd[@]}"
            else
                RUSTFLAGS="-C target-cpu=native" "${cmd[@]}"
            fi
        )
    else
        (
            cd "$repo_dir"
            "${cmd[@]}"
        )
    fi

    gpu_end="$(gpu_snapshot "$gpu_query_id")"

    local avg_hps
    local median_hps
    local counted_hashes
    local late_hashes
    local late_hash_pct
    avg_hps="$(extract_json_number "avg_hps" "$report_file")"
    median_hps="$(extract_json_number "median_hps" "$report_file")"
    counted_hashes="$(extract_json_number "total_counted_hashes" "$report_file")"
    late_hashes="$(extract_json_number "total_late_hashes" "$report_file")"
    late_hash_pct="$(extract_json_number "late_hash_pct" "$report_file")"

    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "$variant" "$pair" "$order" "$avg_hps" "$median_hps" "$counted_hashes" "$late_hashes" "$late_hash_pct" "$gpu_start" "$gpu_end" "$report_file" \
        >> "$raw_tsv"

    echo "  avg_hps=${avg_hps} median_hps=${median_hps} counted=${counted_hashes} late=${late_hashes} late_pct=${late_hash_pct} gpu_start=${gpu_start} gpu_end=${gpu_end}"
}

total_runs=$((pairs * 2))
run_idx=0
for ((pair = 1; pair <= pairs; pair++)); do
    if ((pair % 2 == 1)); then
        first_variant="baseline"
        first_repo="$baseline_dir"
        first_profile="$baseline_profile"
        first_native="$baseline_native"
        second_variant="candidate"
        second_repo="$candidate_dir"
        second_profile="$candidate_profile"
        second_native="$candidate_native"
    else
        first_variant="candidate"
        first_repo="$candidate_dir"
        first_profile="$candidate_profile"
        first_native="$candidate_native"
        second_variant="baseline"
        second_repo="$baseline_dir"
        second_profile="$baseline_profile"
        second_native="$baseline_native"
    fi

    run_single "$first_variant" "$first_repo" "$pair" "first" "$first_profile" "$first_native"
    run_idx=$((run_idx + 1))
    if ((cooldown_secs > 0 && run_idx < total_runs)); then
        echo "  cooldown ${cooldown_secs}s"
        sleep "$cooldown_secs"
    fi

    run_single "$second_variant" "$second_repo" "$pair" "second" "$second_profile" "$second_native"
    run_idx=$((run_idx + 1))
    if ((cooldown_secs > 0 && run_idx < total_runs)); then
        echo "  cooldown ${cooldown_secs}s"
        sleep "$cooldown_secs"
    fi
done

baseline_avg="$(awk -F '\t' 'NR>1 && $1=="baseline" {sum+=$4; n+=1} END {if (n>0) printf "%.12f", sum/n; else print "nan"}' "$raw_tsv")"
candidate_avg="$(awk -F '\t' 'NR>1 && $1=="candidate" {sum+=$4; n+=1} END {if (n>0) printf "%.12f", sum/n; else print "nan"}' "$raw_tsv")"
delta_pct="$(awk -v b="$baseline_avg" -v c="$candidate_avg" 'BEGIN { if (b == 0 || b == "nan" || c == "nan") print "nan"; else printf "%.4f", ((c-b)/b)*100.0 }')"

baseline_late_pct="$(awk -F '\t' 'NR>1 && $1=="baseline" {sum+=$8; n+=1} END {if (n>0) printf "%.6f", sum/n; else print "nan"}' "$raw_tsv")"
candidate_late_pct="$(awk -F '\t' 'NR>1 && $1=="candidate" {sum+=$8; n+=1} END {if (n>0) printf "%.6f", sum/n; else print "nan"}' "$raw_tsv")"

{
    echo "bench_kind=$bench_kind"
    echo "bench_secs=$bench_secs"
    echo "bench_rounds=$bench_rounds"
    echo "bench_warmup_rounds=$bench_warmup_rounds"
    echo "pairs=$pairs"
    echo "cooldown_secs=$cooldown_secs"
    echo "default_profile=$profile"
    echo "default_native=$native"
    echo "baseline_profile=$baseline_profile"
    echo "candidate_profile=$candidate_profile"
    echo "baseline_native=$baseline_native"
    echo "candidate_native=$candidate_native"
    echo "nvidia_devices=$nvidia_devices"
    echo "nvidia_max_rregcount=${nvidia_max_rregcount:-auto}"
    echo "nvidia_hashes_per_launch_per_lane=${nvidia_hashes_per_launch_per_lane:-shipping-default}"
    echo "nvidia_fused_target_check=$nvidia_fused_target_check"
    echo "nvidia_no_adaptive_launch_depth=$nvidia_no_adaptive_launch_depth"
    echo "baseline_dir=$baseline_dir"
    echo "candidate_dir=$candidate_dir"
    echo "baseline_avg_hps=$baseline_avg"
    echo "candidate_avg_hps=$candidate_avg"
    echo "delta_pct=$delta_pct"
    echo "baseline_avg_late_hash_pct=$baseline_late_pct"
    echo "candidate_avg_late_hash_pct=$candidate_late_pct"
    echo "results_tsv=$raw_tsv"
} > "$summary_txt"

cat "$summary_txt"
