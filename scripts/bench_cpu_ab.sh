#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  scripts/bench_cpu_ab.sh \
    --baseline-dir <path> \
    --candidate-dir <path> \
    [--bench-kind kernel|backend|end-to-end] \
    [--threads <n>] \
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
    [--output-dir <path>] \
    [-- <extra miner args>]

Example:
  scripts/bench_cpu_ab.sh \
    --baseline-dir ../seine-baseline \
    --candidate-dir . \
    --bench-kind backend \
    --pairs 4 \
    --bench-secs 20 \
    --bench-rounds 3 \
    --bench-warmup-rounds 1 \
    --threads 1 \
    --cooldown-secs 20 \
    --profile release
EOF
}

baseline_dir=""
candidate_dir=""
bench_kind="backend"
threads=1
bench_secs=20
bench_rounds=3
bench_warmup_rounds=1
pairs=3
cooldown_secs=15
profile="release"
native=0
baseline_profile=""
candidate_profile=""
baseline_native=0
candidate_native=0
native_override=0
output_dir=""
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
        --threads)
            threads="${2:-}"
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
        --output-dir)
            output_dir="${2:-}"
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
if ! [[ "$threads" =~ ^[0-9]+$ ]] || ((threads < 1)); then
    echo "error: --threads must be an integer >= 1" >&2
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

if [[ -z "$output_dir" ]]; then
    stamp="$(date +%Y%m%d_%H%M%S)"
    output_dir="data/bench_cpu_ab_${bench_kind}_${stamp}"
fi
mkdir -p "$output_dir"
output_dir="$(cd "$output_dir" && pwd)"

raw_tsv="$output_dir/results.tsv"
summary_txt="$output_dir/summary.txt"
printf "variant\tpair\torder\tavg_hps\tmedian_hps\tcounted_hashes\tlate_hashes\treport\n" > "$raw_tsv"

extract_json_number() {
    local key="$1"
    local file="$2"
    local value
    value="$(tr -d '\n\r\t ' < "$file" | sed -n "s/.*\"${key}\":\\([-0-9.eE+]*\\).*/\\1/p")"
    if [[ -z "$value" ]]; then
        echo "error: key '${key}' not found in ${file}" >&2
        exit 1
    fi
    printf "%s" "$value"
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
        --backend cpu
        --threads "$threads"
        --disable-cpu-autotune-threads
        --bench-secs "$bench_secs"
        --bench-rounds "$bench_rounds"
        --bench-warmup-rounds "$bench_warmup_rounds"
        --ui plain
        --bench-output "$report_file"
    )
    if ((${#extra_args[@]})); then
        cmd+=("${extra_args[@]}")
    fi

    echo "[pair ${pair}/${pairs}] ${variant}:${order} | repo=${repo_dir} profile=${run_profile} native=${run_native}"
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

    local avg_hps
    local median_hps
    local counted_hashes
    local late_hashes
    avg_hps="$(extract_json_number "avg_hps" "$report_file")"
    median_hps="$(extract_json_number "median_hps" "$report_file")"
    counted_hashes="$(extract_json_number "total_counted_hashes" "$report_file")"
    late_hashes="$(extract_json_number "total_late_hashes" "$report_file")"
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "$variant" "$pair" "$order" "$avg_hps" "$median_hps" "$counted_hashes" "$late_hashes" "$report_file" \
        >> "$raw_tsv"

    echo "  avg_hps=${avg_hps} median_hps=${median_hps} counted=${counted_hashes} late=${late_hashes}"
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

{
    echo "bench_kind=$bench_kind"
    echo "threads=$threads"
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
    echo "baseline_dir=$baseline_dir"
    echo "candidate_dir=$candidate_dir"
    echo "baseline_avg_hps=$baseline_avg"
    echo "candidate_avg_hps=$candidate_avg"
    echo "delta_pct=$delta_pct"
    echo "results_tsv=$raw_tsv"
} > "$summary_txt"

cat "$summary_txt"
