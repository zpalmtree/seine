#!/usr/bin/env python3
"""Read-only, aggregate telemetry briefing for agents. No third-party packages."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

REMOTE_SQL = (
    'docker compose -f /opt/seine/telemetry/deploy/compose.yaml '
    '--env-file /opt/seine/telemetry/deploy/.env exec -T db '
    'psql -X -U telemetry -d telemetry -qAt -v ON_ERROR_STOP=1'
)


def remote(host, command, data=None):
    return subprocess.run(
        ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', '--', host, command],
        input=data, text=True, capture_output=True, timeout=60,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='root@2.31.36.180', help='Collector SSH destination')
    args = parser.parse_args()
    if args.host.startswith('-'):
        parser.error('host must be an SSH destination, not an option')
    briefing = {
        'checked_at': datetime.now(ZoneInfo('America/New_York')).strftime('%Y-%m-%d %H:%M:%S %Z'),
        'timezone': 'America/New_York',
        'scope': 'Aggregates from release builds in the last seven days; installations are not people.',
        'performance_screen': {
            'minimum_installations': 5,
            'minimum_reports_per_installation': 3,
            'minimum_span_hours_per_installation': 24,
            'interpretation': 'Screening thresholds, not statistical confidence or benchmark proof.',
        },
    }
    failed = False
    for key, command, data in (
        ('health', '/opt/seine/telemetry/deploy/check.sh --infrastructure-only', None),
        ('data_quality', REMOTE_SQL, Path(__file__).with_name('data_quality.sql').read_text()),
        ('telemetry', REMOTE_SQL, Path(__file__).with_suffix('.sql').read_text()),
        ('diagnostics', REMOTE_SQL, Path(__file__).with_name('diagnostics.sql').read_text()),
        ('tuning', REMOTE_SQL, Path(__file__).with_name('tuning_report.sql').read_text()),
        ('release_health', REMOTE_SQL, Path(__file__).with_name('release_health.sql').read_text()),
    ):
        try:
            result = remote(args.host, command, data)
            if result.returncode:
                briefing[key] = {'status': 'error', 'detail': (result.stderr or result.stdout).strip()[:1000]}
                failed = True
            elif key in ('telemetry', 'data_quality', 'diagnostics', 'tuning', 'release_health'):
                briefing[key] = json.loads(result.stdout)
            else:
                briefing[key] = {'status': 'ok', 'detail': result.stdout.strip()}
        except (OSError, subprocess.TimeoutExpired, json.JSONDecodeError) as error:
            briefing[key] = {'status': 'error', 'detail': str(error)[:1000]}
            failed = True
    data = briefing.get('telemetry', {})
    if data.get('status') != 'error':
        briefing['assessment'] = (
            'No qualifying performance cohorts yet. Use inventory only; collect more observations before comparing rates.'
            if not data['performance_candidates'] else
            'Candidate cohorts are available. Prioritize common hardware, then verify any proposed change with controlled paired benchmarks.'
        )
    if failed:
        briefing['assessment'] = 'Resolve the reported health/query failure before relying on this briefing.'
    quality = briefing.get('data_quality', {})
    if not failed and quality.get('status') == 'error':
        failed = True
        briefing['assessment'] = (
            'Hardware inventory is arriving without performance samples from at least five '
            'installations of one version, each reporting at least three times over two hours. '
            'Investigate the miner sampling path and pool connectivity using inventory_only_clusters; '
            'missing samples alone do not prove a sampling bug.'
        )
    diagnostics = briefing.get('diagnostics', {})
    briefing['diagnostic_interpretation'] = (
        'Event detail contains typed causes captured at the failing operation; absent detail means '
        'legacy or uninstrumented code, not an unknown error message. OS error numbers are platform-specific. '
        'Requested bytes describe one failed allocation. Available memory is a report-time snapshot, '
        'not proof of OOM and excludes reserved HugeTLB pages. Unclean clusters show the last observed '
        'phase of the previous run, not a crash cause; checkpoints can lag or be absent. '
        'Hashing in a restart report establishes work after restart, not recovery from every issue. '
        'Several events can describe one failure.'
        ' Unclean versions identify the interrupted release using its checkpoint version or an '
        'unambiguous observed build mapping; unknown stays unknown. reporting_versions identify '
        'the restarting clients. Previous-run event clusters are checkpoint context, may duplicate '
        'earlier uploads, and do not establish a termination cause. Hardware in those clusters is '
        'observed after restart. Dependency source IDs come from a fixed allowlist.'
        ' shutdown_request records an observed OS, API or TUI stop request; it does not prove cleanup '
        'completed or establish why a process terminated. Null includes older clients.'
        ' last_cleanup_stage is the last entered step, not a completed step. Shutdown clusters '
        'include live checkpoints and final reports; they are not clean-exit counts. '
        'driver_cuda_version describes CUDA driver compatibility, not the display-driver release. '
        'nvrtc_version describes the loaded compiler. Both use major*1000 + minor*10. '
        'Previous NVIDIA versions belong to the interrupted run, not the restarting process.'
    )
    if diagnostics.get('status') == 'attention':
        failed = True
        briefing['diagnostic_assessment'] = (
            'At least three installations of one version reported errors, quarantines or '
            'unclean previous runs within 24 hours. Investigate issue clusters by build, '
            'source location and hardware. An unclean run is not proof of a crash.'
        )
    elif diagnostics.get('status') == 'unavailable':
        briefing['diagnostic_assessment'] = 'No diagnostic coverage yet; missing failure reports do not establish healthy miners.'
    elif diagnostics.get('status') == 'ok':
        briefing['diagnostic_assessment'] = 'Review issue clusters, including single-installation failures. No fleet threshold is exceeded.'
    tuning = briefing.get('tuning', {})
    if tuning.get('status') == 'error':
        briefing['tuning_assessment'] = 'Paired-trial query failed; do not interpret missing hints as no gain.'
    elif tuning.get('ranked_hints'):
        supported = sum(h.get('confidence') == 'observational_support' for h in tuning['ranked_hints'])
        briefing['tuning_assessment'] = (
            f'{supported} ranked CPU-kernel cohorts have observational support; other hints have mixed or limited evidence. '
            'All require matching native validation before shipping; confidence labels are not probabilities or '
            'independent-host proof. No automatic defaults or remote overrides.'
        )
    elif tuning.get('unique_trials', 0):
        briefing['tuning_assessment'] = (
            'Paired trials are arriving, but no hints meet coverage. '
            f"{tuning.get('coverage_only_trials', 0)} trials are coverage-only; "
            f"{tuning.get('missing_context_trials', 0)} lack required initial context. "
            'Worker-count/joint policy analysis remains staged. Unknown context and costs are not zero; '
            'no-confirmed-gain is not a regression.'
        )
    else:
        briefing['tuning_assessment'] = (
            'No paired trials in this window; verify producer release adoption and collector compatibility. '
            'Missing trials do not establish no gain.'
        )
    release = briefing.get('release_health', {})
    briefing['release_health_interpretation'] = (
        'Schema-14 miners only; coverage shows how many installations per version can report these fields. '
        'Update, pool, fork and AMD counts are summed over report windows; auto_install, pool settings, '
        'AMD detection and GPU kernel sources use each installation\'s latest report. '
        'install_failure_stages records only the last failed stage per report window. '
        'amd_gpu_not_mining counts installations whose OS lists an AMD display device (pci_devices > 0, '
        'Linux only) while HIP is unavailable or no AMD GPU was selected, usually missing ROCm; '
        'hip_unavailable alone is normal on machines without AMD GPUs. nvrtc_failed_prebuilt_ptx means '
        'NVRTC was installed but failed. Pool counts are booleans and counts, never hosts or memos.'
    )
    if release.get('status') == 'error':
        briefing['release_health_assessment'] = 'Release-health query failed; missing schema-14 aggregates do not establish healthy updates.'
    elif release.get('status') == 'attention':
        failed = True
        briefing['release_health_assessment'] = (
            'At least three installations of one version reported failed self-update installs, or mining '
            'paused on work this build cannot mine, within 24 hours. Check install_failure_stages and '
            'fork_readiness by version; paused miners need an update.'
        )
    elif release.get('status') == 'unavailable':
        briefing['release_health_assessment'] = 'No schema-14 reports yet; update, pool and AMD detection health are unknown, not healthy.'
    elif release.get('status') == 'ok':
        briefing['release_health_assessment'] = (
            'Review install failures, fork readiness, amd_gpu_not_mining and NVRTC fallbacks below the fleet threshold.'
        )
    print(json.dumps(briefing, indent=2))
    return int(failed)


if __name__ == '__main__':
    sys.exit(main())
