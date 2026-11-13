#!/usr/bin/env bash
# Check HTTPS, database, retention, derived rows, disk and backup freshness without reporting IDs.
set -euo pipefail
deploy_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
compose=(docker compose --project-directory "$deploy_dir" --env-file "$deploy_dir/.env" -f "$deploy_dir/compose.yaml")
# The generated deployment file is root-owned and contains shell-safe values.
source "$deploy_dir/.env"
code="$(curl --silent --show-error --max-time 10 --resolve "$TELEMETRY_HOST:443:127.0.0.1" -o /dev/null -w '%{http_code}' "https://$TELEMETRY_HOST/healthz")"
[[ "$code" == 204 ]] || { echo "HTTPS health check failed: HTTP $code" >&2; exit 1; }
expired="$("${compose[@]}" exec -T db psql -U telemetry -d telemetry -v ON_ERROR_STOP=1 -Atc "SELECT count(*) FROM telemetry_reports WHERE received_at < now() - interval '30 days 2 hours'")"
[[ "$expired" == 0 ]] || { echo 'Telemetry retention cleanup is overdue.' >&2; exit 1; }
# Scheduled checks read one derived row per report (src/schema.sql).
underived="$("${compose[@]}" exec -T db psql -U telemetry -d telemetry -v ON_ERROR_STOP=1 -Atc "SELECT count(*) FROM telemetry_reports r WHERE r.received_at >= now() - interval '1 hour' AND NOT EXISTS (SELECT 1 FROM telemetry_report_facts f WHERE f.report_id = r.report_id)")"
[[ "$underived" == 0 ]] || { echo 'Recent reports are missing derived rows; run telemetry/rebuild_facts.sql.' >&2; exit 1; }
python3 - <<'PY'
import os
from pathlib import Path
import shutil
import time

for path in ('/', '/var/lib/docker', '/var/backups/seine-telemetry'):
    usage = shutil.disk_usage(path)
    if usage.free < max(5 * 1024**3, usage.total * .15):
        raise SystemExit(f'Low free disk space on {path}')
backups = Path(os.environ.get('TELEMETRY_BACKUP_DIR', '/var/backups/seine-telemetry'))
reports = list(backups.glob('telemetry-*/reports.csv.gz'))
if not reports or time.time() - max(p.stat().st_mtime for p in reports) > 27 * 3600:
    raise SystemExit('Telemetry recovery backup is missing or older than 27 hours.')
if any(time.time() - p.stat().st_mtime > 8 * 86400 for p in reports):
    raise SystemExit('Telemetry recovery backup expiry is overdue.')
PY
if [[ "${1:-}" == --infrastructure-only ]]; then
    echo 'Telemetry HTTPS, database, retention, disk and backups are healthy.'
    exit 0
fi
quality="$("${compose[@]}" exec -T db psql -X -U telemetry -d telemetry -qAt -v ON_ERROR_STOP=1 < "$deploy_dir/../data_quality.sql")"
diagnostics="$("${compose[@]}" exec -T db psql -X -U telemetry -d telemetry -qAt -v ON_ERROR_STOP=1 < "$deploy_dir/../diagnostic_alerts.sql")"
python3 - "$quality" "$diagnostics" <<'PYTHON'
import json, sys
quality, diagnostics = map(json.loads, sys.argv[1:])
failures = []
if quality['status'] != 'ok':
    failures.append('Telemetry data quality failed: sustained hardware-only reports without backend samples.')
if diagnostics['status'] == 'attention':
    failures.append('Miner reliability needs attention: errors or unclean runs above the per-version share on the newest release line, or failed installs or paused mining on at least three installations of one version, in 24 hours. Run telemetry/agent_report.py.')
if failures:
    sys.exit('\n'.join(failures))
PYTHON
echo 'Telemetry HTTPS, database, retention, disk, backups, sample completeness and miner reliability thresholds are healthy.'
