#!/usr/bin/env python3
"""Bounded, stateful Discord alerts from aggregate telemetry checks (stdlib only)."""
import argparse
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
REPEAT_SECONDS = 4 * 3600
# Fleet findings remain visible in reports; only collector outages need nudges.
REMINDER_CATEGORIES = {'infrastructure', 'ingestion', 'monitor'}
CONFIRM_SECONDS = 240
MAX_SENDS = 10
# Installations with pool connection warnings in one hour, versus the prior day.
POOL_SPIKE_MIN_INSTALLATIONS = 20
POOL_SPIKE_FACTOR = 3


def identity(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()[:24]


def label(value):
    # Miner-controlled labels cannot inject mentions, markdown, or links.
    return ''.join(c if c.isalnum() or c in ' ._()-' else '_' for c in str(value))[:100]


def issue(category, key, summary, **presentation):
    return {'category': category, 'key': category + ':' + identity(key), 'summary': summary,
            **presentation}


def evaluate(results):
    """Unknown queries never resolve their previous incidents."""
    active, known = [], {'monitor'}
    for category, result in results.items():
        if result is None:
            active.append(issue('monitor', category, f'{category} check could not complete. Inspect the telemetry alert service journal.'))
        else:
            known.add(category)
    if results.get('infrastructure') is False:
        active.append(issue('infrastructure', 'health', 'Collector HTTPS, database, retention, disk or backup check failed. Run telemetry/deploy/check.sh --infrastructure-only.'))
    quality = results.get('quality')
    if quality is not None:
        for row in quality['versions']:
            count = row['sustained_inventory_only_installations']
            if count >= 5:
                # These shipped versions predate the pool sampling fix in v0.2.26.
                legacy = row['version'] in ('0.2.24', 'v0.2.24', '0.2.25', 'v0.2.25')
                action = ('Known pool-reporting bug in this release. Upgrade to v0.2.26 or later.'
                          if legacy else 'Inspect miner activity and the performance sampling path.')
                active.append(issue(
                    'quality', row['version'],
                    'Installations repeatedly send hardware inventory without hashrate samples. '
                    'Missing samples do not establish that mining stopped.',
                    version=label(row['version']), affected=count, action=action,
                    severity='info' if legacy else 'warning'))
    diagnostics = results.get('diagnostics')
    if diagnostics is not None:
        for field, title in [('recent_error_versions', 'miner errors/quarantines'), ('recent_unclean_versions', 'unclean previous runs (not proof of crashes)')]:
            for row in diagnostics[field]:
                count = row['affected_installations']
                if count >= 3:
                    version = label(row['version'])
                    attribution = ''
                    if field == 'recent_unclean_versions':
                        version = ('Unknown previous version' if row['version'] == 'unknown'
                                   else f'Previous run {version}')
                        attribution = ' The restarting version is not assumed to be the interrupted version.'
                    active.append(issue('diagnostics', [field, row['version']], f'{version}: {count} installations with {title} in 24 hours.{attribution} Run telemetry/agent_report.py for issue clusters.'))
        pool = diagnostics.get('pool_connection_warnings')
        if pool is not None:
            known.add('pool')
            recent = pool['last_hour_installations']
            baseline = float(pool['baseline_hourly_installations'] or 0)
            if recent >= max(POOL_SPIKE_MIN_INSTALLATIONS, POOL_SPIKE_FACTOR * baseline):
                active.append(issue('pool', 'connection_spike', f'{recent} installations reported pool connection failures in the last hour (prior-day average {baseline:.0f}/hour); {pool["last_hour_limit_installations"]} were refused at the pool connection limit. Check bntpool Stratum, the gateway and DNS.'))
    performance = results.get('performance')
    if performance is not None:
        known.add('ingestion')
        minutes = performance['minutes_since_last_report']
        if minutes is None or minutes >= 120:
            active.append(issue('ingestion', 'stale', 'No periodic release reports in the last two hours. Check ingest delivery and miner activity.'))
        for row in performance['performance_cohorts']:
            if row['median_ratio'] <= .8 and row['affected_installations'] >= max(3, .6 * row['installations']):
                key = {k: row[k] for k in ('version', 'baseline_version', 'hardware', 'build', 'settings')}
                active.append(issue('performance', key, f'{label(row["version"])} / {label(row["hardware"].get("cpu_model", "unknown CPU"))} / {label(row["settings"].get("backend"))}: paired median hashrate fell {100 * (1-row["median_ratio"]):.0f}% across {row["installations"]} installations versus {label(row["baseline_version"])}. Observational signal; verify with controlled benchmarks.'))
    return active, known


def run(command, data=None):
    return subprocess.run(command, input=data, text=True, capture_output=True, timeout=45)


def check_failure(category, result=None, error=None):
    # Do not log SQL, connection strings, payloads or arbitrary stderr. Keep the
    # bounded failure class that operators need to distinguish query failures.
    detail = 'process failed'
    if isinstance(error, subprocess.TimeoutExpired):
        detail = 'process timeout'
    elif isinstance(error, ValueError):
        detail = 'invalid JSON response'
    elif isinstance(error, OSError):
        detail = 'process unavailable'
    elif result is not None:
        if 'canceling statement due to statement timeout' in result.stderr:
            detail = 'database statement timeout'
        elif 'canceling statement due to lock timeout' in result.stderr:
            detail = 'database lock timeout'
        else:
            detail = f'process exited {result.returncode}'
    print(f'Telemetry {category} check failed: {detail}.', file=sys.stderr)


def collect():
    results = {}
    try:
        health = run(['bash', str(ROOT / 'deploy/check.sh'), '--infrastructure-only'])
        results['infrastructure'] = health.returncode == 0
    except (OSError, subprocess.TimeoutExpired) as error:
        check_failure('infrastructure', error=error)
        results['infrastructure'] = None
    compose = ['docker', 'compose', '--project-directory', str(ROOT / 'deploy'),
               '--env-file', str(ROOT / 'deploy/.env'), '-f', str(ROOT / 'deploy/compose.yaml'),
               'exec', '-T', 'db', 'psql', '-X', '-U', 'telemetry', '-d', 'telemetry',
               '-qAt', '-v', 'ON_ERROR_STOP=1']
    for category, name in [('quality', 'data_quality.sql'), ('diagnostics', 'diagnostic_alerts.sql'), ('performance', 'alerts.sql')]:
        try:
            result = run(compose, (ROOT / name).read_text())
            results[category] = json.loads(result.stdout) if result.returncode == 0 else None
            if result.returncode:
                check_failure(category, result=result)
        except (OSError, subprocess.TimeoutExpired, ValueError) as error:
            check_failure(category, error=error)
            results[category] = None
    return results


def save(path, state):
    temporary = path.with_suffix('.tmp')
    with temporary.open('w') as stream:
        json.dump(state, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def advance(state, active, known, now, send, persist):
    """Two spaced observations to open/clear, retry failed delivery next check."""
    current = {item['key']: item for item in active}
    incidents = state.setdefault('incidents', {})
    if now - state.get('last_check', now) > 900:
        for record in incidents.values():
            record.pop('clear_since', None)
            if record['last_sent'] is None:
                record['first_seen'] = now
    state['last_check'] = now
    for key, item in current.items():
        record = incidents.setdefault(key, {**item, 'first_seen': now, 'last_sent': None})
        record.update(item)
        record.pop('clear_since', None)
        record['last_seen'] = now
    sends = 0
    # Oldest unsent notifications first; cap messages during widespread incidents.
    for key, record in sorted(list(incidents.items()), key=lambda pair: pair[1].get('last_sent') or 0):
        if record['category'] not in known:
            # A failed query also breaks consecutive confirmation of recovery.
            record.pop('clear_since', None)
            if record['last_sent'] is None:
                record['first_seen'] = now
            continue
        present = key in current
        if present:
            if now - record['first_seen'] < CONFIRM_SECONDS:
                continue
            if record['last_sent'] is not None:
                if (record['category'] not in REMINDER_CATEGORIES
                        or now - record['last_sent'] < REPEAT_SECONDS):
                    continue
            transition = 'FIRING' if record['last_sent'] is None else 'REMINDER'
        else:
            if record['last_sent'] is None:
                del incidents[key]
                continue
            clear_since = record.setdefault('clear_since', now)
            if now - clear_since < CONFIRM_SECONDS:
                continue
            transition = 'CLEARED'
        if sends >= MAX_SENDS:
            break
        send(transition, record, now)
        sends += 1
        if present:
            record['last_sent'] = now
        else:
            del incidents[key]
        persist(state)  # Only acknowledge delivery after HTTP success.
    persist(state)
    return sends


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def validate_webhook(url):
    parsed = urlsplit(url)
    if (parsed.scheme != 'https' or parsed.hostname not in ('discord.com', 'discordapp.com')
            or not parsed.path.startswith('/api/webhooks/') or parsed.username or parsed.password
            or parsed.port not in (None, 443) or parsed.fragment):
        raise ValueError('Expected an HTTPS Discord webhook URL')


def format_message(transition, record, now):
    """Build a bounded Discord embed using the pool relay's status colors."""
    titles = {
        'quality': 'Hashrate samples missing',
        'infrastructure': 'Telemetry infrastructure check failed',
        'ingestion': 'Telemetry reports overdue',
        'diagnostics': 'Miner reliability needs attention',
        'performance': 'Possible hashrate regression',
        'monitor': 'Telemetry check unavailable',
        'setup': 'Telemetry alerts enabled',
    }
    category = record['category']
    severity = record.get('severity', 'critical' if category == 'infrastructure' else 'warning')
    if category == 'setup':
        severity = 'info'
    colors = {'critical': 0xE74C3C, 'warning': 0xF39C12, 'info': 0x3498DB}
    cleared = transition == 'CLEARED'
    status = {'FIRING': 'Active', 'REMINDER': 'Reminder', 'CLEARED': 'Cleared',
              'SETUP TEST': 'Delivery test'}.get(transition, label(transition))
    fields = [
        {'name': 'Status', 'value': status, 'inline': True},
        {'name': 'Severity', 'value': severity.title(), 'inline': True},
    ]
    if 'version' in record:
        fields.append({'name': 'Version', 'value': label(record['version']) or 'Unknown', 'inline': True})
    if 'affected' in record:
        fields.append({'name': 'Previously affected installations' if cleared else 'Affected installations',
                       'value': label(record['affected']) or 'Unknown', 'inline': True})
    if cleared:
        fields.append({'name': 'What cleared means', 'value':
                       'Threshold cleared or observations aged out; this does not prove every miner recovered.'})
    elif record.get('action'):
        fields.append({'name': 'Next step', 'value': str(record['action'])[:1024]})
    stamp = datetime.fromtimestamp(now, ZoneInfo('America/New_York')).strftime('%Y-%m-%d %H:%M %Z')
    embed = {
        'author': {'name': 'Seine telemetry'},
        'title': f'[{status}] {titles.get(category, label(category))}'[:256],
        'description': (('Last observed: ' if cleared else '') + str(record['summary']))[:3000],
        'color': 0x2ECC71 if cleared else colors.get(severity, 0x3498DB),
        'fields': fields,
        'footer': {'text': f'{stamp} · Incident {str(record["key"])[:100]}'},
    }
    return {'allowed_mentions': {'parse': []}, 'embeds': [embed]}


def deliver(url, transition, record, now):
    validate_webhook(url)
    parsed = urlsplit(url)
    query = [(key, value) for key, value in parse_qsl(parsed.query) if key != 'wait']
    url = urlunsplit(parsed._replace(query=urlencode(query + [('wait', 'true')])))
    body = json.dumps(format_message(transition, record, now)).encode()
    request = urllib.request.Request(url, body, {'Content-Type': 'application/json', 'User-Agent': 'seine-telemetry-alerts/1'})
    with urllib.request.build_opener(NoRedirect).open(request, timeout=10) as response:
        if response.status != 200:
            raise RuntimeError('Notification delivery failed')
        result = json.loads(response.read(65536))
        if not result.get('id'):
            raise RuntimeError('Notification confirmation missing')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true', help='Read checks and print candidates; no state writes or messages')
    parser.add_argument('--state-dir', type=Path, default=Path('/var/lib/seine-telemetry-alerts'))
    args = parser.parse_args()
    os.umask(0o077)
    if args.dry_run:
        active, known = evaluate(collect())
        print(json.dumps({'active': active, 'observed_categories': sorted(known)}, indent=2))
        return 0
    url = os.environ.get('DISCORD_WEBHOOK_URL', '').strip()
    validate_webhook(url)
    args.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (args.state_dir / 'lock').open('w') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print('Another alert check is running; skipping.')
            return 0
        path = args.state_dir / 'state.json'
        state = json.loads(path.read_text()) if path.exists() else {'incidents': {}}
        active, known = evaluate(collect())
        sends = advance(state, active, known, time.time(),
                        lambda *values: deliver(url, *values), lambda value: save(path, value))
        print(f'Telemetry alerts: {len(active)} active thresholds, {sends} delivered, {len(known)} observed categories.')
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        # urllib exceptions may contain the webhook token; never log their text.
        print(f'Telemetry alert check failed ({type(error).__name__}); delivery remains pending. Check configuration and service connectivity.', file=sys.stderr)
        sys.exit(1)
