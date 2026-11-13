"""Actual webhook formatter output with synthetic aggregates; never sends messages."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from alerts import advance, evaluate, format_message, issue


def messages():
    now = 1792965060
    quality, _ = evaluate({'quality': {'versions': [
        {'version': 'v0.2.24', 'sustained_inventory_only_installations': 99},
        {'version': 'v0.2.26', 'sustained_inventory_only_installations': 5},
    ]}})
    unclean, _ = evaluate({'diagnostics': {
        'recent_error_versions': [], 'recent_unclean_versions': [
            {'version': 'v0.2.35', 'affected_installations': 3, 'reporting_versions': ['v0.2.36']},
            {'version': 'unknown', 'affected_installations': 3, 'reporting_versions': ['v0.2.26']},
        ]}})
    drop = {'version': 'v0.3.1', 'baseline_version': 'v0.3.0',
            'hardware': {'cpu_model': 'Example CPU (fixture)'}, 'build': [],
            'settings': {'backend': 'cpu'}, 'installations': 5, 'affected_installations': 3,
            'median_ratio': .79, 'model_installations': 469, 'model_median_ratio': .993}
    performance, _ = evaluate({'performance': {'minutes_since_last_report': 0, 'performance_cohorts': [
        drop, dict(drop, baseline_version='v0.3.1')]}})
    cases = {
        'performance-release': ('FIRING', performance[0]),
        'performance-unchanged': ('FIRING', performance[1]),
        'performance-cleared': ('CLEARED', performance[1]),
        'legacy': ('FIRING', quality[0]),
        'warning': ('FIRING', quality[1]),
        'reminder': ('REMINDER', issue('infrastructure', 'fixture', 'Collector infrastructure check still failing.')),
        'cleared': ('CLEARED', quality[0]),
        'critical': ('FIRING', issue('infrastructure', 'fixture', 'Collector infrastructure check failed.')),
        'setup': ('SETUP TEST', issue('setup', 'fixture', 'Delivery test only; this is not an incident.')),
        'old-state': ('CLEARED', issue('quality', 'fixture', 'Previously reported missing samples.')),
        'unclean-upgrade': ('FIRING', unclean[0]),
        'unclean-unknown': ('FIRING', unclean[1]),
        'unclean-cleared': ('CLEARED', unclean[0]),
    }
    fixtures = {name: format_message(transition, record, now)
                for name, (transition, record) in cases.items()}
    state, lifecycle = {}, []
    for seconds, title, active in [
        (0, 'Pending confirmation', quality),
        (300, 'New incidents', quality),
        (15000, 'Known incidents: no reminders', quality),
        (15300, 'Pending recovery', []),
        (15600, 'Confirmed recovery', []),
        (15900, 'Pending recurrence', quality),
        (16200, 'Confirmed recurrence', quality),
    ]:
        delivered = []
        advance(state, active, {'quality'}, now + seconds,
                lambda transition, record, stamp: delivered.append(transition),
                lambda value: None)
        lifecycle.append({'state': title, 'delivered': delivered})
    fixtures['quiet-lifecycle'] = lifecycle
    return fixtures
