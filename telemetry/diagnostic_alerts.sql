BEGIN READ ONLY;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '2s';
SET LOCAL TIME ZONE 'America/New_York';
-- Alert evaluation needs counts, not the full seven-day hardware/event clusters.
WITH covered AS (
    SELECT installation_id, received_at, payload->>'miner_version' AS version,
           payload->'diagnostics' AS diagnostics
    FROM telemetry_reports
    WHERE received_at >= now() - interval '7 days'
      AND NOT (payload->>'debug_build')::boolean
      AND payload->'diagnostics' IS NOT NULL
      AND payload->'diagnostics' <> 'null'::jsonb
), build_versions AS (
    SELECT diagnostics->>'build_commit' AS build_commit, min(version) AS version
    FROM covered
    WHERE diagnostics->>'build_commit' <> 'unknown'
    GROUP BY 1 HAVING count(DISTINCT version) = 1
), recent_unclean AS (
    SELECT coalesce(c.diagnostics #>> '{previous_run,miner_version}', b.version, 'unknown') AS version,
           count(DISTINCT c.installation_id) AS affected_installations,
           jsonb_agg(DISTINCT c.version ORDER BY c.version) AS reporting_versions
    FROM covered c LEFT JOIN build_versions b
      ON b.build_commit = c.diagnostics #>> '{previous_run,build_commit}'
    WHERE c.received_at >= now() - interval '24 hours'
      AND (c.diagnostics->>'previous_run_unclean')::boolean
    GROUP BY 1
), recent_events AS (
    SELECT installation_id, received_at, version, event
    FROM covered CROSS JOIN LATERAL jsonb_array_elements(diagnostics->'events') event
    WHERE received_at >= now() - interval '25 hours'
), recent_errors AS (
    SELECT version, count(DISTINCT installation_id) AS affected_installations
    FROM recent_events WHERE received_at >= now() - interval '24 hours'
      AND event->>'code' IN ('error','backend_error','backend_quarantined','run_failed','rust_panic')
    GROUP BY 1
), recent_install_failures AS (
    SELECT version, count(DISTINCT installation_id) AS affected_installations
    FROM covered
    WHERE received_at >= now() - interval '24 hours'
      AND (diagnostics #>> '{update,install_failures}')::bigint > 0
    GROUP BY 1
), recent_paused AS (
    SELECT version, count(DISTINCT installation_id) AS affected_installations
    FROM covered
    WHERE received_at >= now() - interval '24 hours'
      AND (diagnostics->>'unsupported_work_paused_secs')::bigint > 0
    GROUP BY 1
), pool_connection AS (
    SELECT installation_id, received_at,
           event #>> '{detail,reason}' = 'pool_connection_limit' AS refused_at_limit
    FROM recent_events
    WHERE event #>> '{detail,operation}' IN ('pool_resolve','pool_connect','pool_read','pool_write','pool_login')
), pool_connection_hours AS (
    SELECT date_trunc('hour', received_at) AS hour, count(DISTINCT installation_id) AS installations
    FROM pool_connection WHERE received_at < now() - interval '1 hour'
    GROUP BY 1
)
SELECT jsonb_build_object(
    'recent_error_versions', coalesce((SELECT jsonb_agg(recent_errors ORDER BY version) FROM recent_errors), '[]'::jsonb),
    'recent_unclean_versions', coalesce((SELECT jsonb_agg(recent_unclean ORDER BY version) FROM recent_unclean), '[]'::jsonb),
    'recent_install_failure_versions', coalesce((SELECT jsonb_agg(r ORDER BY version) FROM recent_install_failures r), '[]'::jsonb),
    'recent_unsupported_work_versions', coalesce((SELECT jsonb_agg(r ORDER BY version) FROM recent_paused r), '[]'::jsonb),
    'pool_connection_warnings', jsonb_build_object(
        'last_hour_installations', (SELECT count(DISTINCT installation_id) FROM pool_connection WHERE received_at >= now() - interval '1 hour'),
        'last_hour_limit_installations', (SELECT count(DISTINCT installation_id) FROM pool_connection WHERE received_at >= now() - interval '1 hour' AND refused_at_limit),
        'baseline_hourly_installations', (SELECT coalesce(sum(installations), 0) / 24.0 FROM pool_connection_hours)
    ),
    'status', CASE WHEN EXISTS (SELECT 1 FROM recent_errors WHERE affected_installations >= 3)
                    OR EXISTS (SELECT 1 FROM recent_unclean WHERE affected_installations >= 3)
                    OR EXISTS (SELECT 1 FROM recent_install_failures WHERE affected_installations >= 3)
                    OR EXISTS (SELECT 1 FROM recent_paused WHERE affected_installations >= 3) THEN 'attention'
                   WHEN NOT EXISTS (SELECT 1 FROM covered) THEN 'unavailable' ELSE 'ok' END
);
ROLLBACK;
