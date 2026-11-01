BEGIN READ ONLY;
-- The on-demand report builds seven-day clusters. Scheduled checks use the
-- separate diagnostic_alerts.sql query and retain their shorter deadline.
SET LOCAL statement_timeout = '30s';
SET LOCAL lock_timeout = '2s';
SET LOCAL TIME ZONE 'America/New_York';
WITH reports AS (
    SELECT installation_id, received_at,
           jsonb_build_object('hardware', payload->'hardware',
                              'backends', payload->'backends') AS payload,
           payload->>'miner_version' AS version, payload->'diagnostics' AS diagnostics
    FROM telemetry_reports
    WHERE received_at >= now() - interval '7 days'
      AND NOT (payload->>'debug_build')::boolean
), covered AS (
    SELECT * FROM reports WHERE diagnostics IS NOT NULL AND diagnostics <> 'null'::jsonb
), versions AS (
    SELECT version, count(DISTINCT installation_id) AS installations,
           count(DISTINCT installation_id) FILTER (WHERE diagnostics IS NOT NULL AND diagnostics <> 'null'::jsonb) AS diagnostic_installations
    FROM reports GROUP BY 1
), events AS (
    SELECT installation_id, received_at, version, payload, diagnostics, event
    FROM covered CROSS JOIN LATERAL jsonb_array_elements(diagnostics->'events') event
), build_versions AS (
    -- Historical schema-4 checkpoints have a build but no release label. Infer
    -- only an unambiguous observed mapping; never blame the restarting version.
    SELECT diagnostics->>'build_commit' AS build_commit, min(version) AS version
    FROM covered
    WHERE diagnostics->>'build_commit' <> 'unknown'
    GROUP BY 1 HAVING count(DISTINCT version) = 1
), unclean AS (
    SELECT c.*,
           coalesce(c.diagnostics #>> '{previous_run,miner_version}', b.version, 'unknown') AS previous_version
    FROM covered c LEFT JOIN build_versions b
      ON b.build_commit = c.diagnostics #>> '{previous_run,build_commit}'
    WHERE c.received_at >= now() - interval '24 hours'
      AND (c.diagnostics->>'previous_run_unclean')::boolean
), clusters AS (
    SELECT version, diagnostics->>'build_commit' AS build_commit,
           diagnostics->>'source_fingerprint' AS source_fingerprint,
           payload #>> '{hardware,cpu_model}' AS cpu,
           payload #>> '{hardware,os}' AS os,
           payload #>> '{hardware,runtime_environment}' AS environment,
           diagnostics->>'mode' AS mode,
           diagnostics->'requested_backends' AS requested_backends,
           event->>'code' AS code, event->>'kind' AS kind,
           event->>'source' AS source, (event->>'line')::int AS line,
           event->>'backend' AS backend,
           event->'detail' AS detail, event->>'phase' AS phase,
           diagnostics->>'cpu_page_mode' AS requested_page_mode,
           (diagnostics->>'requested_cpu_threads')::int AS requested_threads,
           diagnostics->'nvidia_software' AS nvidia_software,
           min((payload #>> '{hardware,resources,available_memory_gib}')::int) AS minimum_reported_available_memory_gib,
           count(DISTINCT installation_id) AS affected_installations,
           sum((event->>'count')::bigint) AS occurrences,
           max(received_at) AS last_received
    FROM events GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
), unclean_clusters AS (
    SELECT previous_version AS version, diagnostics->>'build_commit' AS reporting_build_commit,
           payload #>> '{hardware,cpu_model}' AS cpu,
           payload #>> '{hardware,os}' AS os,
           payload #>> '{hardware,runtime_environment}' AS environment,
           diagnostics #>> '{previous_run,phase}' AS last_observed_phase,
           diagnostics #>> '{previous_run,build_commit}' AS previous_build_commit,
           diagnostics #>> '{previous_run,shutdown_request}' AS shutdown_request,
           diagnostics #>> '{previous_run,cleanup_stage}' AS last_cleanup_stage,
           diagnostics #> '{previous_run,nvidia_software}' AS previous_nvidia_software,
           min((diagnostics #>> '{previous_run,uptime_secs}')::bigint) AS minimum_previous_uptime_secs,
           max((diagnostics #>> '{previous_run,uptime_secs}')::bigint) AS maximum_previous_uptime_secs,
           count(DISTINCT installation_id) AS affected_installations,
           count(DISTINCT installation_id) FILTER (WHERE EXISTS (
               SELECT 1 FROM jsonb_array_elements(payload->'backends') b
               WHERE (b->>'hashes')::bigint > 0
           )) AS installations_hashing_in_restart_report,
           max(received_at) AS last_received,
           jsonb_agg(DISTINCT version ORDER BY version) AS reporting_versions,
           count(DISTINCT installation_id) FILTER (WHERE
               (diagnostics #>> '{previous_run,events_truncated}')::boolean
           ) AS installations_with_truncated_previous_events
    FROM unclean
    GROUP BY 1,2,3,4,5,6,7,8,9,10
), previous_events AS (
    SELECT previous_version AS version,
           diagnostics #>> '{previous_run,build_commit}' AS build_commit,
           payload #>> '{hardware,cpu_model}' AS restart_cpu,
           payload #>> '{hardware,os}' AS restart_os,
           event->>'code' AS code, event->>'kind' AS kind,
           event->>'source' AS source, (event->>'line')::int AS line,
           event->>'backend' AS backend, event->'detail' AS detail,
           event->>'phase' AS phase,
           count(DISTINCT installation_id) AS affected_installations,
           max(received_at) AS last_received
    FROM unclean CROSS JOIN LATERAL jsonb_array_elements(
        coalesce(diagnostics #> '{previous_run,recent_events}', '[]'::jsonb)
    ) event
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
), shutdown_clusters AS (
    SELECT version, diagnostics->>'shutdown_request' AS shutdown_request,
           diagnostics->>'cleanup_stage' AS last_cleanup_stage,
           diagnostics->>'reason' AS report_reason,
           count(DISTINCT installation_id) AS affected_installations,
           max(received_at) AS last_received
    FROM covered
    WHERE received_at >= now() - interval '24 hours'
      AND (diagnostics->>'shutdown_request' IS NOT NULL OR diagnostics->>'cleanup_stage' IS NOT NULL)
    GROUP BY 1,2,3,4
), recent_errors AS (
    SELECT version, count(DISTINCT installation_id) AS affected_installations
    FROM events WHERE received_at >= now() - interval '24 hours'
      AND event->>'code' IN ('error','backend_error','backend_quarantined','run_failed','rust_panic')
    GROUP BY 1
), pool_connection AS (
    -- Pool connection warnings (resolve, connect, read, write, login and
    -- connection-limit refusals). Fleet-wide spikes against the prior day
    -- point at the pool or the network, not at one miner.
    SELECT installation_id, received_at,
           event #>> '{detail,reason}' = 'pool_connection_limit' AS refused_at_limit
    FROM events
    WHERE received_at >= now() - interval '25 hours'
      AND event #>> '{detail,operation}' IN ('pool_resolve', 'pool_connect', 'pool_read', 'pool_write', 'pool_login')
), pool_connection_hours AS (
    SELECT date_trunc('hour', received_at) AS hour, count(DISTINCT installation_id) AS installations
    FROM pool_connection
    WHERE received_at < now() - interval '1 hour'
    GROUP BY 1
), recent_unclean AS (
    SELECT previous_version AS version, count(DISTINCT installation_id) AS affected_installations,
           jsonb_agg(DISTINCT version ORDER BY version) AS reporting_versions
    FROM unclean
    GROUP BY 1
)
SELECT jsonb_build_object(
    'window_days', 7,
    'diagnostic_installations', (SELECT count(DISTINCT installation_id) FROM covered),
    'versions', coalesce((SELECT jsonb_agg(versions ORDER BY version) FROM versions), '[]'::jsonb),
    'issue_clusters', coalesce((SELECT jsonb_agg(c) FROM (SELECT * FROM clusters ORDER BY (code IN ('error','backend_error','backend_quarantined','run_failed','rust_panic')) DESC, (detail IS NOT NULL) DESC, affected_installations DESC, occurrences DESC, version, source, line LIMIT 30) c), '[]'::jsonb),
    'shutdown_clusters', coalesce((SELECT jsonb_agg(c) FROM (SELECT * FROM shutdown_clusters ORDER BY affected_installations DESC, last_received DESC LIMIT 30) c), '[]'::jsonb),
    'unclean_run_clusters', coalesce((SELECT jsonb_agg(c) FROM (SELECT * FROM unclean_clusters ORDER BY affected_installations DESC, last_received DESC LIMIT 30) c), '[]'::jsonb),
    -- Recovered events may already appear in issue_clusters. Do not add their
    -- counts to current-run error alerts or imply they caused termination.
    'previous_run_issue_clusters', coalesce((SELECT jsonb_agg(c) FROM (SELECT * FROM previous_events ORDER BY (code IN ('error','backend_error','backend_quarantined','run_failed','rust_panic')) DESC, (detail IS NOT NULL) DESC, affected_installations DESC, last_received DESC LIMIT 30) c), '[]'::jsonb),
    'unclean_run_installations', (SELECT count(DISTINCT installation_id) FROM covered WHERE (diagnostics->>'previous_run_unclean')::boolean),
    'reports_with_dropped_diagnostics', (SELECT count(*) FROM covered WHERE (diagnostics->>'dropped_events')::bigint > 0 OR (diagnostics->>'dropped_backend_samples')::bigint > 0),
    'recent_error_versions', coalesce((SELECT jsonb_agg(recent_errors ORDER BY version) FROM recent_errors), '[]'::jsonb),
    'recent_unclean_versions', coalesce((SELECT jsonb_agg(recent_unclean ORDER BY version) FROM recent_unclean), '[]'::jsonb),
    'pool_connection_warnings', jsonb_build_object(
        'last_hour_installations', (SELECT count(DISTINCT installation_id) FROM pool_connection WHERE received_at >= now() - interval '1 hour'),
        'last_hour_limit_installations', (SELECT count(DISTINCT installation_id) FROM pool_connection WHERE received_at >= now() - interval '1 hour' AND refused_at_limit),
        -- Hours with no warnings count as zero.
        'baseline_hourly_installations', (SELECT coalesce(sum(installations), 0) / 24.0 FROM pool_connection_hours)
    ),
    'status', CASE WHEN EXISTS (SELECT 1 FROM recent_errors WHERE affected_installations >= 3)
                    OR EXISTS (SELECT 1 FROM recent_unclean WHERE affected_installations >= 3) THEN 'attention'
                   WHEN NOT EXISTS (SELECT 1 FROM covered) THEN 'unavailable' ELSE 'ok' END
);
ROLLBACK;
