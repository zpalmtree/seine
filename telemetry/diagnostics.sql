BEGIN READ ONLY;
-- The on-demand report builds seven-day clusters. Scheduled checks use the
-- separate diagnostic_alerts.sql query and retain their shorter deadline.
SET LOCAL statement_timeout = '60s';
SET LOCAL lock_timeout = '2s';
-- Row estimates for the jsonb expansions are far too high, and the planner would
-- keep the shorter scans on one process; decode payloads in parallel.
SET LOCAL parallel_tuple_cost = 0.01;
SET LOCAL parallel_setup_cost = 0;
SET LOCAL min_parallel_index_scan_size = 0;
SET LOCAL min_parallel_table_scan_size = 0;
SET LOCAL enable_memoize = off;
-- The planner expects a hundred events per report and would sort every event by
-- its cluster keys through an index walk; group them in hash tables instead.
SET LOCAL enable_sort = off;
SET LOCAL enable_incremental_sort = off;
SET LOCAL max_parallel_workers_per_gather = 3;
SET LOCAL jit = off;
SET LOCAL work_mem = '64MB';
SET LOCAL TIME ZONE 'America/New_York';
WITH installations AS MATERIALIZED (
    -- Seven days of narrow derived rows (src/schema.sql), reduced to one row per
    -- installation, version and build.
    SELECT installation_id, version, build_commit, has_diagnostics,
           min(received_at) AS first_seen, max(received_at) AS last_seen,
           coalesce(bool_or(previous_run_unclean), false) AS previous_run_unclean,
           count(*) FILTER (WHERE dropped_diagnostics) AS dropped_reports
    FROM telemetry_report_facts
    WHERE received_at >= now() - interval '7 days'
      AND NOT debug_build
    GROUP BY 1, 2, 3, 4
), covered AS (
    SELECT * FROM installations WHERE has_diagnostics
), versions AS (
    SELECT version, count(DISTINCT installation_id) AS installations,
           count(DISTINCT installation_id) FILTER (WHERE has_diagnostics) AS diagnostic_installations
    FROM installations GROUP BY 1
), installation_events AS MATERIALIZED (
    -- The only seven-day payload pass: every event, reduced to one row per
    -- installation and cluster before the clusters count installations.
    SELECT installation_id, p->>'miner_version' AS version, d->>'build_commit' AS build_commit,
           d->>'source_fingerprint' AS source_fingerprint,
           p #>> '{hardware,cpu_model}' AS cpu,
           p #>> '{hardware,os}' AS os,
           p #>> '{hardware,runtime_environment}' AS environment,
           d->>'mode' AS mode,
           d->'requested_backends' AS requested_backends,
           event->>'code' AS code, event->>'kind' AS kind,
           event->>'source' AS source, (event->>'line')::int AS line,
           event->>'backend' AS backend,
           event->'detail' AS detail, event->>'phase' AS phase,
           d->>'cpu_page_mode' AS requested_page_mode,
           (d->>'requested_cpu_threads')::int AS requested_threads,
           d->'nvidia_software' AS nvidia_software,
           min((p #>> '{hardware,resources,available_memory_gib}')::int) AS minimum_reported_available_memory_gib,
           sum((event->>'count')::bigint) AS occurrences,
           max(received_at) AS last_received
    FROM telemetry_reports
    -- Each payload reference would otherwise decompress the stored value again.
    CROSS JOIN LATERAL unnest(ARRAY[payload || '{}'::jsonb]) p
    CROSS JOIN LATERAL (SELECT p->'diagnostics' AS d) diagnostics
    CROSS JOIN LATERAL jsonb_array_elements(d->'events') event
    WHERE received_at >= now() - interval '7 days'
      AND NOT (p->>'debug_build')::boolean
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
), recent AS MATERIALIZED (
    -- The last 25 hours read payloads once each into narrow columns. Restart
    -- reports are few and keep their hardware, backends and diagnostics.
    SELECT installation_id, received_at, p->>'miner_version' AS version,
           (d->>'previous_run_unclean')::boolean AS previous_run_unclean,
           (d #>> '{update,install_failures}')::bigint > 0 AS install_failures,
           (d->>'unsupported_work_paused_secs')::bigint > 0 AS paused,
           d->>'shutdown_request' AS shutdown_request,
           d->>'cleanup_stage' AS cleanup_stage,
           d->>'reason' AS reason,
           -- Path queries are plain function calls and run in the parallel workers;
           -- a sub-select per report would run on one process.
           jsonb_path_exists(d, '$.events[*] ? (@.code == $codes[*])',
               '{"codes": ["error", "backend_error", "backend_quarantined", "run_failed", "rust_panic"]}') AS error,
           -- Pool connection warnings (resolve, connect, read, write, login and
           -- connection-limit refusals). Fleet-wide spikes against the prior day
           -- point at the pool or the network, not at one miner.
           jsonb_path_exists(d, '$.events[*] ? (@.detail.operation == $operations[*])',
               '{"operations": ["pool_resolve", "pool_connect", "pool_read", "pool_write", "pool_login"]}') AS pool_warning,
           jsonb_path_exists(d, '$.events[*] ? (@.detail.operation == $operations[*] && @.detail.reason == "pool_connection_limit")',
               '{"operations": ["pool_resolve", "pool_connect", "pool_read", "pool_write", "pool_login"]}') AS refused_at_limit,
           CASE WHEN (d->>'previous_run_unclean')::boolean
                THEN jsonb_build_object('hardware', p->'hardware', 'backends', p->'backends') END AS payload,
           CASE WHEN (d->>'previous_run_unclean')::boolean THEN d END AS diagnostics
    FROM telemetry_reports
    -- Each payload reference would otherwise decompress the stored value again.
    CROSS JOIN LATERAL unnest(ARRAY[payload || '{}'::jsonb]) p
    CROSS JOIN LATERAL (SELECT p->'diagnostics' AS d) diagnostics
    WHERE received_at >= now() - interval '25 hours'
      AND NOT (p->>'debug_build')::boolean
      AND d IS NOT NULL
      AND d <> 'null'::jsonb
), build_versions AS (
    -- Historical schema-4 checkpoints have a build but no release label. Infer
    -- only an unambiguous observed mapping; never blame the restarting version.
    SELECT build_commit, min(version) AS version
    FROM covered
    WHERE build_commit <> 'unknown'
    GROUP BY 1 HAVING count(DISTINCT version) = 1
), unclean AS (
    SELECT c.*,
           coalesce(c.diagnostics #>> '{previous_run,miner_version}', b.version, 'unknown') AS previous_version
    FROM recent c LEFT JOIN build_versions b
      ON b.build_commit = c.diagnostics #>> '{previous_run,build_commit}'
    WHERE c.received_at >= now() - interval '24 hours'
      AND c.previous_run_unclean
), clusters AS (
    SELECT version, build_commit, source_fingerprint, cpu, os, environment, mode,
             requested_backends, code, kind, source, line, backend, detail, phase,
             requested_page_mode, requested_threads, nvidia_software,
           min(minimum_reported_available_memory_gib) AS minimum_reported_available_memory_gib,
           count(*) AS affected_installations,
           sum(occurrences) AS occurrences,
           max(last_received) AS last_received
    FROM installation_events GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
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
           ) AS installations_with_truncated_previous_events,
           count(DISTINCT installation_id) FILTER (WHERE
               (diagnostics->>'previous_run_rebooted')::boolean
           ) AS installations_rebooted
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
    SELECT version, shutdown_request,
           cleanup_stage AS last_cleanup_stage,
           reason AS report_reason,
           count(DISTINCT installation_id) AS affected_installations,
           max(received_at) AS last_received
    FROM recent
    WHERE received_at >= now() - interval '24 hours'
      AND (shutdown_request IS NOT NULL OR cleanup_stage IS NOT NULL)
    GROUP BY 1,2,3,4
), recent_installations AS (
    SELECT version, count(DISTINCT installation_id) AS installations
    FROM covered WHERE last_seen >= now() - interval '24 hours'
    GROUP BY 1
), release_lines AS (
    -- Same release-line rule as data_quality.sql: the newest major.minor among
    -- v-prefixed release builds; source builds and commits never set it.
    SELECT DISTINCT version,
           (regexp_match(version, '^v?(\d+)\.(\d+)\.\d+$'))[1]::int AS major,
           (regexp_match(version, '^v?(\d+)\.(\d+)\.\d+$'))[2]::int AS minor,
           version ~ '^v\d+\.\d+\.\d+$' AS release_build
    FROM recent_installations
), newest_line AS (
    SELECT major, minor FROM release_lines WHERE release_build
    ORDER BY major DESC, minor DESC LIMIT 1
), legacy_versions AS (
    SELECT l.version FROM release_lines l, newest_line n
    WHERE l.major IS NOT NULL AND (l.major, l.minor) < (n.major, n.minor)
), recent_errors AS (
    SELECT version, count(DISTINCT installation_id) AS affected_installations
    FROM recent WHERE received_at >= now() - interval '24 hours' AND error
    GROUP BY 1
), recent_install_failures AS (
    SELECT version, count(DISTINCT installation_id) AS affected_installations
    FROM recent
    WHERE received_at >= now() - interval '24 hours' AND install_failures
    GROUP BY 1
), recent_paused AS (
    SELECT version, count(DISTINCT installation_id) AS affected_installations
    FROM recent
    WHERE received_at >= now() - interval '24 hours' AND paused
    GROUP BY 1
), pool_connection AS (
    SELECT installation_id, received_at, refused_at_limit
    FROM recent WHERE pool_warning
), pool_connection_hours AS (
    SELECT date_trunc('hour', received_at) AS hour, count(DISTINCT installation_id) AS installations
    FROM pool_connection
    WHERE received_at < now() - interval '1 hour'
    GROUP BY 1
), unclean_installations AS (
    -- A reboot, or one restart of an installation first seen in the last day (a
    -- machine being set up or torn down), is listed but not counted toward attention.
    SELECT u.installation_id, u.restarts, f.first_seen >= now() - interval '24 hours' AS new_installation
    FROM (SELECT installation_id, count(*) AS restarts FROM unclean GROUP BY 1) u
    JOIN (SELECT installation_id, min(first_seen) AS first_seen FROM covered
          WHERE installation_id IN (SELECT installation_id FROM unclean) GROUP BY 1) f USING (installation_id)
), recent_unclean AS (
    SELECT u.previous_version AS version, count(DISTINCT u.installation_id) AS affected_installations,
           count(DISTINCT u.installation_id) FILTER (
             WHERE (u.diagnostics->>'previous_run_rebooted')::boolean) AS rebooted_installations,
           count(DISTINCT u.installation_id) FILTER (
             WHERE i.new_installation AND i.restarts = 1
               AND (u.diagnostics->>'previous_run_rebooted')::boolean IS NOT TRUE) AS new_installation_single_restarts,
           count(DISTINCT u.installation_id) FILTER (
             WHERE (u.diagnostics->>'previous_run_rebooted')::boolean IS NOT TRUE
               AND NOT (i.new_installation AND i.restarts = 1)) AS counted_installations,
           jsonb_agg(DISTINCT u.version ORDER BY u.version) AS reporting_versions
    FROM unclean u JOIN unclean_installations i USING (installation_id)
    GROUP BY 1
), recent_errors_rated AS (
    -- Errors and unclean runs scale with each version's fleet: attention needs at
    -- least three installations and 5% (errors) or 10% (unclean runs) of the
    -- version's installations reporting in 24 hours. Older release lines and
    -- unattributed runs are listed but only an upgrade changes them.
    SELECT r.version, r.affected_installations,
           greatest(coalesce(i.installations, 0), r.affected_installations) AS installations_24h,
           r.version IN (SELECT version FROM legacy_versions) AS legacy,
           r.affected_installations >= 3
             AND r.affected_installations >= 0.05 * greatest(coalesce(i.installations, 0), r.affected_installations)
             AND r.version NOT IN (SELECT version FROM legacy_versions) AS needs_attention
    FROM recent_errors r LEFT JOIN recent_installations i USING (version)
), recent_unclean_rated AS (
    SELECT r.version, r.affected_installations, r.counted_installations,
           r.rebooted_installations, r.new_installation_single_restarts, r.reporting_versions,
           greatest(coalesce(i.installations, 0), r.affected_installations) AS installations_24h,
           r.version IN (SELECT version FROM legacy_versions) AS legacy,
           r.counted_installations >= 3
             AND r.counted_installations >= 0.10 * greatest(coalesce(i.installations, 0), r.affected_installations)
             AND r.version <> 'unknown'
             AND r.version NOT IN (SELECT version FROM legacy_versions) AS needs_attention
    FROM recent_unclean r LEFT JOIN recent_installations i USING (version)
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
    'unclean_run_installations', (SELECT count(DISTINCT installation_id) FROM covered WHERE previous_run_unclean),
    'reports_with_dropped_diagnostics', (SELECT coalesce(sum(dropped_reports), 0) FROM covered),
    'recent_error_versions', coalesce((SELECT jsonb_agg(recent_errors ORDER BY version) FROM recent_errors_rated recent_errors), '[]'::jsonb),
    'recent_unclean_versions', coalesce((SELECT jsonb_agg(recent_unclean ORDER BY version) FROM recent_unclean_rated recent_unclean), '[]'::jsonb),
    'recent_install_failure_versions', coalesce((SELECT jsonb_agg(r ORDER BY version) FROM recent_install_failures r), '[]'::jsonb),
    'recent_unsupported_work_versions', coalesce((SELECT jsonb_agg(r ORDER BY version) FROM recent_paused r), '[]'::jsonb),
    'pool_connection_warnings', jsonb_build_object(
        'last_hour_installations', (SELECT count(DISTINCT installation_id) FROM pool_connection WHERE received_at >= now() - interval '1 hour'),
        'last_hour_limit_installations', (SELECT count(DISTINCT installation_id) FROM pool_connection WHERE received_at >= now() - interval '1 hour' AND refused_at_limit),
        -- Hours with no warnings count as zero.
        'baseline_hourly_installations', (SELECT coalesce(sum(installations), 0) / 24.0 FROM pool_connection_hours)
    ),
    'status', CASE WHEN EXISTS (SELECT 1 FROM recent_errors_rated WHERE needs_attention)
                    OR EXISTS (SELECT 1 FROM recent_unclean_rated WHERE needs_attention)
                    OR EXISTS (SELECT 1 FROM recent_install_failures WHERE affected_installations >= 3)
                    OR EXISTS (SELECT 1 FROM recent_paused WHERE affected_installations >= 3) THEN 'attention'
                   WHEN NOT EXISTS (SELECT 1 FROM covered) THEN 'unavailable' ELSE 'ok' END
);
ROLLBACK;
