BEGIN READ ONLY;
SET LOCAL statement_timeout = '30s';
SET LOCAL lock_timeout = '2s';
-- Row estimates for the jsonb expansions are far too high; keep the scan parallel.
SET LOCAL parallel_tuple_cost = 0.01;
SET LOCAL TIME ZONE 'America/New_York';
-- Alert evaluation needs counts, not the full seven-day hardware/event clusters.
WITH covered AS (
    -- Only the last 25 hours read diagnostics; older rows carry narrow columns.
    SELECT installation_id, received_at, p->>'miner_version' AS version,
           p #>> '{diagnostics,build_commit}' AS build_commit,
           CASE WHEN received_at >= now() - interval '25 hours' THEN p->'diagnostics' END AS diagnostics
    FROM telemetry_reports
    -- Each payload reference would otherwise decompress the stored value again.
    CROSS JOIN LATERAL unnest(ARRAY[payload || '{}'::jsonb]) p
    WHERE received_at >= now() - interval '7 days'
      AND NOT (p->>'debug_build')::boolean
      AND p->'diagnostics' IS NOT NULL
      AND p->'diagnostics' <> 'null'::jsonb
), build_versions AS (
    SELECT build_commit, min(version) AS version
    FROM covered
    WHERE build_commit <> 'unknown'
    GROUP BY 1 HAVING count(DISTINCT version) = 1
), unclean AS (
    SELECT c.*,
           coalesce(c.diagnostics #>> '{previous_run,miner_version}', b.version, 'unknown') AS previous_version
    FROM covered c LEFT JOIN build_versions b
      ON b.build_commit = c.diagnostics #>> '{previous_run,build_commit}'
    WHERE c.received_at >= now() - interval '24 hours'
      AND (c.diagnostics->>'previous_run_unclean')::boolean
), unclean_installations AS (
    -- Same rule as diagnostics.sql: a reboot, or one restart of an installation first
    -- seen in the last day, is listed but not counted toward attention.
    SELECT u.installation_id, u.restarts, f.first_seen >= now() - interval '24 hours' AS new_installation
    FROM (SELECT installation_id, count(*) AS restarts FROM unclean GROUP BY 1) u
    JOIN (SELECT installation_id, min(received_at) AS first_seen FROM covered
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
), recent_events AS (
    SELECT installation_id, received_at, version, event
    FROM covered CROSS JOIN LATERAL jsonb_array_elements(diagnostics->'events') event
    WHERE received_at >= now() - interval '25 hours'
), recent_installations AS (
    SELECT version, count(DISTINCT installation_id) AS installations
    FROM covered WHERE received_at >= now() - interval '24 hours'
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
    'recent_error_versions', coalesce((SELECT jsonb_agg(recent_errors ORDER BY version) FROM recent_errors_rated recent_errors), '[]'::jsonb),
    'recent_unclean_versions', coalesce((SELECT jsonb_agg(recent_unclean ORDER BY version) FROM recent_unclean_rated recent_unclean), '[]'::jsonb),
    'recent_install_failure_versions', coalesce((SELECT jsonb_agg(r ORDER BY version) FROM recent_install_failures r), '[]'::jsonb),
    'recent_unsupported_work_versions', coalesce((SELECT jsonb_agg(r ORDER BY version) FROM recent_paused r), '[]'::jsonb),
    'pool_connection_warnings', jsonb_build_object(
        'last_hour_installations', (SELECT count(DISTINCT installation_id) FROM pool_connection WHERE received_at >= now() - interval '1 hour'),
        'last_hour_limit_installations', (SELECT count(DISTINCT installation_id) FROM pool_connection WHERE received_at >= now() - interval '1 hour' AND refused_at_limit),
        'baseline_hourly_installations', (SELECT coalesce(sum(installations), 0) / 24.0 FROM pool_connection_hours)
    ),
    'status', CASE WHEN EXISTS (SELECT 1 FROM recent_errors_rated WHERE needs_attention)
                    OR EXISTS (SELECT 1 FROM recent_unclean_rated WHERE needs_attention)
                    OR EXISTS (SELECT 1 FROM recent_install_failures WHERE affected_installations >= 3)
                    OR EXISTS (SELECT 1 FROM recent_paused WHERE affected_installations >= 3) THEN 'attention'
                   WHEN NOT EXISTS (SELECT 1 FROM covered) THEN 'unavailable' ELSE 'ok' END
);
ROLLBACK;
