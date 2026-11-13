BEGIN READ ONLY;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '2s';
-- Row estimates for the jsonb expansions are far too high, and the planner would
-- keep a six-hour scan on one process; decode payloads in parallel.
SET LOCAL parallel_tuple_cost = 0.01;
SET LOCAL parallel_setup_cost = 0;
SET LOCAL min_parallel_index_scan_size = 0;
SET LOCAL min_parallel_table_scan_size = 0;
SET LOCAL enable_memoize = off;
SET LOCAL jit = off;
WITH reports AS (
    SELECT installation_id, received_at, p->>'miner_version' AS version,
           p #>> '{hardware,cpu_model}' AS cpu,
           p #>> '{hardware,os}' AS os,
           p #>> '{diagnostics,phase}' AS phase,
           -- Path queries are plain function calls and run in the parallel workers;
           -- a sub-select per report would run on one process.
           jsonb_path_exists(p, '$.diagnostics.events[*] ? (@.detail.operation == $operations[*])',
               '{"operations": ["pool_resolve", "pool_connect", "pool_read", "pool_write", "pool_login"]}') AS connection_warning,
           jsonb_array_length(p->'backends') > 0 AS has_samples,
           jsonb_path_exists(p, '$.backends[*] ? (@.settings_changed == false && @.observed_secs >= 60)') AS has_stable_samples
    FROM telemetry_reports
    -- Each payload reference would otherwise decompress the stored value again.
    CROSS JOIN LATERAL unnest(ARRAY[payload || '{}'::jsonb]) p
    WHERE received_at >= now() - interval '6 hours'
      AND NOT (p->>'debug_build')::boolean
      AND coalesce(p #>> '{diagnostics,reason}', 'periodic') = 'periodic'
), installations AS (
    SELECT installation_id, version, count(*) AS reports,
           bool_or(has_samples) AS has_samples,
           max(received_at) - min(received_at) AS span
    FROM reports GROUP BY 1, 2
), release_lines AS (
    -- A release line is major.minor. Release builds report a v-prefixed tag;
    -- source builds and commits never set the newest line.
    SELECT DISTINCT version,
           (regexp_match(version, '^v?(\d+)\.(\d+)\.\d+$'))[1]::int AS major,
           (regexp_match(version, '^v?(\d+)\.(\d+)\.\d+$'))[2]::int AS minor,
           version ~ '^v\d+\.\d+\.\d+$' AS release_build
    FROM installations
), newest_line AS (
    SELECT major, minor FROM release_lines WHERE release_build
    ORDER BY major DESC, minor DESC LIMIT 1
), legacy_versions AS (
    -- Releases before the newest line can only be fixed by upgrading, so they
    -- are reported but never fail the check. Unparsed versions stay current.
    SELECT l.version FROM release_lines l, newest_line n
    WHERE l.major IS NOT NULL AND (l.major, l.minor) < (n.major, n.minor)
), inventory_only AS (
    SELECT * FROM installations
    WHERE reports >= 3 AND span >= interval '2 hours' AND NOT has_samples
), inventory_context AS (
    SELECT r.installation_id, r.version, r.cpu, r.os,
           bool_or(r.connection_warning) AS connection_warning,
           bool_or(r.phase = 'mining') AS observed_mining_phase
    FROM reports r JOIN inventory_only i USING (installation_id, version)
    GROUP BY 1,2,3,4
), inventory_clusters AS (
    SELECT version, cpu, os, connection_warning, observed_mining_phase,
           version IN (SELECT version FROM legacy_versions) AS legacy,
           count(DISTINCT installation_id) AS installations
    FROM inventory_context GROUP BY 1,2,3,4,5,6
    ORDER BY installations DESC, version, cpu, os LIMIT 30
), versions AS (
    SELECT version, count(*) AS installations,
           count(*) FILTER (WHERE reports >= 3 AND span >= interval '2 hours'
                            AND NOT has_samples) AS sustained_inventory_only_installations,
           version IN (SELECT version FROM legacy_versions) AS legacy
    FROM installations GROUP BY 1
)
SELECT jsonb_build_object(
    'window_hours', 6,
    'reports', (SELECT count(*) FROM reports),
    'reports_with_samples', (SELECT count(*) FROM reports WHERE has_samples),
    'reports_with_stable_samples', (SELECT count(*) FROM reports WHERE has_stable_samples),
    'inventory_only_reports', (SELECT count(*) FROM reports WHERE NOT has_samples),
    'inventory_only_clusters', coalesce((SELECT jsonb_agg(inventory_clusters) FROM inventory_clusters), '[]'::jsonb),
    'versions', coalesce((SELECT jsonb_agg(versions ORDER BY version) FROM versions), '[]'::jsonb),
    'newest_release_line', (SELECT format('v%s.%s', major, minor) FROM newest_line),
    'status', CASE WHEN EXISTS (SELECT 1 FROM versions
                                WHERE sustained_inventory_only_installations >= 5 AND NOT legacy)
                   THEN 'error' ELSE 'ok' END
);
ROLLBACK;
