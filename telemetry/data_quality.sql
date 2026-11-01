BEGIN READ ONLY;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '2s';
WITH reports AS (
    SELECT installation_id, received_at, payload->>'miner_version' AS version,
           payload #>> '{hardware,cpu_model}' AS cpu,
           payload #>> '{hardware,os}' AS os,
           payload #>> '{diagnostics,phase}' AS phase,
           EXISTS (SELECT 1 FROM jsonb_array_elements(payload #> '{diagnostics,events}') e
                   WHERE e #>> '{detail,operation}' IN
                       ('pool_resolve','pool_connect','pool_read','pool_write','pool_login')) AS connection_warning,
           jsonb_array_length(payload->'backends') > 0 AS has_samples,
           EXISTS (SELECT 1 FROM jsonb_array_elements(payload->'backends') b
                   WHERE NOT (b->>'settings_changed')::boolean
                     AND (b->>'observed_secs')::double precision >= 60) AS has_stable_samples
    FROM telemetry_reports
    WHERE received_at >= now() - interval '6 hours'
      AND NOT (payload->>'debug_build')::boolean
      AND coalesce(payload #>> '{diagnostics,reason}', 'periodic') = 'periodic'
), installations AS (
    SELECT installation_id, version, count(*) AS reports,
           bool_or(has_samples) AS has_samples,
           max(received_at) - min(received_at) AS span
    FROM reports GROUP BY 1, 2
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
           count(DISTINCT installation_id) AS installations
    FROM inventory_context GROUP BY 1,2,3,4,5
    ORDER BY installations DESC, version, cpu, os LIMIT 30
), versions AS (
    SELECT version, count(*) AS installations,
           count(*) FILTER (WHERE reports >= 3 AND span >= interval '2 hours'
                            AND NOT has_samples) AS sustained_inventory_only_installations
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
    'status', CASE WHEN EXISTS (SELECT 1 FROM versions WHERE sustained_inventory_only_installations >= 5)
                   THEN 'error' ELSE 'ok' END
);
ROLLBACK;
