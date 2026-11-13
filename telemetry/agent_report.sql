BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '30s';
SET LOCAL lock_timeout = '2s';
SET LOCAL jit = off;
SET LOCAL work_mem = '64MB';
SET LOCAL TIME ZONE 'America/New_York';

-- Seven days of narrow derived rows (src/schema.sql). Payloads are read only for
-- each installation's latest report and for one member report of each cohort.
WITH activity AS MATERIALIZED (
    SELECT installation_id, received_at::date AS day, count(*) AS reports,
           max(received_at) AS last_report
    FROM telemetry_report_facts
    WHERE received_at >= now() - interval '7 days' AND NOT debug_build
    GROUP BY 1, 2
), latest_reports AS (
    SELECT installation_id, max(last_report) AS received_at FROM activity GROUP BY 1
), latest AS MATERIALIZED (
    SELECT DISTINCT ON (l.installation_id) l.installation_id, l.received_at,
           t.payload->>'miner_version' AS version,
           t.payload #>> '{hardware,cpu_model}' AS cpu,
           t.payload #>> '{hardware,os}' AS os,
           t.payload->'backends' AS backends
    FROM latest_reports l JOIN telemetry_reports t
      ON t.installation_id = l.installation_id AND t.received_at = l.received_at
    WHERE NOT (t.payload->>'debug_build')::boolean
    ORDER BY l.installation_id
), population AS (
    SELECT cpu, os, count(*) AS installations FROM latest
    GROUP BY 1, 2 ORDER BY installations DESC, cpu, os LIMIT 20
), versions AS (
    SELECT version, count(*) AS installations FROM latest
    GROUP BY 1 ORDER BY installations DESC, version
), gpus AS (
    SELECT sample #>> '{settings,backend}' AS backend,
           coalesce(sample #>> '{settings,device_model}', 'unknown') AS gpu,
           count(DISTINCT installation_id) AS installations
    FROM latest CROSS JOIN LATERAL jsonb_array_elements(backends) AS sample
    WHERE sample #>> '{settings,backend}' IN ('nvidia', 'amd', 'metal')
    GROUP BY 1, 2 ORDER BY installations DESC, backend, gpu LIMIT 20
), daily AS (
    SELECT day, sum(reports)::bigint AS reports, count(*) AS installations
    FROM activity GROUP BY 1 ORDER BY 1
), samples AS (
    -- One entry per report and settings: identical devices in one report must
    -- not inflate independent observations. Shutdown and failure windows are
    -- inventory only.
    SELECT f.installation_id, f.received_at, f.report_id, f.version, f.context_hash,
           s.settings_hash, s.hps
    FROM telemetry_report_facts f
    CROSS JOIN LATERAL unnest(f.settings_hashes, f.hps) AS s(settings_hash, hps)
    WHERE f.received_at >= now() - interval '7 days' AND NOT f.debug_build AND f.periodic
), per_installation AS (
    SELECT installation_id, version, context_hash, settings_hash,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY hps) AS hps,
           min(report_id) AS example_report
    FROM samples GROUP BY 1, 2, 3, 4
    HAVING count(*) >= 3 AND max(received_at) - min(received_at) >= interval '24 hours'
), cohorts AS (
    SELECT version, context_hash, settings_hash, count(*) AS installations,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY hps) AS median_hps,
           percentile_cont(0.1) WITHIN GROUP (ORDER BY hps) AS p10_hps,
           percentile_cont(0.9) WITHIN GROUP (ORDER BY hps) AS p90_hps,
           -- Any member report carries the cohort's keys.
           min(example_report) AS example_report
    FROM per_installation GROUP BY 1, 2, 3
    HAVING count(*) >= 5
), performance AS (
    SELECT c.version, k.cpu, k.os, k.context, k.settings, c.installations,
           c.median_hps, c.p10_hps, c.p90_hps
    FROM cohorts c
    CROSS JOIN LATERAL (
        SELECT t.payload #>> '{hardware,cpu_model}' AS cpu, t.payload #>> '{hardware,os}' AS os,
               telemetry_performance_context(t.payload) AS context,
               telemetry_cohort_settings(sample->'settings') AS settings
        FROM telemetry_reports t
        CROSS JOIN LATERAL jsonb_array_elements(t.payload->'backends') sample
        WHERE t.report_id = c.example_report
          AND jsonb_hash_extended(telemetry_cohort_settings(sample->'settings'), 0)
              IS NOT DISTINCT FROM c.settings_hash
        LIMIT 1) k
    ORDER BY installations DESC, version, cpu, os, context, settings LIMIT 20
)
SELECT jsonb_build_object(
    'window_days', 7,
    'reports', (SELECT coalesce(sum(reports), 0) FROM activity),
    'installations', (SELECT count(*) FROM latest),
    'active_installations_24h', (SELECT count(*) FROM latest WHERE received_at >= now() - interval '24 hours'),
    'minutes_since_last_report', (SELECT floor(extract(epoch FROM now() - max(last_report))/60) FROM activity),
    'versions', coalesce((SELECT jsonb_agg(versions) FROM versions), '[]'::jsonb),
    'cpu_population', coalesce((SELECT jsonb_agg(population) FROM population), '[]'::jsonb),
    'gpu_population', coalesce((SELECT jsonb_agg(gpus) FROM gpus), '[]'::jsonb),
    'daily_activity', coalesce((SELECT jsonb_agg(daily) FROM daily), '[]'::jsonb),
    'performance_candidates', coalesce((SELECT jsonb_agg(performance) FROM performance), '[]'::jsonb)
);
ROLLBACK;
