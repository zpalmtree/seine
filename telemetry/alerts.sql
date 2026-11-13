-- Paired observational screen, never proof of a miner performance regression.
BEGIN READ ONLY;
SET LOCAL statement_timeout = '30s';
SET LOCAL lock_timeout = '2s';
SET LOCAL jit = off;
SET LOCAL work_mem = '64MB';
-- Reads the narrow derived rows (src/schema.sql): cohorts compare hashes of their
-- jsonb keys, and the keys are read back from one member report at the end.
WITH samples AS (
    SELECT f.installation_id, f.received_at, f.version,
           CASE WHEN f.received_at >= now() - interval '36 hours' THEN 'recent' ELSE 'baseline' END AS period,
           ARRAY[f.hardware_hash, f.build_hash, s.settings_hash] AS cohort, s.hps
    FROM telemetry_report_facts f
    -- One entry per report and settings: identical devices are one observation.
    CROSS JOIN LATERAL unnest(f.settings_hashes, f.hps) AS s(settings_hash, hps)
    WHERE f.received_at >= now() - interval '7 days'
      AND NOT f.debug_build AND f.periodic
), per_installation AS (
    SELECT installation_id, version, period, cohort, max(received_at) AS last_report,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY hps) AS hps
    FROM samples GROUP BY 1,2,3,4
    HAVING count(*) >= 3 AND max(received_at)-min(received_at) >= interval '24 hours'
), pairs AS (
    SELECT r.installation_id, r.last_report, r.version, b.version AS baseline_version,
           r.cohort, r.hps / b.hps AS ratio
    FROM per_installation r JOIN per_installation b
      ON r.installation_id=b.installation_id AND r.cohort=b.cohort
    WHERE r.period='recent' AND b.period='baseline' AND b.hps > 0
), cohort_stats AS (
    SELECT version, baseline_version, cohort,
           count(*) AS installations,
           count(*) FILTER (WHERE ratio <= 0.8) AS affected_installations,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY ratio) AS median_ratio,
           -- Any member report carries the cohort's keys.
           (array_agg(installation_id ORDER BY installation_id))[1] AS example_installation,
           (array_agg(last_report ORDER BY installation_id))[1] AS example_report
    FROM pairs GROUP BY 1,2,3
), keyed AS (
    -- Small cohorts are read back too: they count toward their model below.
    SELECT s.*, k.hardware, k.build, k.settings
    FROM cohort_stats s
    CROSS JOIN LATERAL (
        SELECT telemetry_alert_hardware(t.payload) AS hardware,
               telemetry_alert_build(t.payload) AS build,
               telemetry_cohort_settings(sample->'settings') AS settings
        FROM telemetry_reports t
        CROSS JOIN LATERAL jsonb_array_elements(t.payload->'backends') sample
        WHERE t.installation_id = s.example_installation AND t.received_at = s.example_report
          AND ARRAY[jsonb_hash_extended(telemetry_alert_hardware(t.payload), 0),
                    jsonb_hash_extended(telemetry_alert_build(t.payload), 0),
                    jsonb_hash_extended(telemetry_cohort_settings(sample->'settings'), 0)] = s.cohort
        LIMIT 1) k
), models AS (
    -- Every paired installation of the same CPU and device on the same backend,
    -- whatever its resources or settings: a small cohort can drift on its own.
    SELECT p.version, p.baseline_version, k.hardware->>'cpu_model' AS cpu_model,
           k.settings->>'backend' AS backend, k.settings->>'device_model' AS device_model,
           count(DISTINCT p.installation_id) AS installations,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY p.ratio) AS median_ratio
    FROM pairs p JOIN keyed k
      ON k.version IS NOT DISTINCT FROM p.version
     AND k.baseline_version IS NOT DISTINCT FROM p.baseline_version AND k.cohort = p.cohort
    GROUP BY 1,2,3,4,5
), cohorts AS (
    SELECT k.version, k.baseline_version, k.hardware, k.build, k.settings,
           k.installations, k.affected_installations, k.median_ratio,
           m.installations AS model_installations, m.median_ratio AS model_median_ratio
    FROM keyed k JOIN models m
      ON m.version IS NOT DISTINCT FROM k.version
     AND m.baseline_version IS NOT DISTINCT FROM k.baseline_version
     AND m.cpu_model IS NOT DISTINCT FROM k.hardware->>'cpu_model'
     AND m.backend IS NOT DISTINCT FROM k.settings->>'backend'
     AND m.device_model IS NOT DISTINCT FROM k.settings->>'device_model'
    WHERE k.installations >= 5
)
SELECT jsonb_build_object(
    'minutes_since_last_report', (
        SELECT floor(extract(epoch FROM now()-max(received_at))/60) FROM telemetry_report_facts
        WHERE received_at >= now() - interval '7 days'
          AND NOT debug_build AND periodic),
    'performance_cohorts', coalesce((SELECT jsonb_agg(cohorts) FROM cohorts), '[]'::jsonb)
);
ROLLBACK;
