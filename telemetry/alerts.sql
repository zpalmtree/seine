-- Paired observational screen, never proof of a miner performance regression.
BEGIN READ ONLY;
SET LOCAL statement_timeout = '30s';
SET LOCAL lock_timeout = '2s';
-- Row estimates for the jsonb expansions are far too high; keep the scan parallel.
SET LOCAL parallel_tuple_cost = 0.01;
-- One parallel pass keeps only narrow columns: cohorts compare hashes of their
-- jsonb keys, and the keys of the few reported cohorts are read back at the end.
WITH samples AS MATERIALIZED (
    SELECT installation_id, received_at, p->>'miner_version' AS version,
           CASE WHEN received_at >= now() - interval '36 hours' THEN 'recent' ELSE 'baseline' END AS period,
           ARRAY[jsonb_hash_extended(cohort.hardware, 0), jsonb_hash_extended(cohort.build, 0),
                 jsonb_hash_extended(cohort.settings, 0)] AS cohort,
           (sample->>'hashes')::double precision /
             NULLIF((sample->>'observed_secs')::double precision, 0) AS hps
    FROM telemetry_reports
    -- Each payload reference would otherwise decompress the stored value again.
    CROSS JOIN LATERAL unnest(ARRAY[payload || '{}'::jsonb]) p
    CROSS JOIN LATERAL jsonb_array_elements(p->'backends') sample
    CROSS JOIN LATERAL (
        -- Keep in step with the read-back in cohorts below.
        SELECT (p->'hardware') #- '{resources,available_memory_gib}' AS hardware,
               -- Compare releases within one workload mode. Exact source identity
               -- intentionally differs across releases, unlike absolute-rate cohorts.
               jsonb_build_array(p->'build_target', p->'build_features',
                                 p #> '{diagnostics,mode}') AS build,
               -- Schema-14 GPU context stays out of cohorts: the kernel source differs
               -- between otherwise identical runs (a first build, then its cache).
               (sample->'settings') - 'gpu_arch' - 'gpu_kernel_source' - 'amd_perf_level' AS settings) cohort
    WHERE received_at >= now() - interval '7 days'
      AND NOT (p->>'debug_build')::boolean
      AND coalesce(p #>> '{diagnostics,reason}', 'periodic') = 'periodic'
      AND NOT (sample->>'settings_changed')::boolean
      AND (sample->>'observed_secs')::double precision >= 60
), per_report AS (
    SELECT installation_id, received_at, version, period, cohort,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY hps) AS hps
    FROM samples GROUP BY 1,2,3,4,5
), per_installation AS (
    SELECT installation_id, version, period, cohort, max(received_at) AS last_report,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY hps) AS hps
    FROM per_report GROUP BY 1,2,3,4
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
    FROM pairs GROUP BY 1,2,3 HAVING count(*) >= 5
), cohorts AS (
    SELECT s.version, s.baseline_version, k.hardware, k.build, k.settings,
           s.installations, s.affected_installations, s.median_ratio
    FROM cohort_stats s
    CROSS JOIN LATERAL (
        SELECT c.* FROM telemetry_reports t
        CROSS JOIN LATERAL jsonb_array_elements(t.payload->'backends') sample
        CROSS JOIN LATERAL (
        -- Same keys as samples above.
        SELECT (t.payload->'hardware') #- '{resources,available_memory_gib}' AS hardware,
               jsonb_build_array(t.payload->'build_target', t.payload->'build_features',
                                 t.payload #> '{diagnostics,mode}') AS build,
               (sample->'settings') - 'gpu_arch' - 'gpu_kernel_source' - 'amd_perf_level' AS settings) c
        WHERE t.installation_id = s.example_installation AND t.received_at = s.example_report
          AND ARRAY[jsonb_hash_extended(c.hardware, 0), jsonb_hash_extended(c.build, 0),
                    jsonb_hash_extended(c.settings, 0)] = s.cohort
        LIMIT 1) k
)
SELECT jsonb_build_object(
    'minutes_since_last_report', (
        SELECT floor(extract(epoch FROM now()-max(received_at))/60) FROM telemetry_reports
        WHERE received_at >= now() - interval '7 days'
          AND NOT (payload->>'debug_build')::boolean
          AND coalesce(payload #>> '{diagnostics,reason}', 'periodic') = 'periodic'),
    'performance_cohorts', coalesce((SELECT jsonb_agg(cohorts) FROM cohorts), '[]'::jsonb)
);
ROLLBACK;
