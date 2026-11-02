-- Paired observational screen, never proof of a miner performance regression.
BEGIN READ ONLY;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '2s';
WITH reports AS (
    SELECT installation_id, received_at, payload,
           payload->>'miner_version' AS version,
           (payload->'hardware') #- '{resources,available_memory_gib}' AS hardware,
           -- Compare releases within one workload mode. Exact source identity
           -- intentionally differs across releases, unlike absolute-rate cohorts.
           jsonb_build_array(payload->'build_target', payload->'build_features',
                             payload #> '{diagnostics,mode}') AS build,
           CASE WHEN received_at >= now() - interval '36 hours' THEN 'recent' ELSE 'baseline' END AS period
    FROM telemetry_reports
    WHERE received_at >= now() - interval '7 days'
      AND NOT (payload->>'debug_build')::boolean
      AND coalesce(payload #>> '{diagnostics,reason}', 'periodic') = 'periodic'
), samples AS (
    SELECT installation_id, received_at, version, hardware, build, period,
           -- Schema-14 GPU context stays out of cohorts: the kernel source differs
           -- between otherwise identical runs (a first build, then its cache).
           (sample->'settings') - 'gpu_arch' - 'gpu_kernel_source' - 'amd_perf_level' AS settings,
           (sample->>'hashes')::double precision /
             NULLIF((sample->>'observed_secs')::double precision, 0) AS hps
    FROM reports CROSS JOIN LATERAL jsonb_array_elements(payload->'backends') sample
    WHERE NOT (sample->>'settings_changed')::boolean
      AND (sample->>'observed_secs')::double precision >= 60
), per_report AS (
    SELECT installation_id, received_at, version, hardware, build, period, settings,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY hps) AS hps
    FROM samples GROUP BY 1,2,3,4,5,6,7
), per_installation AS (
    SELECT installation_id, version, hardware, build, period, settings,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY hps) AS hps
    FROM per_report GROUP BY 1,2,3,4,5,6
    HAVING count(*) >= 3 AND max(received_at)-min(received_at) >= interval '24 hours'
), pairs AS (
    SELECT r.installation_id, r.version, b.version AS baseline_version,
           r.hardware, r.build, r.settings, r.hps / b.hps AS ratio
    FROM per_installation r JOIN per_installation b
      ON r.installation_id=b.installation_id AND r.hardware=b.hardware
     AND r.build=b.build AND r.settings=b.settings
    WHERE r.period='recent' AND b.period='baseline' AND b.hps > 0
), cohorts AS (
    SELECT version, baseline_version, hardware, build, settings,
           count(*) AS installations,
           count(*) FILTER (WHERE ratio <= 0.8) AS affected_installations,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY ratio) AS median_ratio
    FROM pairs GROUP BY 1,2,3,4,5 HAVING count(*) >= 5
)
SELECT jsonb_build_object(
    'minutes_since_last_report', (SELECT floor(extract(epoch FROM now()-max(received_at))/60) FROM reports),
    'performance_cohorts', coalesce((SELECT jsonb_agg(cohorts) FROM cohorts), '[]'::jsonb)
);
ROLLBACK;
