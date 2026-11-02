BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '2s';

-- What the tuner settled on, per CPU identity: the source for the built-in start table.
WITH decoded AS MATERIALIZED (
    SELECT installation_id, received_at, p.*
    FROM telemetry_reports
    CROSS JOIN LATERAL jsonb_to_record(payload) AS p(
        debug_build boolean, miner_version text, hardware jsonb, tuning jsonb
    )
    WHERE received_at >= now() - interval '7 days' AND NOT p.debug_build
      AND p.tuning -> 'settled' IS NOT NULL
), latest AS (
    SELECT DISTINCT ON (installation_id) installation_id, miner_version, hardware,
           tuning -> 'settled' AS settled
    FROM decoded
    ORDER BY installation_id, received_at DESC
), rows AS (
    SELECT hardware ->> 'cpu_vendor' AS vendor,
           (hardware ->> 'cpu_family')::int AS family,
           (hardware ->> 'cpu_model_id')::int AS model,
           hardware ->> 'cpu_model' AS cpu,
           hardware ->> 'os' AS os,
           hardware ->> 'runtime_environment' AS environment,
           (settled ->> 'workers')::int AS workers,
           settled ->> 'kernel' AS kernel,
           (settled ->> 'settled_ms')::bigint AS settled_ms,
           (settled ->> 'comparisons')::int AS comparisons,
           (settled ->> 'ram_limited')::boolean AS ram_limited,
           settled ->> 'seed' AS seed,
           least((hardware #>> '{resources,allowed_logical_cpus}')::int,
                 greatest(1, (((hardware #>> '{resources,effective_memory_gib}')::float8 - 0.5) / 2)::int)
           ) AS room
    FROM latest
), cohorts AS (
    SELECT vendor, family, model, min(cpu) AS cpu, count(*) AS installations,
           mode() WITHIN GROUP (ORDER BY kernel) AS kernel,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY workers::float8 / NULLIF(room, 0)) AS share_of_room,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY settled_ms) AS settled_ms_p50,
           percentile_cont(0.9) WITHIN GROUP (ORDER BY settled_ms) AS settled_ms_p90,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY comparisons) AS comparisons_p50,
           avg((ram_limited)::int) AS ram_limited_share
    FROM rows GROUP BY vendor, family, model
)
SELECT jsonb_build_object(
    'status', 'ok',
    'window_days', 7,
    'installations', (SELECT count(*) FROM rows),
    'cohorts', coalesce((SELECT jsonb_agg(c ORDER BY c.installations DESC) FROM cohorts c WHERE c.installations >= 5), '[]'::jsonb)
);
ROLLBACK;
