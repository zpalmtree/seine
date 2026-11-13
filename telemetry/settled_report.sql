BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '2s';
SET LOCAL jit = off;
SET LOCAL work_mem = '64MB';
SET LOCAL effective_io_concurrency = 32;

-- What the tuner settled on, per CPU identity: the source for the built-in start table.
-- The narrow derived rows (src/schema.sql) pick each installation's latest report
-- carrying a settled record; only those payloads are read.
WITH latest_reports AS (
    SELECT DISTINCT ON (installation_id) installation_id, report_id
    FROM telemetry_report_facts
    WHERE received_at >= now() - interval '7 days' AND NOT debug_build AND settled
    ORDER BY installation_id, received_at DESC
), latest AS (
    SELECT t.installation_id, t.payload ->> 'miner_version' AS miner_version,
           t.payload -> 'hardware' AS hardware,
           t.payload #> '{tuning,settled}' AS settled
    FROM telemetry_reports t
    -- One list of keys, so the reports are fetched in storage order.
    WHERE t.report_id = ANY (ARRAY(SELECT report_id FROM latest_reports))
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
