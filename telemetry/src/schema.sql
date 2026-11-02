CREATE TABLE IF NOT EXISTS telemetry_reports (
    report_id text PRIMARY KEY,
    installation_id text NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    payload jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS telemetry_reports_received ON telemetry_reports (received_at);
CREATE INDEX IF NOT EXISTS telemetry_reports_installation ON telemetry_reports (installation_id, received_at DESC);

-- One latest report per installation prevents uptime from skewing popularity.
CREATE OR REPLACE VIEW telemetry_installations AS
SELECT DISTINCT ON (installation_id) installation_id, received_at, payload
FROM telemetry_reports ORDER BY installation_id, received_at DESC;

CREATE OR REPLACE VIEW telemetry_cpu_population AS
SELECT payload #>> '{hardware,cpu_model}' AS cpu_model,
       payload #>> '{hardware,os}' AS os,
       count(*) AS installations
FROM telemetry_installations
GROUP BY 1, 2;

-- Context separates unlike runtime/resource environments; free RAM is diagnostic only.
-- Append context to the existing view columns so CREATE OR REPLACE migrates in place.
CREATE OR REPLACE VIEW telemetry_backend_performance AS
WITH samples AS (
    SELECT installation_id, report_id, payload->>'miner_version' AS miner_version,
           payload #>> '{hardware,cpu_model}' AS cpu_model,
           payload #>> '{hardware,os}' AS os,
           -- Schema-14 GPU context stays out of cohorts; see release_health.sql.
           (sample->'settings') - 'gpu_arch' - 'gpu_kernel_source' - 'amd_perf_level' AS settings,
           jsonb_build_object(
               'os_version', payload #> '{hardware,os_version}',
               'runtime_environment', payload #> '{hardware,runtime_environment}',
               'memory_gib', payload #> '{hardware,memory_gib}',
               'logical_cpus', payload #> '{hardware,logical_cpus}',
               'physical_cores', payload #> '{hardware,physical_cores}',
               'cpu_vendor', payload #> '{hardware,cpu_vendor}',
               'cpu_family', payload #> '{hardware,cpu_family}',
               'cpu_model_id', payload #> '{hardware,cpu_model_id}',
               'build_target', payload->'build_target',
               'build_features', payload->'build_features',
               'build_commit', payload #> '{diagnostics,build_commit}',
               'source_fingerprint', payload #> '{diagnostics,source_fingerprint}',
               'mode', payload #> '{diagnostics,mode}',
               'resources', NULLIF(payload #> '{hardware,resources}', 'null'::jsonb) - 'available_memory_gib'
           ) AS context,
           (sample->>'hashes')::double precision / NULLIF((sample->>'observed_secs')::double precision, 0) AS hps
    FROM telemetry_reports CROSS JOIN LATERAL jsonb_array_elements(payload->'backends') AS sample
    WHERE NOT (payload->>'debug_build')::boolean
      AND coalesce(payload #>> '{diagnostics,reason}', 'periodic') = 'periodic'
      AND NOT (sample->>'settings_changed')::boolean
      AND (sample->>'observed_secs')::double precision >= 60
), per_report AS (
    -- Multiple identical GPUs in one report are not independent observations.
    SELECT installation_id, report_id, miner_version, cpu_model, os, settings, context,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY hps) AS hps
    FROM samples GROUP BY 1, 2, 3, 4, 5, 6, 7
), per_installation AS (
    SELECT installation_id, miner_version, cpu_model, os, settings, context,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY hps) AS hps
    FROM per_report GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT miner_version, cpu_model, os, settings, count(*) AS installations,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY hps) AS median_hps,
       percentile_cont(0.1) WITHIN GROUP (ORDER BY hps) AS p10_hps,
       percentile_cont(0.9) WITHIN GROUP (ORDER BY hps) AS p90_hps,
       context
FROM per_installation GROUP BY 1, 2, 3, 4, context;
