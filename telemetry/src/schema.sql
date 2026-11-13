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

-- Cohort keys, defined once: the derived rows below store their hashes and the
-- reports read the keys back through the same functions.
-- Free RAM is diagnostic only and must not split a cohort on every report.
CREATE OR REPLACE FUNCTION telemetry_alert_hardware(payload jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT (payload->'hardware') #- '{resources,available_memory_gib}'
$$;

-- Release comparisons stay within one workload mode. Exact source identity
-- intentionally differs across releases, unlike absolute-rate cohorts.
CREATE OR REPLACE FUNCTION telemetry_alert_build(payload jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT jsonb_build_array(payload->'build_target', payload->'build_features',
                             payload #> '{diagnostics,mode}')
$$;

-- Schema-14 GPU context stays out of cohorts: the kernel source differs between
-- otherwise identical runs (a first build, then its cache). See release_health.sql.
CREATE OR REPLACE FUNCTION telemetry_cohort_settings(settings jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT settings - 'gpu_arch' - 'gpu_kernel_source' - 'amd_perf_level'
$$;

-- Context separates unlike runtime/resource environments for absolute rates.
CREATE OR REPLACE FUNCTION telemetry_performance_context(payload jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT jsonb_build_object(
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
    )
$$;

-- One narrow row per report, so scheduled checks and the agent briefing do not
-- decode seven days of payloads on every run. Written by the insert trigger
-- below and removed with its report. Hashes are jsonb_hash_extended(key, 0).
CREATE TABLE IF NOT EXISTS telemetry_report_facts (
    report_id text PRIMARY KEY REFERENCES telemetry_reports ON DELETE CASCADE,
    installation_id text NOT NULL,
    received_at timestamptz NOT NULL,
    debug_build boolean,
    version text,
    periodic boolean NOT NULL,
    has_diagnostics boolean NOT NULL,
    build_commit text,
    settled boolean NOT NULL,
    previous_run_unclean boolean,
    dropped_diagnostics boolean NOT NULL,
    hardware_hash bigint,
    build_hash bigint,
    -- CPU model, OS and telemetry_performance_context together.
    context_hash bigint,
    -- One entry per distinct backend settings among the report's stable samples
    -- (unchanged settings, at least a minute observed), with their median rate.
    -- Identical devices in one report are not independent observations.
    settings_hashes bigint[] NOT NULL,
    hps double precision[] NOT NULL
);
CREATE INDEX IF NOT EXISTS telemetry_report_facts_received ON telemetry_report_facts (received_at);

-- The only definition of the derived row; the trigger and rebuild_facts.sql read it.
CREATE OR REPLACE VIEW telemetry_report_facts_source AS
SELECT r.report_id, r.installation_id, r.received_at,
       (p->>'debug_build')::boolean AS debug_build,
       p->>'miner_version' AS version,
       coalesce(p #>> '{diagnostics,reason}', 'periodic') = 'periodic' AS periodic,
       coalesce(p->'diagnostics' <> 'null'::jsonb, false) AS has_diagnostics,
       p #>> '{diagnostics,build_commit}' AS build_commit,
       p #> '{tuning,settled}' IS NOT NULL AS settled,
       (p #>> '{diagnostics,previous_run_unclean}')::boolean AS previous_run_unclean,
       coalesce((p #>> '{diagnostics,dropped_events}')::bigint > 0
                OR (p #>> '{diagnostics,dropped_backend_samples}')::bigint > 0, false) AS dropped_diagnostics,
       jsonb_hash_extended(telemetry_alert_hardware(p), 0) AS hardware_hash,
       jsonb_hash_extended(telemetry_alert_build(p), 0) AS build_hash,
       jsonb_hash_extended(jsonb_build_array(p #> '{hardware,cpu_model}', p #> '{hardware,os}',
                                             telemetry_performance_context(p)), 0) AS context_hash,
       s.settings_hashes, s.hps
FROM telemetry_reports r
-- Each payload reference would otherwise decompress the stored value again.
CROSS JOIN LATERAL unnest(ARRAY[r.payload || '{}'::jsonb]) p
CROSS JOIN LATERAL (
    SELECT coalesce(array_agg(g.settings_hash ORDER BY g.position), '{}') AS settings_hashes,
           coalesce(array_agg(g.hps ORDER BY g.position), '{}') AS hps
    FROM (
        SELECT jsonb_hash_extended(telemetry_cohort_settings(sample->'settings'), 0) AS settings_hash,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY (sample->>'hashes')::double precision /
                   NULLIF((sample->>'observed_secs')::double precision, 0)) AS hps,
               min(position) AS position
        FROM jsonb_array_elements(p->'backends') WITH ORDINALITY AS b(sample, position)
        WHERE NOT (sample->>'settings_changed')::boolean
          AND (sample->>'observed_secs')::double precision >= 60
        GROUP BY 1
    ) g
) s;

CREATE OR REPLACE FUNCTION telemetry_report_facts_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    -- A derived row must never reject a report. check.sh reports any gap.
    BEGIN
        INSERT INTO telemetry_report_facts (
            report_id, installation_id, received_at, debug_build, version, periodic,
            has_diagnostics, build_commit, settled, previous_run_unclean,
            dropped_diagnostics, hardware_hash, build_hash,
            context_hash, settings_hashes, hps)
        SELECT report_id, installation_id, received_at, debug_build, version, periodic,
               has_diagnostics, build_commit, settled, previous_run_unclean,
            dropped_diagnostics, hardware_hash, build_hash,
               context_hash, settings_hashes, hps
        FROM telemetry_report_facts_source WHERE report_id = NEW.report_id;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'telemetry_report_facts skipped one report: %', SQLSTATE;
    END;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER telemetry_report_facts_insert
AFTER INSERT ON telemetry_reports
FOR EACH ROW EXECUTE FUNCTION telemetry_report_facts_insert();

-- Absolute rates by exact build, environment, resources and backend settings.
-- Append context to the existing view columns so CREATE OR REPLACE migrates in place.
CREATE OR REPLACE VIEW telemetry_backend_performance AS
WITH samples AS (
    SELECT installation_id, report_id, payload->>'miner_version' AS miner_version,
           payload #>> '{hardware,cpu_model}' AS cpu_model,
           payload #>> '{hardware,os}' AS os,
           telemetry_cohort_settings(sample->'settings') AS settings,
           telemetry_performance_context(payload) AS context,
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
