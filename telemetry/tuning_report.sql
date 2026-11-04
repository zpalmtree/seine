-- Paired CPU trial screening. No 24-hour residence requirement; no automatic defaults.
BEGIN READ ONLY;
SET LOCAL statement_timeout = '45s';
SET LOCAL lock_timeout = '2s';
-- JSON array estimates overstate trial expansion and understate qualification.
-- Keep relational joins from rescanning the trial sets; lateral JSON expansion
-- still uses nested loops. All settings end with this read-only transaction.
SET LOCAL jit = off;
SET LOCAL enable_nestloop = off;
SET LOCAL work_mem = '64MB';
SET LOCAL TIME ZONE 'America/New_York';
WITH reports AS MATERIALIZED (
    SELECT installation_id, received_at, p.*
    FROM telemetry_reports
    CROSS JOIN LATERAL jsonb_to_record(payload) AS p(
        debug_build boolean, miner_version text, hardware jsonb, diagnostics jsonb,
        build_target text, build_features text, tuning jsonb
    )
    WHERE received_at >= now() - interval '7 days' AND NOT p.debug_build
      AND p.tuning IS NOT NULL AND p.tuning <> 'null'::jsonb
), raw AS (
    SELECT installation_id, received_at,
           jsonb_build_object('version', miner_version,
               'build_commit', diagnostics->'build_commit') AS source_build,
           tuning->>'run_id' AS run_id,
           (trial->>'trial_id')::bigint AS trial_id, trial->'outcome' AS outcome,
           jsonb_build_object(
               'hardware', hardware - 'resources',
               'build_target', build_target, 'build_features', build_features,
               'source_fingerprint', diagnostics->'source_fingerprint',
               'mode', diagnostics->'mode'
           ) AS environment
    FROM reports CROSS JOIN LATERAL jsonb_array_elements(tuning->'trials') trial
), identities AS (
    SELECT installation_id, run_id, trial_id,
           count(DISTINCT jsonb_build_array(outcome, environment)) AS variants
    FROM raw GROUP BY 1,2,3
), trials AS (
    -- Conflicting replays are excluded entirely, not resolved by choosing a winner.
    -- Earliest receipt preserves freshness; replay cannot make an old trial new.
    SELECT DISTINCT ON (r.installation_id, r.run_id, r.trial_id) r.*,
           jsonb_build_object('environment', environment, 'kind', outcome->'kind',
               'policy', outcome->'policy', 'baseline', outcome->'baseline',
               'candidate', outcome->'candidate',
               'resources', (outcome #> '{context_before,resources}') - 'available_memory_mib'
           ) AS context
    FROM raw r JOIN identities i USING (installation_id, run_id, trial_id)
    WHERE i.variants = 1
    ORDER BY r.installation_id, r.run_id, r.trial_id, received_at
), windows AS (
    SELECT installation_id, run_id, trial_id, position,
           (outcome #>> '{policy,required_gain_bps}')::double precision / 10000 AS required_gain_ratio,
           outcome #>> '{policy,quantization}' AS quantization,
           (sample->>'hashes')::bigint AS hashes,
           (sample->>'elapsed_ms')::bigint AS elapsed_ms,
           (sample->>'hashes')::double precision / NULLIF((sample->>'elapsed_ms')::double precision, 0) AS rate,
           ((sample->>'hashes')::bigint >= greatest(1, (outcome #>> '{policy,minimum_hashes}')::bigint)
            AND (sample->>'elapsed_ms')::bigint >= (outcome #>> '{policy,minimum_sample_ms}')::bigint
            OR coalesce(
                (sample->>'hashes')::bigint >= (outcome #>> '{policy,short_minimum_hashes}')::bigint
                AND (sample->>'elapsed_ms')::bigint >= (outcome #>> '{policy,short_sample_ms}')::bigint,
                false)) AS sufficient,
           (NOT (sample->>'changed')::boolean
            AND sample->>'arm' = CASE WHEN position % 3 = 2 THEN 'candidate' ELSE 'baseline' END
            AND sample #> '{context,members}' = CASE WHEN position % 3 = 2 THEN outcome->'candidate' ELSE outcome->'baseline' END
            AND (sample #> '{context,resources}') - 'available_memory_mib'
                = (outcome #> '{context_before,resources}') - 'available_memory_mib') AS stable
    FROM trials t CROSS JOIN LATERAL jsonb_array_elements(outcome->'samples') WITH ORDINALITY s(sample, position)
), health AS MATERIALIZED (
    SELECT installation_id, run_id, trial_id, count(*) AS windows,
           bool_and(stable) AS stable,
           bool_and(sufficient) AS sufficient
    FROM windows GROUP BY 1,2,3
), adjacent AS (
    SELECT installation_id, run_id, trial_id, required_gain_ratio, quantization, position, hashes, elapsed_ms, rate,
           lead(rate) OVER trial_window AS candidate_rate,
           lead(elapsed_ms) OVER trial_window AS candidate_elapsed_ms,
           lead(hashes, 2) OVER trial_window AS after_hashes,
           lead(elapsed_ms, 2) OVER trial_window AS after_elapsed_ms,
           lead(rate, 2) OVER trial_window AS after_rate
    FROM windows
    WINDOW trial_window AS (PARTITION BY installation_id, run_id, trial_id ORDER BY position)
), cycles AS (
    SELECT installation_id, run_id, trial_id,
           required_gain_ratio,
           candidate_rate / NULLIF((hashes + after_hashes)::double precision / (elapsed_ms + after_elapsed_ms), 0) AS ratio,
           abs(after_rate / NULLIF(rate, 0) - 1) AS drift,
           CASE WHEN quantization = 'three_window_hash'
                THEN (1.0/elapsed_ms + 1.0/candidate_elapsed_ms + 1.0/after_elapsed_ms)
                    / NULLIF((hashes + after_hashes)::double precision / (elapsed_ms + after_elapsed_ms), 0)
                ELSE 0 END AS quantization_ratio
    FROM adjacent WHERE position % 3 = 1 AND after_elapsed_ms IS NOT NULL
), measured AS (
    SELECT installation_id, run_id, trial_id, count(*) AS cycles,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY ratio) AS ratio,
           min(ratio) AS minimum_cycle_ratio, max(ratio) AS maximum_cycle_ratio,
           min(ratio - 1 - quantization_ratio - required_gain_ratio) AS minimum_gain_above_resolution,
           max(drift) AS maximum_baseline_drift, max(quantization_ratio) AS maximum_quantization_ratio
    FROM cycles GROUP BY 1,2,3
), qualified AS (
    SELECT t.installation_id, t.received_at, t.source_build, t.context,
           outcome - 'samples' - 'context_before' - 'context_after' - 'baseline' - 'candidate' - 'policy' AS outcome,
           m.ratio, m.minimum_cycle_ratio, m.maximum_cycle_ratio, m.maximum_baseline_drift, m.maximum_quantization_ratio,
           (outcome->>'decision' = 'gain' AND outcome->>'applied' = 'candidate' AND m.cycles = 2
            AND m.minimum_gain_above_resolution > 0) AS confirmed_gain
    FROM trials t JOIN health h USING (installation_id, run_id, trial_id)
    JOIN measured m USING (installation_id, run_id, trial_id)
    WHERE outcome->>'decision' IN ('gain','no_confirmed_gain')
      -- Worker/joint objective and producer contracts are still staged. Preserve
      -- their coverage, but never mix an efficiency policy into throughput hints.
      AND outcome->>'kind' = 'cpu_kernel'
      AND outcome->>'reason' = 'completed'
      AND h.windows IN (3,6) AND m.cycles * 3 = h.windows AND h.stable
      AND h.sufficient
      AND m.maximum_baseline_drift <= (outcome #>> '{policy,maximum_baseline_drift_bps}')::double precision / 10000
      AND m.ratio IS NOT NULL AND m.ratio > 0
      AND outcome->'baseline' <> outcome->'candidate'
      AND outcome #> '{context_before,members}' = outcome->'baseline'
      AND outcome #> '{context_after,members}' = CASE outcome->>'applied'
          WHEN 'candidate' THEN outcome->'candidate' WHEN 'baseline' THEN outcome->'baseline' END
      AND (outcome #> '{context_before,resources}') - 'available_memory_mib'
          = (outcome #> '{context_after,resources}') - 'available_memory_mib'
      AND outcome #>> '{context_before,resources,placement_fingerprint}' IS NOT NULL
      AND outcome #>> '{context_before,resources,allowed_logical_cpus}' IS NOT NULL
      AND outcome #>> '{context_before,resources,available_parallelism}' IS NOT NULL
      AND outcome #>> '{context_before,resources,effective_memory_mib}' IS NOT NULL
      AND environment->>'source_fingerprint' IS NOT NULL
      AND environment->>'source_fingerprint' <> 'unknown'
      AND environment->>'mode' IN ('pool','daemon')
), observations AS (
    -- Costs and outcome denominators include failed/inconclusive trials too.
    -- One installation contributes one cost median, regardless of report volume.
    SELECT context, installation_id, count(*) AS observed_trials,
           count(*) FILTER (WHERE outcome->>'decision' = 'aborted') AS aborted_trials,
           count(*) FILTER (WHERE outcome->>'decision' = 'inconclusive') AS inconclusive_trials,
           count(*) FILTER (WHERE outcome->>'decision' = 'no_confirmed_gain') AS no_confirmed_gain_trials,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY
               (outcome->>'finished_ms')::bigint - (outcome->>'started_ms')::bigint) AS median_wall_ms,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY (outcome #>> '{cost,idle_ms}')::bigint) AS median_idle_ms,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY (outcome #>> '{cost,switching_ms}')::bigint) AS median_switching_ms,
           count(*) FILTER (WHERE outcome #>> '{cost,idle_ms}' IS NOT NULL) AS trials_with_idle_measurement,
           count(*) FILTER (WHERE outcome #>> '{cost,switching_ms}' IS NOT NULL) AS trials_with_switch_measurement
    FROM trials GROUP BY 1,2
), coverage AS (
    SELECT context, count(*) AS observed_installations, sum(observed_trials) AS observed_trials,
           sum(aborted_trials) AS aborted_trials, sum(inconclusive_trials) AS inconclusive_trials,
           sum(no_confirmed_gain_trials) AS no_confirmed_gain_trials,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY median_wall_ms) AS median_wall_ms,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY median_idle_ms) AS median_idle_ms,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY median_switching_ms) AS median_switching_ms,
           sum(trials_with_idle_measurement) AS trials_with_idle_measurement,
           sum(trials_with_switch_measurement) AS trials_with_switch_measurement
    FROM observations GROUP BY 1
), installations AS (
    SELECT context, installation_id, count(*) AS trials,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY ratio) AS ratio,
           min(minimum_cycle_ratio) AS minimum_cycle_ratio,
           max(maximum_cycle_ratio) AS maximum_cycle_ratio,
           max(maximum_baseline_drift) AS maximum_baseline_drift,
           max(maximum_quantization_ratio) AS maximum_quantization_ratio,
           count(*) FILTER (WHERE confirmed_gain) AS confirmed_gain_trials,
           max(received_at) FILTER (WHERE confirmed_gain) AS last_confirmed_gain_received,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY
               (outcome->>'finished_ms')::bigint - (outcome->>'started_ms')::bigint) AS median_wall_ms,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY (outcome #>> '{cost,idle_ms}')::bigint) AS median_idle_ms,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY (outcome #>> '{cost,switching_ms}')::bigint) AS median_switching_ms,
           count(*) FILTER (WHERE outcome #>> '{cost,idle_ms}' IS NOT NULL) AS trials_with_idle_measurement,
           count(*) FILTER (WHERE outcome #>> '{cost,switching_ms}' IS NOT NULL) AS trials_with_switch_measurement,
           min(received_at) AS first_received, max(received_at) AS last_received
    FROM qualified GROUP BY 1,2
), cohorts AS (
    SELECT i.context, count(*) AS installations, sum(trials) AS qualified_trials,
           count(*) FILTER (WHERE ratio > 1) AS installations_with_positive_ratio,
           count(*) FILTER (WHERE confirmed_gain_trials * 2 > o.observed_trials) AS installations_with_resolved_gain,
           max(last_confirmed_gain_received) AS last_confirmed_gain_received,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY ratio) AS median_ratio,
           percentile_cont(0.1) WITHIN GROUP (ORDER BY ratio) AS p10_ratio,
           percentile_cont(0.9) WITHIN GROUP (ORDER BY ratio) AS p90_ratio,
           min(minimum_cycle_ratio) AS minimum_cycle_ratio, max(maximum_cycle_ratio) AS maximum_cycle_ratio,
           max(maximum_baseline_drift) AS maximum_baseline_drift,
           max(maximum_quantization_ratio) AS maximum_quantization_ratio,
           sum(i.trials_with_idle_measurement) AS trials_with_idle_measurement,
           sum(i.trials_with_switch_measurement) AS trials_with_switch_measurement,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY i.median_wall_ms) AS median_wall_ms,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY i.median_idle_ms) AS median_idle_ms,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY i.median_switching_ms) AS median_switching_ms,
           min(first_received) AS first_received, max(last_received) AS last_received
    FROM installations i JOIN observations o USING (context, installation_id) GROUP BY 1
), provenance AS (
    SELECT context, jsonb_agg(DISTINCT source_build) AS provenance
    FROM qualified GROUP BY context
), hints AS (
    SELECT c.*, false AS automatic_default_eligible, 'requires_native_validation' AS status,
           'throughput' AS objective,
           CASE WHEN median_ratio > 1
                 AND installations_with_resolved_gain >= ceil(v.observed_installations * .6)
                 AND last_confirmed_gain_received >= now() - interval '48 hours'
                THEN 'observational_support' ELSE 'mixed_or_limited_evidence' END AS confidence,
           'Descriptive screening, not a probability or confidence interval; installations are not verified independent hosts.' AS confidence_interpretation,
           CASE WHEN last_received >= now() - interval '48 hours'
                THEN 'latest_qualified_trial_within_48h' ELSE 'latest_qualified_trial_older_than_48h' END AS freshness,
           (to_jsonb(v) - 'context') || jsonb_build_object(
               'unqualified_trials', v.observed_trials - c.qualified_trials,
               'cost_scope', 'all_outcomes_installation_weighted') AS evidence_coverage,
           p.provenance
    FROM cohorts c JOIN coverage v USING (context) JOIN provenance p USING (context)
    WHERE installations >= 5
), outcomes AS (
    SELECT outcome->>'kind' AS kind, outcome->>'decision' AS decision,
           outcome->>'reason' AS reason, count(*) AS trials,
           count(DISTINCT installation_id) AS installations
    FROM trials GROUP BY 1,2,3
)
SELECT jsonb_build_object(
    'window_days', 7, 'status', CASE WHEN EXISTS (SELECT 1 FROM trials) THEN 'ok' ELSE 'unavailable' END,
    'reporting_installations', (SELECT count(DISTINCT installation_id) FROM reports),
    'unique_trials', (SELECT count(*) FROM trials),
    'qualified_trials', (SELECT count(*) FROM qualified),
    'coverage_only_trials', (SELECT count(*) FROM trials) - (SELECT count(*) FROM qualified),
    'missing_context_trials', (SELECT count(*) FROM trials WHERE
        outcome #>> '{context_before,resources,allowed_logical_cpus}' IS NULL
        OR outcome #>> '{context_before,resources,placement_fingerprint}' IS NULL
        OR outcome #>> '{context_before,resources,available_parallelism}' IS NULL
        OR outcome #>> '{context_before,resources,effective_memory_mib}' IS NULL
        OR environment->>'source_fingerprint' IS NULL OR environment->>'source_fingerprint' = 'unknown'
        OR environment->>'mode' IS NULL OR environment->>'mode' NOT IN ('pool','daemon')),
    'staged_kind_trials', (SELECT count(*) FROM trials WHERE outcome->>'kind' <> 'cpu_kernel'),
    'conflicting_trial_identities', (SELECT count(*) FROM identities WHERE variants > 1),
    'reported_dropped_trials', coalesce((SELECT sum((tuning->>'dropped_trials')::bigint) FROM reports), 0),
    'outcomes', coalesce((SELECT jsonb_agg(outcomes ORDER BY kind, decision, reason) FROM outcomes), '[]'::jsonb),
    'cohorts_below_coverage', (SELECT count(*) FROM cohorts WHERE installations < 5),
    'ranked_hints', coalesce((SELECT jsonb_agg(h) FROM (SELECT * FROM hints ORDER BY
        (confidence = 'observational_support') DESC,
        median_ratio DESC, installations DESC, last_received DESC LIMIT 30) h), '[]'::jsonb),
    'interpretation', 'Installation-weighted same-context CPU-kernel A/B/A observations, not independent-host proof or automatic defaults. Gain support requires both cycles above policy margin and resolution, a reported gain, and a majority of each installation’s observed trials. Coverage includes aborted and inconclusive outcomes and their costs; missing-context counts may overlap staged kinds. Percentiles describe spread, not confidence intervals. No-confirmed-gain is not a regression. Worker/joint objectives and energy evidence remain staged. Validate on matching native hardware before shipping hints with a release.'
);
ROLLBACK;
