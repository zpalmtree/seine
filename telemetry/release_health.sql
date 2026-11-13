BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '90s';
SET LOCAL lock_timeout = '2s';
-- Row estimates for the jsonb expansions are far too high; keep the scan parallel.
SET LOCAL parallel_tuple_cost = 0.01;
SET LOCAL parallel_setup_cost = 0;
SET LOCAL max_parallel_workers_per_gather = 3;
SET LOCAL enable_memoize = off;
-- Group estimates are far too high for hashing to look cheaper; it is.
SET LOCAL enable_sort = off;
SET LOCAL enable_incremental_sort = off;
SET LOCAL jit = off;
SET LOCAL work_mem = '64MB';
SET LOCAL TIME ZONE 'America/New_York';
-- Schema-14 operational fields: self-update, fork readiness, AMD detection,
-- GPU kernel sources and pool churn. Aggregates only; never installation IDs.
-- Settings-like fields (auto_install, pool booleans, AMD detection, GPU kernel
-- source) use each installation's latest report carrying them; window counts
-- are summed over every report in the seven days.
WITH reports AS MATERIALIZED (
    SELECT installation_id, received_at,
           p->>'miner_version' AS version,
           p #>> '{hardware,os}' AS os,
           p #>> '{hardware,cpu_vendor}' AS cpu_vendor,
           p #>> '{hardware,cpu_model}' AS cpu_model,
           (p #>> '{hardware,cpu_family}')::int AS cpu_family,
           (p #>> '{hardware,cpu_model_id}')::int AS cpu_model_id,
           (p->>'schema_version')::int AS schema_version,
           -- Events and previous-run records are most of a report. No section below
           -- reads these keys, and every pass would carry them.
           NULLIF(p->'diagnostics', 'null'::jsonb) - '{events,previous_run,nvidia_software,requested_backends,build_commit,source_fingerprint,phase,reason,cpu_affinity,cpu_autotune,cpu_page_mode,requested_cpu_threads,dropped_events,dropped_backend_samples,previous_run_unclean,previous_run_rebooted,shutdown_request,cleanup_stage}'::text[] AS d,
           -- Path queries are plain function calls and run in the parallel workers;
           -- a sub-select per report would run on one process.
           jsonb_path_query_array(p, '$.diagnostics.events[*] ? (@.detail.reason == "unsupported_work" || @.detail.operation == "receive_work")') AS unsupported_events,
           -- GPU samples with schema-14 settings; CPU samples are not read here.
           jsonb_path_query_array(p, '$.backends[*] ? ((@.settings.backend == "nvidia" || @.settings.backend == "amd") && (@.settings.gpu_arch != null || @.settings.gpu_kernel_source != null || @.settings.amd_perf_level != null))') AS gpu_backends,
           jsonb_path_exists(p, '$.backends[*] ? (@.settings.pow == "blake2b")') AS mined_blake2b
    FROM telemetry_reports
    -- Each payload reference would otherwise decompress the stored value again.
    CROSS JOIN LATERAL unnest(ARRAY[payload || '{}'::jsonb]) p
    WHERE received_at >= now() - interval '7 days'
      AND NOT (p->>'debug_build')::boolean
), coverage AS (
    SELECT version, count(*) AS installations,
           count(*) FILTER (WHERE schema14) AS schema14_installations
    FROM (SELECT version, installation_id, coalesce(bool_or(schema_version >= 14), false) AS schema14
          FROM reports GROUP BY 1, 2) installation
    GROUP BY 1
),
-- Self-update.
updates AS NOT MATERIALIZED (
    SELECT installation_id, received_at, version, d->'update' AS u
    FROM reports WHERE d->'update' IS NOT NULL
), update_installations AS (
    -- One row per installation and version; installation counts follow from its flags.
    SELECT version, installation_id,
           -- On only when the newest report carrying the setting has it on.
           max(received_at) FILTER (WHERE (u->>'auto_install')::boolean) = max(received_at) AS auto_install,
           sum((u->>'checks_api')::bigint) AS checks_api,
           sum((u->>'checks_web')::bigint) AS checks_web,
           sum((u->>'checks_failed')::bigint) AS checks_failed,
           sum((u->>'installs')::bigint) AS installs,
           sum((u->>'install_failures')::bigint) AS install_failures,
           coalesce(bool_or((u->>'checks_failed')::bigint > 0), false) AS failed_checks,
           coalesce(bool_or((u->>'installs')::bigint > 0), false) AS installing,
           coalesce(bool_or((u->>'install_failures')::bigint > 0), false) AS failed_installs,
           coalesce(bool_or((u->>'install_failures')::bigint > 0
                            AND received_at >= now() - interval '24 hours'), false) AS failed_installs_24h
    FROM updates GROUP BY 1, 2
), update_versions AS (
    SELECT version,
           count(*) AS installations,
           count(*) FILTER (WHERE auto_install) AS auto_install_installations,
           sum(checks_api) AS checks_api,
           sum(checks_web) AS checks_web,
           sum(checks_failed) AS checks_failed,
           count(*) FILTER (WHERE failed_checks) AS installations_with_failed_checks,
           sum(installs) AS installs,
           count(*) FILTER (WHERE installing) AS installations_installing,
           sum(install_failures) AS install_failures,
           count(*) FILTER (WHERE failed_installs) AS installations_with_install_failures,
           count(*) FILTER (WHERE failed_installs_24h) AS installations_with_install_failures_24h
    FROM update_installations GROUP BY 1
), update_failure_stages AS (
    -- Only the most recent failed stage per report window is recorded.
    SELECT version, u->>'last_install_failure' AS stage,
           count(*) AS reports, count(DISTINCT installation_id) AS installations
    FROM updates WHERE u->>'last_install_failure' IS NOT NULL
    GROUP BY 1, 2
),
-- Fork readiness: work this build cannot mine.
unsupported_events AS (
    SELECT r.installation_id, r.received_at, r.version, (e->>'count')::bigint AS occurrences
    FROM reports r CROSS JOIN LATERAL jsonb_array_elements(r.unsupported_events) e
    WHERE r.unsupported_events <> '[]'::jsonb
), paused AS (
    SELECT installation_id, received_at, version, d->>'mode' AS mode,
           (d->>'unsupported_work_paused_secs')::bigint AS secs
    FROM reports WHERE (d->>'unsupported_work_paused_secs')::bigint > 0
), fork_versions AS (
    SELECT v.version, v.mode,
           (SELECT count(DISTINCT installation_id) FROM paused p WHERE p.version = v.version AND p.mode IS NOT DISTINCT FROM v.mode) AS paused_installations,
           (SELECT count(DISTINCT installation_id) FROM paused p WHERE p.version = v.version AND p.mode IS NOT DISTINCT FROM v.mode
              AND p.received_at >= now() - interval '24 hours') AS paused_installations_24h,
           (SELECT count(*) FROM paused p WHERE p.version = v.version AND p.mode IS NOT DISTINCT FROM v.mode) AS paused_reports,
           (SELECT coalesce(sum(secs), 0) FROM paused p WHERE p.version = v.version AND p.mode IS NOT DISTINCT FROM v.mode) AS paused_secs
    FROM (SELECT DISTINCT version, mode FROM paused) v
), fork_event_versions AS (
    SELECT version, count(DISTINCT installation_id) AS installations,
           count(DISTINCT installation_id) FILTER (WHERE received_at >= now() - interval '24 hours') AS installations_24h,
           sum(occurrences) AS occurrences
    FROM unsupported_events GROUP BY 1
),
-- AMD detection. Most non-AMD machines report hip_unavailable with no AMD
-- devices; only pci_devices > 0 shows an AMD GPU that is not being mined.
-- Schema 15 leaves graphics built into the CPU out of pci_devices. Older
-- reports count them, so those devices are discounted: the ones HIP skipped as
-- integrated, or one on Ryzen CPUs that carry graphics ("Radeon" in the brand
-- string, Ryzen 7000 desktops at family 25 model 97 and Ryzen 9000 desktops
-- at family 26 model 68).
amd_latest AS (
    SELECT DISTINCT ON (r.installation_id) r.installation_id, r.version, r.os, r.schema_version,
           r.cpu_vendor, r.cpu_model, r.cpu_family, r.cpu_model_id,
           (r.d #>> '{amd_detection,hip_unavailable}')::boolean AS hip_unavailable,
           (r.d #>> '{amd_detection,found}')::int AS found,
           (r.d #>> '{amd_detection,selected}')::int AS selected,
           (r.d #>> '{amd_detection,integrated_skipped}')::int AS integrated_skipped,
           (r.d #>> '{amd_detection,unsupported_skipped}')::int AS unsupported_skipped,
           (r.d #>> '{amd_detection,pci_devices}')::int AS pci_devices
    FROM reports r
    -- The newest report carrying the detection, found without sorting every report.
    JOIN (SELECT installation_id, max(received_at) AS received_at
          FROM reports WHERE d->'amd_detection' IS NOT NULL GROUP BY 1) newest
      USING (installation_id, received_at)
    WHERE r.d->'amd_detection' IS NOT NULL
    ORDER BY r.installation_id
), amd AS (
    SELECT installation_id, version, os, hip_unavailable, found, selected,
           integrated_skipped, unsupported_skipped,
           CASE
               WHEN schema_version < 15 AND pci_devices > 0
               THEN GREATEST(pci_devices - GREATEST(
                        coalesce(integrated_skipped, 0),
                        CASE WHEN cpu_vendor = 'AuthenticAMD'
                                  AND (cpu_model ILIKE '%radeon%'
                                       OR (cpu_family = 25 AND cpu_model_id = 97)
                                       OR (cpu_family = 26 AND cpu_model_id = 68))
                             THEN 1 ELSE 0 END), 0)
               ELSE pci_devices
           END AS pci_devices
    FROM amd_latest
), amd_cohorts AS (
    SELECT version, os,
           count(*) AS installations,
           count(*) FILTER (WHERE pci_devices IS NULL) AS pci_unknown,
           count(*) FILTER (WHERE pci_devices > 0 OR found > 0) AS amd_present,
           count(*) FILTER (WHERE selected > 0) AS mining_on_amd,
           count(*) FILTER (WHERE pci_devices > 0 AND (hip_unavailable OR selected = 0)) AS amd_gpu_not_mining,
           count(*) FILTER (WHERE pci_devices > 0 AND hip_unavailable) AS not_mining_hip_unavailable,
           count(*) FILTER (WHERE pci_devices > 0 AND NOT hip_unavailable AND selected = 0) AS not_mining_none_selected,
           count(*) FILTER (WHERE pci_devices IS NULL AND found > 0 AND selected = 0) AS found_none_selected_pci_unknown,
           count(*) FILTER (WHERE integrated_skipped > 0) AS installations_skipping_integrated,
           count(*) FILTER (WHERE unsupported_skipped > 0) AS installations_skipping_unsupported,
           count(*) FILTER (WHERE hip_unavailable) AS hip_unavailable_any,
           coalesce(sum(found), 0) AS devices_found,
           coalesce(sum(selected), 0) AS devices_selected,
           coalesce(sum(pci_devices), 0) AS pci_devices
    FROM amd GROUP BY 1, 2
),
-- GPU kernel source, architecture and AMD performance level from each
-- installation's latest report with schema-14 GPU settings.
gpu_samples AS (
    SELECT installation_id, received_at, version, os, s->'settings' AS st,
           (s->>'instance')::bigint AS instance,
           max(received_at) OVER (PARTITION BY installation_id) AS latest
    FROM reports CROSS JOIN LATERAL jsonb_array_elements(gpu_backends) s
    WHERE gpu_backends <> '[]'::jsonb
), latest_gpu AS (
    SELECT * FROM gpu_samples WHERE received_at = latest
), kernel_sources AS (
    SELECT st->>'backend' AS backend, coalesce(st->>'gpu_arch', 'unknown') AS gpu_arch,
           coalesce(st->>'gpu_kernel_source', 'unknown') AS gpu_kernel_source,
           count(DISTINCT installation_id) AS installations, count(*) AS devices
    FROM latest_gpu GROUP BY 1, 2, 3
), amd_perf_levels AS (
    SELECT coalesce(st->>'gpu_arch', 'unknown') AS gpu_arch, os,
           coalesce(st->>'amd_perf_level', 'unknown') AS amd_perf_level,
           count(DISTINCT installation_id) AS installations, count(*) AS devices
    FROM latest_gpu WHERE st->>'backend' = 'amd' GROUP BY 1, 2, 3
),
-- Time to first hash. Every report of a run repeats its record, so DISTINCT
-- counts each run once, and first runs (cold kernel builds) stay visible.
-- first_hash_ms includes any wait for pool work; lag_behind_cpu_ms removes it.
startup_reports AS NOT MATERIALIZED (
    SELECT installation_id, received_at, version, d->'backend_startup' AS b,
           (d->>'uptime_secs')::bigint AS uptime_secs, mined_blake2b
    FROM reports WHERE d->'backend_startup' IS NOT NULL
), startup_records AS (
    SELECT DISTINCT installation_id, version, b->'first_hashes' AS first_hashes
    FROM startup_reports
), first_hash_runs AS (
    SELECT DISTINCT r.installation_id, r.version, f->>'backend' AS backend,
           (f->>'instance')::bigint AS instance, (f->>'first_hash_ms')::bigint AS ms,
           (f->>'first_hash_ms')::bigint - (
               SELECT min((c->>'first_hash_ms')::bigint)
               FROM jsonb_array_elements(r.first_hashes) c WHERE c->>'backend' = 'cpu'
           ) AS lag_behind_cpu_ms
    FROM startup_records r
    CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.first_hashes, '[]'::jsonb)) f
), first_hash_cohorts AS (
    SELECT r.version, r.backend, coalesce(a.gpu_arch, 'n/a') AS gpu_arch,
           count(DISTINCT r.installation_id) AS installations, count(*) AS runs,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY r.ms) AS first_hash_ms_p50,
           percentile_cont(0.9) WITHIN GROUP (ORDER BY r.ms) AS first_hash_ms_p90,
           max(r.ms) AS first_hash_ms_max,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY r.lag_behind_cpu_ms) AS lag_behind_cpu_ms_p50,
           percentile_cont(0.9) WITHIN GROUP (ORDER BY r.lag_behind_cpu_ms) AS lag_behind_cpu_ms_p90
    FROM first_hash_runs r
    LEFT JOIN (
        SELECT DISTINCT ON (installation_id, instance) installation_id, instance,
               st->>'gpu_arch' AS gpu_arch
        FROM latest_gpu ORDER BY installation_id, instance
    ) a ON a.installation_id = r.installation_id AND a.instance = r.instance
    GROUP BY 1, 2, 3
), latest_startup AS (
    -- Long enough that a missing backend is stuck, not still starting. A run with
    -- no hashes at all had no work, which says nothing about its backends.
    SELECT DISTINCT ON (r.installation_id) r.installation_id, r.version, r.b, r.mined_blake2b
    FROM startup_reports r
    JOIN (SELECT installation_id, max(received_at) AS received_at
          FROM startup_reports
          WHERE uptime_secs >= 600 AND jsonb_array_length(coalesce(b->'first_hashes', '[]'::jsonb)) > 0
          GROUP BY 1) newest USING (installation_id, received_at)
    WHERE r.uptime_secs >= 600 AND jsonb_array_length(coalesce(r.b->'first_hashes', '[]'::jsonb)) > 0
    ORDER BY r.installation_id
), expected_backends AS (
    SELECT l.installation_id, l.version, e.backend, e.expected, l.mined_blake2b,
           (SELECT count(*) FROM jsonb_array_elements(l.b->'first_hashes') f
            WHERE f->>'backend' = e.backend) AS hashing
    FROM latest_startup l
    CROSS JOIN LATERAL (VALUES
        ('cpu', (l.b->>'expected_cpu')::int), ('nvidia', (l.b->>'expected_nvidia')::int),
        ('amd', (l.b->>'expected_amd')::int), ('metal', (l.b->>'expected_metal')::int)
    ) AS e(backend, expected)
    WHERE e.expected > 0
), expected_backend_state AS (
    -- Metal that Seine adds by itself mines BLAKE2b only, so before the fork it
    -- waits idle. Telemetry cannot tell it from an explicit --backend metal, so
    -- an idle Metal is counted apart until the run mines BLAKE2b work.
    SELECT *, backend = 'metal' AND hashing = 0 AND NOT mined_blake2b AS idle_until_fork
    FROM expected_backends
), backends_not_hashing AS (
    SELECT version, backend, count(*) AS installations,
           count(*) FILTER (WHERE hashing < expected AND NOT idle_until_fork) AS installations_missing_backends,
           coalesce(sum(greatest(expected - hashing, 0)) FILTER (WHERE NOT idle_until_fork), 0) AS instances_not_hashing,
           count(*) FILTER (WHERE idle_until_fork) AS installations_idle_until_fork
    FROM expected_backend_state GROUP BY 1, 2
),
-- Pool connection setup and churn (pool mode only).
pool_reports AS NOT MATERIALIZED (
    SELECT installation_id, received_at, version, d->'pool' AS p
    FROM reports WHERE d->'pool' IS NOT NULL
), pool_activity AS MATERIALIZED (
    -- One row per installation, version and day; the totals below add these up.
    SELECT installation_id, version, received_at::date AS day, count(*) AS reports,
           max(received_at) AS last_report,
           sum((p->>'failovers')::bigint) AS failovers,
           sum((p->>'reconnects')::bigint) AS reconnects,
           sum((p->>'refills')::bigint) AS refills,
           sum((p->>'secs_on_backup')::bigint) AS secs_on_backup,
           coalesce(bool_or((p->>'failovers')::bigint > 0), false) AS failing_over,
           coalesce(bool_or((p->>'reconnects')::bigint > 0), false) AS reconnecting
    FROM pool_reports GROUP BY 1, 2, 3
), pool_latest AS (
    SELECT DISTINCT ON (r.installation_id) r.installation_id, r.p
    FROM pool_reports r
    JOIN (SELECT installation_id, max(last_report) AS received_at
          FROM pool_activity GROUP BY 1) newest USING (installation_id, received_at)
    ORDER BY r.installation_id
), pool_installation AS (
    SELECT installation_id,
           sum(failovers) AS failovers,
           sum(reconnects) AS reconnects,
           sum(refills) AS refills,
           sum(secs_on_backup) AS secs_on_backup
    FROM pool_activity GROUP BY 1
), pool_daily AS (
    SELECT day, sum(reports)::bigint AS reports,
           count(DISTINCT installation_id) AS installations,
           sum(failovers) AS failovers,
           sum(reconnects) AS reconnects,
           sum(refills) AS refills,
           sum(secs_on_backup) AS secs_on_backup
    FROM pool_activity GROUP BY 1
), pool_versions AS (
    SELECT version, count(DISTINCT installation_id) AS installations,
           count(DISTINCT installation_id) FILTER (WHERE failing_over) AS installations_failing_over,
           count(DISTINCT installation_id) FILTER (WHERE reconnecting) AS installations_reconnecting,
           sum(failovers) AS failovers,
           sum(reconnects) AS reconnects,
           sum(refills) AS refills,
           sum(secs_on_backup) AS secs_on_backup
    FROM pool_activity GROUP BY 1
), recent_install_failures AS (
    SELECT version, installations_with_install_failures_24h AS affected_installations
    FROM update_versions WHERE installations_with_install_failures_24h > 0
), recent_paused AS (
    SELECT version, count(DISTINCT installation_id) AS affected_installations
    FROM paused WHERE received_at >= now() - interval '24 hours' GROUP BY 1
)
SELECT jsonb_build_object(
    'window_days', 7,
    'coverage', coalesce((SELECT jsonb_agg(c ORDER BY version) FROM coverage c), '[]'::jsonb),
    'update', jsonb_build_object(
        'versions', coalesce((SELECT jsonb_agg(u ORDER BY version) FROM update_versions u), '[]'::jsonb),
        'install_failure_stages', coalesce((SELECT jsonb_agg(s ORDER BY version, installations DESC, stage) FROM update_failure_stages s), '[]'::jsonb)
    ),
    'fork_readiness', jsonb_build_object(
        'paused_versions', coalesce((SELECT jsonb_agg(f ORDER BY version, mode) FROM fork_versions f), '[]'::jsonb),
        'unsupported_work_event_versions', coalesce((SELECT jsonb_agg(f ORDER BY version) FROM fork_event_versions f), '[]'::jsonb)
    ),
    'amd', jsonb_build_object(
        'detection', coalesce((SELECT jsonb_agg(a ORDER BY amd_gpu_not_mining DESC, installations DESC, version, os) FROM amd_cohorts a), '[]'::jsonb),
        'amd_gpu_not_mining_installations', (SELECT count(*) FROM amd WHERE pci_devices > 0 AND (hip_unavailable OR selected = 0)),
        'perf_levels', coalesce((SELECT jsonb_agg(a ORDER BY gpu_arch, os, installations DESC, amd_perf_level) FROM amd_perf_levels a), '[]'::jsonb)
    ),
    'gpu_kernel_sources', coalesce((SELECT jsonb_agg(k ORDER BY backend, gpu_arch, installations DESC, gpu_kernel_source) FROM kernel_sources k), '[]'::jsonb),
    'startup', jsonb_build_object(
        'first_hash', coalesce((SELECT jsonb_agg(c ORDER BY version, backend, gpu_arch) FROM first_hash_cohorts c), '[]'::jsonb),
        'backends_not_hashing', coalesce((SELECT jsonb_agg(b ORDER BY version, backend) FROM backends_not_hashing b), '[]'::jsonb)
    ),
    'pool', jsonb_build_object(
        'installations', (SELECT count(*) FROM pool_latest),
        'tls_installations', (SELECT count(*) FROM pool_latest WHERE (p->>'tls')::boolean),
        'socks_installations', (SELECT count(*) FROM pool_latest WHERE (p->>'socks')::boolean),
        'payout_memo_installations', (SELECT count(*) FROM pool_latest WHERE (p->>'payout_memo')::boolean),
        'backup_pool_installations', (SELECT count(*) FROM pool_latest WHERE (p->>'backup_pools')::int > 0),
        'failovers', (SELECT coalesce(sum(failovers), 0) FROM pool_installation),
        'reconnects', (SELECT coalesce(sum(reconnects), 0) FROM pool_installation),
        'refills', (SELECT coalesce(sum(refills), 0) FROM pool_installation),
        'secs_on_backup', (SELECT coalesce(sum(secs_on_backup), 0) FROM pool_installation),
        'installations_failing_over', (SELECT count(*) FROM pool_installation WHERE failovers > 0),
        'installations_on_backup', (SELECT count(*) FROM pool_installation WHERE secs_on_backup > 0),
        -- Per-installation seven-day totals; quantiles describe the distribution,
        -- not any one installation.
        'per_installation', (SELECT jsonb_build_object(
            'failovers_p50', percentile_cont(0.5) WITHIN GROUP (ORDER BY failovers),
            'failovers_p90', percentile_cont(0.9) WITHIN GROUP (ORDER BY failovers),
            'reconnects_p50', percentile_cont(0.5) WITHIN GROUP (ORDER BY reconnects),
            'reconnects_p90', percentile_cont(0.9) WITHIN GROUP (ORDER BY reconnects),
            'refills_p50', percentile_cont(0.5) WITHIN GROUP (ORDER BY refills),
            'refills_p90', percentile_cont(0.9) WITHIN GROUP (ORDER BY refills),
            'secs_on_backup_p90', percentile_cont(0.9) WITHIN GROUP (ORDER BY secs_on_backup)
        ) FROM pool_installation),
        'versions', coalesce((SELECT jsonb_agg(v ORDER BY version) FROM pool_versions v), '[]'::jsonb),
        'daily', coalesce((SELECT jsonb_agg(d ORDER BY day) FROM pool_daily d), '[]'::jsonb)
    ),
    'recent_install_failure_versions', coalesce((SELECT jsonb_agg(r ORDER BY version) FROM recent_install_failures r), '[]'::jsonb),
    'recent_unsupported_work_versions', coalesce((SELECT jsonb_agg(r ORDER BY version) FROM recent_paused r), '[]'::jsonb),
    -- Same fleet threshold as diagnostics.sql: three installations of one version in 24 hours.
    'status', CASE WHEN EXISTS (SELECT 1 FROM recent_install_failures WHERE affected_installations >= 3)
                    OR EXISTS (SELECT 1 FROM recent_paused WHERE affected_installations >= 3) THEN 'attention'
                   WHEN NOT EXISTS (SELECT 1 FROM reports WHERE schema_version >= 14) THEN 'unavailable'
                   ELSE 'ok' END
);
ROLLBACK;
