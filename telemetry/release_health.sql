BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '45s';
SET LOCAL lock_timeout = '2s';
SET LOCAL TIME ZONE 'America/New_York';
-- Schema-14 operational fields: self-update, fork readiness, AMD detection,
-- GPU kernel sources and pool churn. Aggregates only; never installation IDs.
-- Settings-like fields (auto_install, pool booleans, AMD detection, GPU kernel
-- source) use each installation's latest report carrying them; window counts
-- are summed over every report in the seven days.
WITH reports AS MATERIALIZED (
    SELECT installation_id, received_at,
           payload->>'miner_version' AS version,
           payload #>> '{hardware,os}' AS os,
           payload #>> '{hardware,cpu_vendor}' AS cpu_vendor,
           payload #>> '{hardware,cpu_model}' AS cpu_model,
           (payload #>> '{hardware,cpu_family}')::int AS cpu_family,
           (payload #>> '{hardware,cpu_model_id}')::int AS cpu_model_id,
           (payload->>'schema_version')::int AS schema_version,
           NULLIF(payload->'diagnostics', 'null'::jsonb) AS d,
           payload->'backends' AS backends
    FROM telemetry_reports
    WHERE received_at >= now() - interval '7 days'
      AND NOT (payload->>'debug_build')::boolean
), coverage AS (
    SELECT version, count(DISTINCT installation_id) AS installations,
           count(DISTINCT installation_id) FILTER (WHERE schema_version >= 14) AS schema14_installations
    FROM reports GROUP BY 1
),
-- Self-update.
updates AS (
    SELECT installation_id, received_at, version, d->'update' AS u
    FROM reports WHERE d->'update' IS NOT NULL
), latest_update AS (
    SELECT DISTINCT ON (installation_id, version) installation_id, version, (u->>'auto_install')::boolean AS auto_install
    FROM updates ORDER BY installation_id, version, received_at DESC
), update_versions AS (
    SELECT version,
           count(DISTINCT installation_id) AS installations,
           (SELECT count(*) FROM latest_update l WHERE l.version = u.version AND l.auto_install) AS auto_install_installations,
           sum((u->>'checks_api')::bigint) AS checks_api,
           sum((u->>'checks_web')::bigint) AS checks_web,
           sum((u->>'checks_failed')::bigint) AS checks_failed,
           count(DISTINCT installation_id) FILTER (WHERE (u->>'checks_failed')::bigint > 0) AS installations_with_failed_checks,
           sum((u->>'installs')::bigint) AS installs,
           count(DISTINCT installation_id) FILTER (WHERE (u->>'installs')::bigint > 0) AS installations_installing,
           sum((u->>'install_failures')::bigint) AS install_failures,
           count(DISTINCT installation_id) FILTER (WHERE (u->>'install_failures')::bigint > 0) AS installations_with_install_failures,
           count(DISTINCT installation_id) FILTER (WHERE (u->>'install_failures')::bigint > 0
                                                     AND received_at >= now() - interval '24 hours') AS installations_with_install_failures_24h
    FROM updates u GROUP BY 1
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
    FROM reports r CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.d->'events', '[]'::jsonb)) e
    WHERE e #>> '{detail,reason}' = 'unsupported_work' OR e #>> '{detail,operation}' = 'receive_work'
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
    SELECT DISTINCT ON (installation_id) installation_id, version, os, schema_version,
           cpu_vendor, cpu_model, cpu_family, cpu_model_id,
           (d #>> '{amd_detection,hip_unavailable}')::boolean AS hip_unavailable,
           (d #>> '{amd_detection,found}')::int AS found,
           (d #>> '{amd_detection,selected}')::int AS selected,
           (d #>> '{amd_detection,integrated_skipped}')::int AS integrated_skipped,
           (d #>> '{amd_detection,unsupported_skipped}')::int AS unsupported_skipped,
           (d #>> '{amd_detection,pci_devices}')::int AS pci_devices
    FROM reports WHERE d->'amd_detection' IS NOT NULL
    ORDER BY installation_id, received_at DESC
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
    FROM reports CROSS JOIN LATERAL jsonb_array_elements(backends) s
    WHERE s #>> '{settings,backend}' IN ('nvidia', 'amd')
      AND coalesce(s #>> '{settings,gpu_arch}', s #>> '{settings,gpu_kernel_source}',
                   s #>> '{settings,amd_perf_level}') IS NOT NULL
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
startup_reports AS (
    SELECT installation_id, received_at, version, d->'backend_startup' AS b,
           (d->>'uptime_secs')::bigint AS uptime_secs
    FROM reports WHERE d->'backend_startup' IS NOT NULL
), first_hash_runs AS (
    SELECT DISTINCT r.installation_id, r.version, f->>'backend' AS backend,
           (f->>'instance')::bigint AS instance, (f->>'first_hash_ms')::bigint AS ms,
           (f->>'first_hash_ms')::bigint - (
               SELECT min((c->>'first_hash_ms')::bigint)
               FROM jsonb_array_elements(r.b->'first_hashes') c WHERE c->>'backend' = 'cpu'
           ) AS lag_behind_cpu_ms
    FROM startup_reports r
    CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.b->'first_hashes', '[]'::jsonb)) f
), first_hash_cohorts AS (
    SELECT r.version, r.backend, coalesce(a.gpu_arch, 'n/a') AS gpu_arch,
           count(DISTINCT r.installation_id) AS installations, count(*) AS runs,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY r.ms) AS first_hash_ms_p50,
           percentile_cont(0.9) WITHIN GROUP (ORDER BY r.ms) AS first_hash_ms_p90,
           max(r.ms) AS first_hash_ms_max,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY r.lag_behind_cpu_ms) AS lag_behind_cpu_ms_p50,
           percentile_cont(0.9) WITHIN GROUP (ORDER BY r.lag_behind_cpu_ms) AS lag_behind_cpu_ms_p90
    FROM first_hash_runs r
    LEFT JOIN LATERAL (
        SELECT g.st->>'gpu_arch' AS gpu_arch FROM latest_gpu g
        WHERE g.installation_id = r.installation_id AND g.instance = r.instance LIMIT 1
    ) a ON true
    GROUP BY 1, 2, 3
), latest_startup AS (
    -- Long enough that a missing backend is stuck, not still starting. A run with
    -- no hashes at all had no work, which says nothing about its backends.
    SELECT DISTINCT ON (installation_id) installation_id, version, b
    FROM startup_reports
    WHERE uptime_secs >= 600 AND jsonb_array_length(coalesce(b->'first_hashes', '[]'::jsonb)) > 0
    ORDER BY installation_id, received_at DESC
), expected_backends AS (
    SELECT l.installation_id, l.version, e.backend, e.expected,
           (SELECT count(*) FROM jsonb_array_elements(l.b->'first_hashes') f
            WHERE f->>'backend' = e.backend) AS hashing
    FROM latest_startup l
    CROSS JOIN LATERAL (VALUES
        ('cpu', (l.b->>'expected_cpu')::int), ('nvidia', (l.b->>'expected_nvidia')::int),
        ('amd', (l.b->>'expected_amd')::int), ('metal', (l.b->>'expected_metal')::int)
    ) AS e(backend, expected)
    WHERE e.expected > 0
), backends_not_hashing AS (
    SELECT version, backend, count(*) AS installations,
           count(*) FILTER (WHERE hashing < expected) AS installations_missing_backends,
           coalesce(sum(greatest(expected - hashing, 0)), 0) AS instances_not_hashing
    FROM expected_backends GROUP BY 1, 2
),
-- Pool connection setup and churn (pool mode only).
pool_reports AS (
    SELECT installation_id, received_at, version, d->'pool' AS p
    FROM reports WHERE d->'pool' IS NOT NULL
), pool_latest AS (
    SELECT DISTINCT ON (installation_id) installation_id, p
    FROM pool_reports ORDER BY installation_id, received_at DESC
), pool_installation AS (
    SELECT installation_id,
           sum((p->>'failovers')::bigint) AS failovers,
           sum((p->>'reconnects')::bigint) AS reconnects,
           sum((p->>'refills')::bigint) AS refills,
           sum((p->>'secs_on_backup')::bigint) AS secs_on_backup
    FROM pool_reports GROUP BY 1
), pool_daily AS (
    SELECT received_at::date AS day, count(*) AS reports,
           count(DISTINCT installation_id) AS installations,
           sum((p->>'failovers')::bigint) AS failovers,
           sum((p->>'reconnects')::bigint) AS reconnects,
           sum((p->>'refills')::bigint) AS refills,
           sum((p->>'secs_on_backup')::bigint) AS secs_on_backup
    FROM pool_reports GROUP BY 1
), pool_versions AS (
    SELECT version, count(DISTINCT installation_id) AS installations,
           count(DISTINCT installation_id) FILTER (WHERE (p->>'failovers')::bigint > 0) AS installations_failing_over,
           count(DISTINCT installation_id) FILTER (WHERE (p->>'reconnects')::bigint > 0) AS installations_reconnecting,
           sum((p->>'failovers')::bigint) AS failovers,
           sum((p->>'reconnects')::bigint) AS reconnects,
           sum((p->>'refills')::bigint) AS refills,
           sum((p->>'secs_on_backup')::bigint) AS secs_on_backup
    FROM pool_reports GROUP BY 1
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
