-- Derive telemetry_report_facts again for every stored report: when the table is
-- first installed, after a change to its functions or source view in
-- src/schema.sql, and after a PostgreSQL major upgrade. Safe to repeat and to
-- run while reports arrive; unchanged rows are left alone. One day per commit.
DO $$
DECLARE
    day timestamptz;
BEGIN
    FOR day IN
        SELECT generate_series(date_trunc('day', min(received_at)), max(received_at), interval '1 day')
        FROM telemetry_reports
    LOOP
        INSERT INTO telemetry_report_facts AS f (
            report_id, installation_id, received_at, debug_build, version, periodic,
            has_diagnostics, build_commit, settled, previous_run_unclean,
            dropped_diagnostics, hardware_hash, build_hash,
            context_hash, settings_hashes, hps)
        SELECT report_id, installation_id, received_at, debug_build, version, periodic,
               has_diagnostics, build_commit, settled, previous_run_unclean,
            dropped_diagnostics, hardware_hash, build_hash,
               context_hash, settings_hashes, hps
        FROM telemetry_report_facts_source
        WHERE received_at >= day AND received_at < day + interval '1 day'
        ON CONFLICT (report_id) DO UPDATE SET
            installation_id = EXCLUDED.installation_id, received_at = EXCLUDED.received_at,
            debug_build = EXCLUDED.debug_build, version = EXCLUDED.version,
            periodic = EXCLUDED.periodic, has_diagnostics = EXCLUDED.has_diagnostics,
            build_commit = EXCLUDED.build_commit, settled = EXCLUDED.settled,
            previous_run_unclean = EXCLUDED.previous_run_unclean,
            dropped_diagnostics = EXCLUDED.dropped_diagnostics,
            hardware_hash = EXCLUDED.hardware_hash, build_hash = EXCLUDED.build_hash,
            context_hash = EXCLUDED.context_hash, settings_hashes = EXCLUDED.settings_hashes,
            hps = EXCLUDED.hps
        WHERE f IS DISTINCT FROM EXCLUDED;
        COMMIT;
    END LOOP;
END
$$;
