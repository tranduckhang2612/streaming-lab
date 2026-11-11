-- Checkpoint stop/restart evidence from the latest checkpoint experiment.
-- Run after: python3 main.py experiment checkpoint
\set ON_ERROR_STOP on

\echo '=== LATEST CHECKPOINT RECOVERY RESULT ==='
SELECT
    experiment_run_id,
    scenario_name,
    topic,
    before_spark_run_id,
    after_spark_run_id,
    before_query_id,
    after_query_id,
    checkpoint_location,
    phase1_delivered,
    downtime_delivered,
    backlog_event_count,
    query_id_preserved,
    spark_run_changed,
    checkpoint_reused,
    offsets_recovered,
    total_expected_count,
    total_actual_count,
    total_missing_count,
    total_extra_count,
    accuracy_complete,
    no_loss_no_duplicates,
    checkpoint_recovery_verified
FROM streaming.checkpoint_recovery_results
WHERE experiment_run_id = (
    SELECT run_id
    FROM streaming.experiment_runs
    WHERE experiment_type = 'checkpoint'
    ORDER BY started_at DESC
    LIMIT 1
)
ORDER BY sequence_number;

\echo '=== OFFSET EVIDENCE ==='
SELECT
    scenario_name,
    before_stop_offsets,
    downtime_end_offsets,
    recovered_offsets
FROM streaming.checkpoint_recovery_results
WHERE experiment_run_id = (
    SELECT run_id
    FROM streaming.experiment_runs
    WHERE experiment_type = 'checkpoint'
    ORDER BY started_at DESC
    LIMIT 1
)
ORDER BY sequence_number;
