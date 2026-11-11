\set ON_ERROR_STOP on

TRUNCATE streaming.experiment_runs,
         streaming.transaction_window_results,
         streaming.pipeline_runs CASCADE;

INSERT INTO streaming.experiment_runs (
    run_id, experiment_type, topic, started_at, finished_at, status
) VALUES (
    '11111111-1111-4111-8111-111111111111', 'checkpoint', 'test-events',
    '2026-01-01T00:00:00Z', '2026-01-01T00:03:00Z', 'passed'
);

INSERT INTO streaming.pipeline_runs (
    spark_run_id, query_id, topic, checkpoint_location,
    watermark_delay_seconds, window_duration_seconds,
    trigger_interval_seconds, started_at, finished_at, status
) VALUES
    ('22222222-2222-4222-8222-222222222222',
     '33333333-3333-4333-8333-333333333333',
     'test-events', '/test/recovery-checkpoint', 30, 60, 5,
     '2026-01-01T00:00:00Z', '2026-01-01T00:01:00Z', 'stopped'),
    ('44444444-4444-4444-8444-444444444444',
     '33333333-3333-4333-8333-333333333333',
     'test-events', '/test/recovery-checkpoint', 30, 60, 5,
     '2026-01-01T00:02:00Z', '2026-01-01T00:03:00Z', 'stopped');

WITH scenario AS (
    INSERT INTO streaming.experiment_scenarios (
        run_id, sequence_number, scenario_name, topic,
        started_at, finished_at, delivered,
        spark_run_id, spark_query_id, spark_checkpoint_location,
        spark_baseline_batch_id, spark_final_batch_id,
        watermark_delay_seconds, ground_truth_status,
        ground_truth_manifest_sha256, ground_truth_event_count,
        ground_truth_window_count, status
    ) VALUES (
        '11111111-1111-4111-8111-111111111111', 1,
        'checkpoint-restart', 'test-events',
        '2026-01-01T00:00:00Z', '2026-01-01T00:03:00Z', 2,
        '44444444-4444-4444-8444-444444444444',
        '33333333-3333-4333-8333-333333333333',
        '/test/recovery-checkpoint', -1, 3, 30, 'complete',
        repeat('a', 64), 2, 1, 'passed'
    ) RETURNING scenario_id
)
INSERT INTO streaming.checkpoint_recovery_evidence (
    scenario_id, before_spark_run_id, after_spark_run_id,
    before_query_id, after_query_id, checkpoint_location,
    before_stop_offsets, downtime_end_offsets, recovered_offsets,
    backlog_event_count, phase1_delivered, downtime_delivered,
    query_id_preserved, spark_run_changed, checkpoint_reused,
    offsets_recovered, identity_status
)
SELECT scenario_id,
    '22222222-2222-4222-8222-222222222222',
    '44444444-4444-4444-8444-444444444444',
    '33333333-3333-4333-8333-333333333333',
    '33333333-3333-4333-8333-333333333333',
    '/test/recovery-checkpoint', '{"0":1}', '{"0":2}', '{"0":2}',
    1, 1, 1, TRUE, TRUE, TRUE, TRUE, 'passed'
FROM scenario;

-- Sequence is local to each generator process, so both phases may contain 1.
INSERT INTO streaming.experiment_verification_events (
    experiment_run_id, scenario_name, topic, transaction_id,
    account_id, merchant_id, merchant_category, amount_vnd, currency,
    event_time, produced_at, sequence, simulated_late, event_lateness_ms,
    manifest_sha256
) VALUES
    ('11111111-1111-4111-8111-111111111111', 'checkpoint-restart',
     'test-events', 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
     'account-001', 'merchant-001', 'grocery', 1000, 'VND',
     '2026-01-01T00:00:01Z', '2026-01-01T00:00:01Z', 1, FALSE, 0,
     repeat('a', 64)),
    ('11111111-1111-4111-8111-111111111111', 'checkpoint-restart',
     'test-events', 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
     'account-001', 'merchant-001', 'grocery', 2000, 'VND',
     '2026-01-01T00:00:02Z', '2026-01-01T00:00:02Z', 1, FALSE, 0,
     repeat('a', 64));

INSERT INTO streaming.experiment_window_ground_truth (
    experiment_run_id, scenario_name, topic, window_start, window_end,
    merchant_category, watermark_delay_seconds, window_duration_seconds,
    expected_count, expected_total_amount_vnd, expected_average_amount_vnd,
    expected_min_amount_vnd, expected_max_amount_vnd,
    expected_simulated_late_count, expected_average_event_lateness_ms,
    manifest_sha256
) VALUES (
    '11111111-1111-4111-8111-111111111111', 'checkpoint-restart',
    'test-events', '2026-01-01T00:00:00Z', '2026-01-01T00:01:00Z',
    'grocery', 30, 60, 2, 3000, 1500, 1000, 2000, 0, 0,
    repeat('a', 64)
);

INSERT INTO streaming.experiment_window_actuals (
    experiment_run_id, scenario_name, topic, window_start, window_end,
    merchant_category, actual_count, actual_total_amount_vnd,
    actual_average_amount_vnd, actual_min_amount_vnd, actual_max_amount_vnd,
    actual_simulated_late_count, actual_average_event_lateness_ms,
    last_batch_id
) VALUES (
    '11111111-1111-4111-8111-111111111111', 'checkpoint-restart',
    'test-events', '2026-01-01T00:00:00Z', '2026-01-01T00:01:00Z',
    'grocery', 2, 3000, 1500, 1000, 2000, 0, 0, 3
);

DO $$
DECLARE
    result streaming.checkpoint_recovery_results%ROWTYPE;
BEGIN
    SELECT * INTO STRICT result
    FROM streaming.checkpoint_recovery_results
    WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111';

    IF NOT result.query_id_preserved
       OR NOT result.spark_run_changed
       OR NOT result.checkpoint_reused
       OR NOT result.offsets_recovered
       OR NOT result.accuracy_complete
       OR NOT result.no_loss_no_duplicates
       OR NOT result.checkpoint_recovery_verified
       OR result.total_expected_count <> 2
       OR result.total_actual_count <> 2 THEN
        RAISE EXCEPTION 'unexpected recovery evidence: %', row_to_json(result);
    END IF;
END;
$$;

-- A duplicate after restart must invalidate the final recovery verdict.
UPDATE streaming.experiment_window_actuals
SET actual_count = 3,
    actual_total_amount_vnd = 4000
WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111'
  AND scenario_name = 'checkpoint-restart';

DO $$
DECLARE
    result streaming.checkpoint_recovery_results%ROWTYPE;
BEGIN
    SELECT * INTO STRICT result
    FROM streaming.checkpoint_recovery_results
    WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111';

    IF result.no_loss_no_duplicates
       OR result.checkpoint_recovery_verified
       OR result.total_extra_count <> 1 THEN
        RAISE EXCEPTION 'duplicate event was not detected: %', row_to_json(result);
    END IF;
END;
$$;

UPDATE streaming.experiment_window_actuals
SET actual_count = 2,
    actual_total_amount_vnd = 3000
WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111'
  AND scenario_name = 'checkpoint-restart';
