\set ON_ERROR_STOP on

TRUNCATE streaming.experiment_runs,
         streaming.transaction_window_results,
         streaming.pipeline_runs CASCADE;

INSERT INTO streaming.experiment_runs (
    run_id, experiment_type, topic, started_at, finished_at, status
) VALUES (
    '11111111-1111-4111-8111-111111111111', 'watermark', 'test-events',
    '2026-01-01T00:00:00Z', '2026-01-01T00:02:00Z', 'passed'
);

INSERT INTO streaming.pipeline_runs (
    spark_run_id, query_id, topic, checkpoint_location,
    watermark_delay_seconds, window_duration_seconds,
    trigger_interval_seconds, started_at, finished_at, status
) VALUES (
    '22222222-2222-4222-8222-222222222222',
    '33333333-3333-4333-8333-333333333333',
    'test-events', '/test/checkpoint', 10, 60, 5,
    '2026-01-01T00:00:00Z', '2026-01-01T00:02:00Z', 'stopped'
);

INSERT INTO streaming.experiment_scenarios (
    run_id, sequence_number, scenario_name, topic,
    started_at, finished_at, spark_run_id, spark_query_id,
    spark_checkpoint_location, spark_baseline_batch_id, spark_final_batch_id,
    watermark_delay_seconds, ground_truth_status,
    ground_truth_manifest_sha256, ground_truth_event_count,
    ground_truth_window_count, status
) VALUES (
    '11111111-1111-4111-8111-111111111111', 1, 'watermark-10s',
    'test-events', '2026-01-01T00:00:00Z', '2026-01-01T00:02:00Z',
    '22222222-2222-4222-8222-222222222222',
    '33333333-3333-4333-8333-333333333333',
    '/test/checkpoint', 0, 3, 10, 'complete', repeat('a', 64), 3, 2,
    'passed'
);

INSERT INTO streaming.experiment_window_ground_truth (
    experiment_run_id, scenario_name, topic, window_start, window_end,
    merchant_category, watermark_delay_seconds, window_duration_seconds,
    expected_count, expected_total_amount_vnd, expected_average_amount_vnd,
    expected_min_amount_vnd, expected_max_amount_vnd,
    expected_simulated_late_count, expected_average_event_lateness_ms,
    manifest_sha256
) VALUES
    ('11111111-1111-4111-8111-111111111111', 'watermark-10s', 'test-events',
     '2026-01-01T00:00:00Z', '2026-01-01T00:01:00Z', 'grocery',
     10, 60, 2, 3000, 1500, 1000, 2000, 1, 22500, repeat('a', 64)),
    ('11111111-1111-4111-8111-111111111111', 'watermark-10s', 'test-events',
     '2026-01-01T00:01:00Z', '2026-01-01T00:02:00Z', 'restaurant',
     10, 60, 1, 1000, 1000, 1000, 1000, 0, 0, repeat('a', 64));

INSERT INTO streaming.experiment_window_actuals (
    experiment_run_id, scenario_name, topic, window_start, window_end,
    merchant_category, actual_count, actual_total_amount_vnd,
    actual_average_amount_vnd, actual_min_amount_vnd, actual_max_amount_vnd,
    actual_simulated_late_count, actual_average_event_lateness_ms,
    last_batch_id
) VALUES
    ('11111111-1111-4111-8111-111111111111', 'watermark-10s', 'test-events',
     '2026-01-01T00:00:00Z', '2026-01-01T00:01:00Z', 'grocery',
     3, 4000, 1333.33, 1000, 2000, 1, 15000, 3),
    ('11111111-1111-4111-8111-111111111111', 'watermark-10s', 'test-events',
     '2026-01-01T00:02:00Z', '2026-01-01T00:03:00Z', 'shopping',
     1, 500, 500, 500, 500, 0, 0, 3);

DO $$
DECLARE
    result streaming.experiment_scenario_accuracy%ROWTYPE;
BEGIN
    SELECT * INTO STRICT result
    FROM streaming.experiment_scenario_accuracy
    WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111'
      AND scenario_name = 'watermark-10s';

    IF NOT result.accuracy_complete
       OR result.expected_window_count <> 2
       OR result.actual_window_count <> 2
       OR result.total_expected_count <> 3
       OR result.total_actual_count <> 4
       OR result.total_missing_count <> 1
       OR result.total_extra_count <> 2
       OR result.overall_count_accuracy_percent <> 40.000
       OR result.overall_amount_accuracy_percent <> 54.545
       OR result.total_late_events_accepted <> 1
       OR result.total_late_events_dropped <> 0 THEN
        RAISE EXCEPTION 'unexpected accuracy summary: %', row_to_json(result);
    END IF;
END;
$$;
