\set ON_ERROR_STOP on

-- Every statement mirrors one panel in experiment_evidence.json with Grafana
-- variables replaced by the deterministic accuracy_regression.sql fixture.

SELECT COUNT(*) FROM (
    SELECT scenario_name AS metric, overall_count_accuracy_percent AS value
    FROM streaming.experiment_scenario_accuracy
    WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111'
      AND scenario_name IN ('watermark-10s')
    ORDER BY sequence_number
) panel_count_accuracy;

SELECT COUNT(*) FROM (
    SELECT scenario_name AS metric, overall_amount_accuracy_percent AS value
    FROM streaming.experiment_scenario_accuracy
    WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111'
      AND scenario_name IN ('watermark-10s')
    ORDER BY sequence_number
) panel_amount_accuracy;

SELECT COUNT(*) FROM (
    SELECT scenario_name, total_missing_count AS missing, total_extra_count AS extra
    FROM streaming.experiment_scenario_accuracy
    WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111'
      AND scenario_name IN ('watermark-10s')
    ORDER BY sequence_number
) panel_missing_extra;

SELECT COUNT(*) FROM (
    SELECT scenario_name, total_late_events_accepted AS accepted,
           total_late_events_dropped AS dropped
    FROM streaming.experiment_scenario_accuracy
    WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111'
      AND scenario_name IN ('watermark-10s')
    ORDER BY sequence_number
) panel_late_events;

SELECT COUNT(*) FROM (
    SELECT scenario_name, target_rate_events_s AS target,
           delivered_rate_events_s AS delivered,
           ROUND(avg_processed_rows_per_second, 3) AS spark_processed
    FROM streaming.experiment_scenario_metrics
    WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111'
      AND scenario_name IN ('watermark-10s')
    ORDER BY sequence_number
) panel_throughput;

SELECT COUNT(*) FROM (
    SELECT scenario_name AS metric, max_batch_p95_latency_ms AS value
    FROM streaming.experiment_scenario_metrics
    WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111'
      AND scenario_name IN ('watermark-10s')
    ORDER BY sequence_number
) panel_latency;

SELECT COUNT(*) FROM (
    SELECT scenario_name, max_total_consumer_lag AS total_lag,
           max_partition_lag
    FROM streaming.experiment_scenario_metrics
    WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111'
      AND scenario_name IN ('watermark-10s')
    ORDER BY sequence_number
) panel_lag;

SELECT COUNT(*) FROM (
    SELECT scenario_name AS metric, p95_checkpoint_commit_ms AS value
    FROM streaming.experiment_scenario_metrics
    WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111'
      AND scenario_name IN ('watermark-10s')
    ORDER BY sequence_number
) panel_checkpoint;

SELECT COUNT(*) FROM (
    SELECT m.sequence_number, m.scenario_name, m.experiment_type, m.topic,
      m.partition_count, m.active_partitions, m.watermark_delay_seconds,
      m.target_rate_events_s, m.delivered_rate_events_s,
      ROUND(m.avg_processed_rows_per_second, 3) AS avg_processed_rows_per_second,
      m.max_total_consumer_lag, m.max_partition_lag,
      ROUND(m.max_batch_p95_latency_ms, 3) AS max_batch_p95_latency_ms,
      ROUND(m.p95_checkpoint_commit_ms::NUMERIC, 3) AS p95_checkpoint_commit_ms,
      m.rows_dropped_by_watermark,
      a.total_expected_count, a.total_actual_count,
      a.total_missing_count, a.total_extra_count,
      a.overall_count_accuracy_percent, a.overall_amount_accuracy_percent,
      a.total_late_events_accepted, a.total_late_events_dropped,
      a.ground_truth_manifest_sha256, a.accuracy_complete,
      m.metrics_bounds_complete, m.spark_status, m.pipeline_effective_status,
      m.scenario_status
    FROM streaming.experiment_scenario_metrics m
    LEFT JOIN streaming.experiment_scenario_accuracy a
      ON a.experiment_run_id = m.experiment_run_id
     AND a.scenario_name = m.scenario_name
    WHERE m.experiment_run_id = '11111111-1111-4111-8111-111111111111'
      AND m.scenario_name IN ('watermark-10s')
    ORDER BY m.sequence_number
) panel_scenario_table;

SELECT COUNT(*) FROM (
    SELECT scenario_name, topic, window_start, window_end, merchant_category,
      expected_present, actual_present, expected_count, actual_count,
      missing_count, extra_count, expected_amount, actual_amount,
      count_accuracy_percent, amount_accuracy_percent,
      expected_late_count, actual_late_count,
      late_events_accepted, late_events_dropped, late_events_extra,
      last_batch_id, manifest_sha256
    FROM streaming.window_accuracy_results
    WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111'
      AND scenario_name IN ('watermark-10s')
    ORDER BY scenario_name, window_start, merchant_category
    LIMIT 2000
) panel_window_detail;

SELECT COUNT(*) FROM (
    SELECT sequence_number, scenario_name, spark_run_id, spark_query_id,
      spark_checkpoint_location, spark_baseline_batch_id, spark_final_batch_id,
      metric_batch_count, p95_checkpoint_commit_ms, max_watermark_at,
      spark_status, pipeline_effective_status, metrics_bounds_complete
    FROM streaming.experiment_scenario_metrics
    WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111'
      AND scenario_name IN ('watermark-10s')
    ORDER BY sequence_number
) panel_checkpoint_evidence;

SELECT COUNT(*) FROM (
    SELECT sequence_number, scenario_name,
      checkpoint_recovery_verified, no_loss_no_duplicates, accuracy_complete,
      query_id_preserved, spark_run_changed, checkpoint_reused,
      offsets_recovered, total_expected_count, total_actual_count,
      total_missing_count, total_extra_count,
      phase1_delivered, downtime_delivered, backlog_event_count, topic,
      before_spark_run_id, after_spark_run_id,
      before_query_id, after_query_id, checkpoint_location
    FROM streaming.checkpoint_recovery_results
    WHERE experiment_run_id = '11111111-1111-4111-8111-111111111111'
      AND scenario_name IN ('watermark-10s')
    ORDER BY sequence_number
) panel_checkpoint_restart;

-- Updated scenario table in the operational dashboard.
SELECT COUNT(*) FROM (
    SELECT left(m.experiment_run_id::text, 8) AS experiment_run,
      m.experiment_started_at, m.experiment_type, m.scenario_name, m.topic,
      m.partition_count, m.active_partitions, m.watermark_delay_seconds,
      m.late_delay_seconds, m.window_duration_seconds, m.trigger_interval_seconds,
      m.target_late_ratio_percent, m.actual_late_ratio_percent, m.simulated_late,
      m.target_rate_events_s, m.delivered_rate_events_s, m.spark_input_rows,
      m.avg_processed_rows_per_second, m.max_total_consumer_lag,
      m.max_partition_lag, m.avg_batch_p95_latency_ms,
      m.max_batch_p95_latency_ms, m.p95_checkpoint_commit_ms,
      m.rows_dropped_by_watermark, a.total_expected_count,
      a.total_actual_count, a.total_missing_count, a.total_extra_count,
      a.overall_count_accuracy_percent, a.overall_amount_accuracy_percent,
      a.total_late_events_accepted, a.total_late_events_dropped,
      a.accuracy_complete, m.metric_batch_count,
      m.measured_dropped_batch_count,
      m.measured_dropped_batch_count = m.metric_batch_count
        AS dropped_measurements_complete,
      m.max_watermark_at, m.spark_run_id, m.spark_checkpoint_location,
      m.metrics_bounds_complete, m.spark_status, m.pipeline_effective_status,
      m.scenario_status
    FROM streaming.experiment_scenario_metrics m
    LEFT JOIN streaming.experiment_scenario_accuracy a
      ON a.experiment_run_id = m.experiment_run_id
     AND a.scenario_name = m.scenario_name
    WHERE m.experiment_run_id = '11111111-1111-4111-8111-111111111111'
    ORDER BY m.experiment_started_at, m.sequence_number
) operational_scenario_table;
