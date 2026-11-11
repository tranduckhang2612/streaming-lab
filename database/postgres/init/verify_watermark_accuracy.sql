-- Scenario-scoped watermark accuracy evidence.
-- Run after: python3 main.py experiment watermark --watermarks 10 30 60

\echo ''
\echo '=== WATERMARK ACCURACY BY EXPERIMENT SCENARIO ==='

SELECT
    experiment_run_id,
    sequence_number,
    scenario_name,
    watermark_delay_seconds AS watermark_s,
    total_expected_count AS expected,
    total_actual_count AS actual,
    total_missing_count AS missing,
    total_extra_count AS extra,
    overall_count_accuracy_percent AS count_accuracy_pct,
    overall_amount_accuracy_percent AS amount_accuracy_pct,
    total_late_events_accepted AS late_accepted,
    total_late_events_dropped AS late_dropped,
    late_event_acceptance_percent AS late_accept_pct,
    accuracy_complete
FROM streaming.experiment_scenario_accuracy
WHERE watermark_delay_seconds IS NOT NULL
ORDER BY experiment_run_id, sequence_number;

\echo ''
\echo '=== WINDOW-LEVEL DETAIL (MISSING AND EXTRA KEYS INCLUDED) ==='

SELECT
    experiment_run_id,
    scenario_name,
    topic,
    window_start,
    window_end,
    merchant_category,
    expected_present,
    actual_present,
    expected_count,
    actual_count,
    missing_count,
    extra_count,
    count_accuracy_percent,
    late_events_accepted,
    late_events_dropped
FROM streaming.window_accuracy_results
ORDER BY experiment_run_id, scenario_name, window_start, merchant_category;

\echo ''
\echo '=== BEST COMPLETE WATERMARK SCENARIO PER RUN ==='

SELECT DISTINCT ON (experiment_run_id)
    experiment_run_id,
    scenario_name,
    watermark_delay_seconds AS watermark_s,
    overall_count_accuracy_percent AS count_accuracy_pct,
    overall_amount_accuracy_percent AS amount_accuracy_pct,
    total_late_events_dropped,
    CASE
        WHEN overall_count_accuracy_percent = 100 THEN 'PERFECT'
        WHEN overall_count_accuracy_percent >= 99 THEN 'EXCELLENT'
        WHEN overall_count_accuracy_percent >= 95 THEN 'GOOD'
        ELSE 'DATA LOSS'
    END AS verdict
FROM streaming.experiment_scenario_accuracy
WHERE accuracy_complete
  AND watermark_delay_seconds IS NOT NULL
ORDER BY experiment_run_id, overall_count_accuracy_percent DESC,
         watermark_delay_seconds ASC;
