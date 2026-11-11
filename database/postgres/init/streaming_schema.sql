-- Tạo mới hoặc nâng cấp database hiện có bằng một file duy nhất.
-- Dừng các Spark writer trước khi áp dụng lên database đang sử dụng.
-- Có thể chạy lại; dữ liệu và khóa chính đã đúng được giữ nguyên.
BEGIN;

CREATE SCHEMA IF NOT EXISTS streaming;

-- Kết quả xử lý và metric của Spark Structured Streaming

CREATE TABLE IF NOT EXISTS streaming.transaction_window_results (
    topic TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    merchant_category TEXT NOT NULL,
    transaction_count BIGINT NOT NULL,
    total_amount_vnd BIGINT NOT NULL,
    average_amount_vnd NUMERIC(18, 2) NOT NULL,
    min_amount_vnd BIGINT NOT NULL,
    max_amount_vnd BIGINT NOT NULL,
    simulated_late_count BIGINT NOT NULL,
    average_event_lateness_ms NUMERIC(18, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (topic, window_start, window_end, merchant_category),
    CHECK (window_end > window_start)
);

CREATE TABLE IF NOT EXISTS streaming.pipeline_runs (
    spark_run_id UUID PRIMARY KEY,
    query_id UUID NOT NULL,
    topic TEXT NOT NULL,
    partition_count INTEGER,
    checkpoint_location TEXT NOT NULL,
    watermark_delay_seconds NUMERIC(12, 3) NOT NULL,
    window_duration_seconds NUMERIC(12, 3) NOT NULL,
    trigger_interval_seconds NUMERIC(12, 3) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'stopped', 'failed')),
    configuration JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (partition_count IS NULL OR partition_count > 0),
    CONSTRAINT pipeline_runs_watermark_delay_seconds_check CHECK (
        watermark_delay_seconds >= 0
        AND watermark_delay_seconds <> 'NaN'::NUMERIC
    ),
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

-- Also upgrade existing tables when this initialization script is reapplied.
ALTER TABLE streaming.pipeline_runs
    ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS superseded_by_run_id UUID;

-- Enforce valid watermarks for new writes. Keep the constraint NOT VALID only
-- when a legacy database contains an invalid historical measurement.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'streaming.pipeline_runs'::REGCLASS
          AND conname = 'pipeline_runs_watermark_delay_seconds_check'
    ) THEN
        ALTER TABLE streaming.pipeline_runs
            ADD CONSTRAINT pipeline_runs_watermark_delay_seconds_check CHECK (
                watermark_delay_seconds >= 0
                AND watermark_delay_seconds <> 'NaN'::NUMERIC
            ) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM streaming.pipeline_runs
        WHERE watermark_delay_seconds < 0
           OR watermark_delay_seconds = 'NaN'::NUMERIC
    ) THEN
        ALTER TABLE streaming.pipeline_runs
            VALIDATE CONSTRAINT pipeline_runs_watermark_delay_seconds_check;
    END IF;
END;
$$;

-- Keep the recorded terminal outcome separate from inferred driver liveness.
CREATE OR REPLACE VIEW streaming.pipeline_run_status AS
SELECT r.*,
    CASE
        WHEN r.status <> 'running' THEN r.status
        WHEN r.superseded_by_run_id IS NOT NULL THEN 'superseded'
        WHEN r.last_heartbeat_at IS NULL THEN 'unknown'
        WHEN r.last_heartbeat_at < CURRENT_TIMESTAMP - INTERVAL '30 seconds'
            THEN 'unresponsive'
        ELSE 'running'
    END AS effective_status,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - r.last_heartbeat_at)) AS heartbeat_age_seconds
FROM streaming.pipeline_runs r;

CREATE TABLE IF NOT EXISTS streaming.pipeline_batch_metrics (
    spark_run_id UUID NOT NULL REFERENCES streaming.pipeline_runs(spark_run_id)
        ON DELETE CASCADE,
    batch_id BIGINT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    topic TEXT NOT NULL,
    num_input_rows BIGINT,
    input_rows_per_second NUMERIC(18, 3),
    processed_rows_per_second NUMERIC(18, 3),
    batch_duration_ms BIGINT,
    trigger_execution_ms BIGINT,
    add_batch_ms BIGINT,
    get_batch_ms BIGINT,
    wal_commit_ms BIGINT,
    commit_offsets_ms BIGINT,
    checkpoint_commit_ms BIGINT,
    watermark_at TIMESTAMPTZ,
    max_event_time TIMESTAMPTZ,
    state_rows_total BIGINT,
    state_memory_bytes BIGINT,
    rows_dropped_by_watermark BIGINT,
    latency_avg_ms NUMERIC(18, 3),
    latency_p95_ms NUMERIC(18, 3),
    latency_max_ms NUMERIC(18, 3),
    total_consumer_lag BIGINT,
    max_partition_lag BIGINT,
    progress_json JSONB NOT NULL,
    PRIMARY KEY (spark_run_id, batch_id)
);

CREATE TABLE IF NOT EXISTS streaming.kafka_partition_metrics (
    spark_run_id UUID NOT NULL,
    batch_id BIGINT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    start_offset BIGINT,
    end_offset BIGINT,
    latest_offset BIGINT,
    rows_read BIGINT,
    consumer_lag BIGINT,
    PRIMARY KEY (spark_run_id, batch_id, topic, partition_id),
    FOREIGN KEY (spark_run_id, batch_id)
        REFERENCES streaming.pipeline_batch_metrics(spark_run_id, batch_id)
        ON DELETE CASCADE
);

-- Kết quả các kịch bản chạy generator

CREATE TABLE IF NOT EXISTS streaming.experiment_runs (
    run_id UUID PRIMARY KEY,
    experiment_type TEXT NOT NULL,
    CONSTRAINT experiment_runs_experiment_type_check CHECK (
        experiment_type IN (
            'custom', 'rate', 'late_ratio', 'partition', 'watermark',
            'checkpoint'
        )
    ),
    topic TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'passed', 'failed')),
    configuration JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

-- Older project versions used more than one name for the experiment-type
-- allow-list (for example experiment_runs_valid_type). Remove every legacy
-- allow-list that does not include checkpoint, then install one canonical
-- constraint. Keeping an old allow-list alongside the new one would still
-- reject checkpoint rows.
DO $$
DECLARE
    legacy_constraint TEXT;
BEGIN
    FOR legacy_constraint IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'streaming.experiment_runs'::REGCLASS
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%experiment_type%'
          AND pg_get_constraintdef(oid) LIKE '%custom%'
          AND pg_get_constraintdef(oid) LIKE '%partition%'
          AND pg_get_constraintdef(oid) NOT LIKE '%checkpoint%'
    LOOP
        EXECUTE format(
            'ALTER TABLE streaming.experiment_runs DROP CONSTRAINT %I',
            legacy_constraint
        );
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'streaming.experiment_runs'::REGCLASS
          AND conname = 'experiment_runs_experiment_type_check'
    ) THEN
        ALTER TABLE streaming.experiment_runs
            ADD CONSTRAINT experiment_runs_experiment_type_check CHECK (
                experiment_type IN (
                    'custom', 'rate', 'late_ratio', 'partition', 'watermark',
                    'checkpoint'
                )
            );
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS streaming.experiment_scenarios (
    scenario_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES streaming.experiment_runs(run_id)
        ON DELETE CASCADE,
    sequence_number INTEGER NOT NULL,
    scenario_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition_count INTEGER,
    active_partitions INTEGER,
    partition_event_counts JSONB,
    partition_balance_cv_percent NUMERIC(8, 3),
    target_rate_events_s NUMERIC(14, 2),
    target_late_ratio_percent NUMERIC(7, 3),
    late_delay_seconds NUMERIC(14, 3),
    duration_seconds NUMERIC(14, 3),
    seed BIGINT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    expected_events BIGINT,
    attempted BIGINT,
    delivered BIGINT,
    failed BIGINT,
    simulated_late BIGINT,
    remaining BIGINT,
    elapsed_seconds NUMERIC(14, 3),
    delivered_rate_events_s NUMERIC(14, 3),
    rate_achievement_percent NUMERIC(8, 3),
    delivery_success_percent NUMERIC(8, 3),
    actual_late_ratio_percent NUMERIC(8, 3),
    spark_run_id UUID,
    spark_query_id UUID,
    spark_checkpoint_location TEXT,
    spark_baseline_batch_id BIGINT,
    spark_final_batch_id BIGINT,
    watermark_delay_seconds NUMERIC(12, 3),
    ground_truth_status TEXT NOT NULL DEFAULT 'unavailable' CHECK (
        ground_truth_status IN ('unavailable', 'complete', 'incomplete')
    ),
    ground_truth_manifest_sha256 TEXT,
    ground_truth_event_count BIGINT,
    ground_truth_window_count BIGINT,
    ground_truth_error TEXT,
    status TEXT NOT NULL CHECK (
        status IN ('passed', 'failed', 'failed-no-summary')
    ),
    result_data JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id, sequence_number),
    UNIQUE (run_id, scenario_name),
    CONSTRAINT experiment_scenarios_spark_run_id_fkey
        FOREIGN KEY (spark_run_id) REFERENCES streaming.pipeline_runs(spark_run_id)
        ON DELETE SET NULL,
    CHECK (partition_count IS NULL OR partition_count > 0),
    CONSTRAINT experiment_scenarios_spark_baseline_batch_id_check CHECK (
        spark_baseline_batch_id IS NULL OR spark_baseline_batch_id >= -1
    ),
    CONSTRAINT experiment_scenarios_spark_final_batch_id_check CHECK (
        spark_final_batch_id IS NULL OR spark_final_batch_id >= -1
    ),
    CONSTRAINT experiment_scenarios_spark_batch_range_check CHECK (
        spark_baseline_batch_id IS NULL
        OR spark_final_batch_id IS NULL
        OR spark_final_batch_id >= spark_baseline_batch_id
    ),
    CONSTRAINT experiment_scenarios_watermark_delay_seconds_check CHECK (
        watermark_delay_seconds IS NULL OR (
            watermark_delay_seconds >= 0
            AND watermark_delay_seconds <> 'NaN'::NUMERIC
        )
    ),
    CHECK (finished_at >= started_at)
);

CREATE TABLE IF NOT EXISTS streaming.experiment_logs (
    scenario_id BIGINT PRIMARY KEY
        REFERENCES streaming.experiment_scenarios(scenario_id)
        ON DELETE CASCADE,
    log_text TEXT NOT NULL,
    stored_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- One row is written only by the controlled stop/produce/restart scenario.
-- The two Spark run IDs must differ while the query ID and checkpoint remain
-- stable; offsets prove that the backlog created during downtime was consumed.
CREATE TABLE IF NOT EXISTS streaming.checkpoint_recovery_evidence (
    scenario_id BIGINT PRIMARY KEY
        REFERENCES streaming.experiment_scenarios(scenario_id)
        ON DELETE CASCADE,
    before_spark_run_id UUID REFERENCES streaming.pipeline_runs(spark_run_id)
        ON DELETE SET NULL,
    after_spark_run_id UUID REFERENCES streaming.pipeline_runs(spark_run_id)
        ON DELETE SET NULL,
    before_query_id UUID,
    after_query_id UUID,
    checkpoint_location TEXT NOT NULL,
    before_stop_offsets JSONB NOT NULL,
    downtime_end_offsets JSONB NOT NULL,
    recovered_offsets JSONB NOT NULL,
    backlog_event_count BIGINT NOT NULL CHECK (backlog_event_count >= 0),
    phase1_delivered BIGINT NOT NULL CHECK (phase1_delivered >= 0),
    downtime_delivered BIGINT NOT NULL CHECK (downtime_delivered >= 0),
    query_id_preserved BOOLEAN NOT NULL,
    spark_run_changed BOOLEAN NOT NULL,
    checkpoint_reused BOOLEAN NOT NULL,
    offsets_recovered BOOLEAN NOT NULL,
    identity_status TEXT NOT NULL CHECK (identity_status IN ('passed', 'failed')),
    verified_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (jsonb_typeof(before_stop_offsets) = 'object'),
    CHECK (jsonb_typeof(downtime_end_offsets) = 'object'),
    CHECK (jsonb_typeof(recovered_offsets) = 'object')
);

-- Upgrade experiment rows created before Spark linkage was normalized.
ALTER TABLE streaming.experiment_scenarios
    ADD COLUMN IF NOT EXISTS spark_run_id UUID,
    ADD COLUMN IF NOT EXISTS spark_query_id UUID,
    ADD COLUMN IF NOT EXISTS spark_checkpoint_location TEXT,
    ADD COLUMN IF NOT EXISTS spark_baseline_batch_id BIGINT,
    ADD COLUMN IF NOT EXISTS spark_final_batch_id BIGINT,
    ADD COLUMN IF NOT EXISTS watermark_delay_seconds NUMERIC(12, 3),
    ADD COLUMN IF NOT EXISTS ground_truth_status TEXT NOT NULL DEFAULT 'unavailable',
    ADD COLUMN IF NOT EXISTS ground_truth_manifest_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS ground_truth_event_count BIGINT,
    ADD COLUMN IF NOT EXISTS ground_truth_window_count BIGINT,
    ADD COLUMN IF NOT EXISTS ground_truth_error TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'streaming.experiment_scenarios'::REGCLASS
          AND conname = 'experiment_scenarios_ground_truth_status_check'
    ) THEN
        ALTER TABLE streaming.experiment_scenarios
            ADD CONSTRAINT experiment_scenarios_ground_truth_status_check
            CHECK (ground_truth_status IN ('unavailable', 'complete', 'incomplete'));
    END IF;
END;
$$;

-- Only pipeline_runs is trusted as a UUID source; malformed historical JSON is ignored.
UPDATE streaming.experiment_scenarios s
SET spark_run_id = COALESCE(s.spark_run_id, p.spark_run_id),
    spark_query_id = COALESCE(s.spark_query_id, p.query_id),
    spark_checkpoint_location = COALESCE(
        s.spark_checkpoint_location,
        p.checkpoint_location,
        NULLIF(s.result_data ->> 'spark_checkpoint_location', '')
    ),
    watermark_delay_seconds = COALESCE(
        s.watermark_delay_seconds,
        CASE
            WHEN p.watermark_delay_seconds >= 0
             AND p.watermark_delay_seconds <> 'NaN'::NUMERIC
            THEN p.watermark_delay_seconds
        END
    )
FROM streaming.pipeline_runs p
WHERE p.spark_run_id = s.spark_run_id
   OR (
       s.spark_run_id IS NULL
       AND s.result_data ->> 'spark_run_id' = p.spark_run_id::TEXT
   );

UPDATE streaming.experiment_scenarios
SET spark_checkpoint_location = NULLIF(
        result_data ->> 'spark_checkpoint_location', ''
    )
WHERE spark_checkpoint_location IS NULL;

-- Preserve an actual watermark recorded in legacy result JSON even when the
-- referenced pipeline run has already been deleted.
UPDATE streaming.experiment_scenarios
SET watermark_delay_seconds = (
        result_data ->> 'watermark_delay_seconds'
    )::NUMERIC(12, 3)
WHERE watermark_delay_seconds IS NULL
  AND CASE
      WHEN jsonb_typeof(result_data -> 'watermark_delay_seconds') = 'number'
      THEN (result_data ->> 'watermark_delay_seconds')::NUMERIC
           BETWEEN 0 AND 999999999.999
      ELSE FALSE
  END;

-- Invalid historical values are unknown measurements, not zero-second
-- watermarks. Clear them before installing the stronger scenario constraint.
UPDATE streaming.experiment_scenarios
SET watermark_delay_seconds = NULL
WHERE watermark_delay_seconds < 0
   OR watermark_delay_seconds = 'NaN'::NUMERIC;

UPDATE streaming.experiment_scenarios
SET spark_baseline_batch_id = (
        result_data ->> 'spark_baseline_batch_id'
    )::BIGINT
WHERE spark_baseline_batch_id IS NULL
  AND CASE
      WHEN result_data ->> 'spark_baseline_batch_id' ~ '^-?[0-9]{1,18}$'
      THEN (result_data ->> 'spark_baseline_batch_id')::NUMERIC >= -1
      ELSE FALSE
  END;

UPDATE streaming.experiment_scenarios
SET spark_final_batch_id = (
        result_data ->> 'spark_final_batch_id'
    )::BIGINT
WHERE spark_final_batch_id IS NULL
  AND CASE
      WHEN result_data ->> 'spark_final_batch_id' ~ '^-?[0-9]{1,18}$'
      THEN (result_data ->> 'spark_final_batch_id')::NUMERIC >= -1
           AND (
               spark_baseline_batch_id IS NULL
               OR (result_data ->> 'spark_final_batch_id')::NUMERIC
                  >= spark_baseline_batch_id
           )
      ELSE FALSE
  END;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'streaming.experiment_scenarios'::REGCLASS
          AND conname = 'experiment_scenarios_spark_run_id_fkey'
    ) THEN
        ALTER TABLE streaming.experiment_scenarios
            ADD CONSTRAINT experiment_scenarios_spark_run_id_fkey
            FOREIGN KEY (spark_run_id)
            REFERENCES streaming.pipeline_runs(spark_run_id)
            ON DELETE SET NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'streaming.experiment_scenarios'::REGCLASS
          AND conname = 'experiment_scenarios_spark_baseline_batch_id_check'
    ) THEN
        ALTER TABLE streaming.experiment_scenarios
            ADD CONSTRAINT experiment_scenarios_spark_baseline_batch_id_check
            CHECK (spark_baseline_batch_id IS NULL OR spark_baseline_batch_id >= -1);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'streaming.experiment_scenarios'::REGCLASS
          AND conname = 'experiment_scenarios_spark_final_batch_id_check'
    ) THEN
        ALTER TABLE streaming.experiment_scenarios
            ADD CONSTRAINT experiment_scenarios_spark_final_batch_id_check
            CHECK (spark_final_batch_id IS NULL OR spark_final_batch_id >= -1);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'streaming.experiment_scenarios'::REGCLASS
          AND conname = 'experiment_scenarios_spark_batch_range_check'
    ) THEN
        ALTER TABLE streaming.experiment_scenarios
            ADD CONSTRAINT experiment_scenarios_spark_batch_range_check
            CHECK (
                spark_baseline_batch_id IS NULL
                OR spark_final_batch_id IS NULL
                OR spark_final_batch_id >= spark_baseline_batch_id
            );
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'streaming.experiment_scenarios'::REGCLASS
          AND conname = 'experiment_scenarios_watermark_delay_seconds_check'
          AND pg_get_constraintdef(oid) NOT LIKE '%NaN%'
    ) THEN
        ALTER TABLE streaming.experiment_scenarios
            DROP CONSTRAINT experiment_scenarios_watermark_delay_seconds_check;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'streaming.experiment_scenarios'::REGCLASS
          AND conname = 'experiment_scenarios_watermark_delay_seconds_check'
    ) THEN
        ALTER TABLE streaming.experiment_scenarios
            ADD CONSTRAINT experiment_scenarios_watermark_delay_seconds_check
            CHECK (
                watermark_delay_seconds IS NULL OR (
                    watermark_delay_seconds >= 0
                    AND watermark_delay_seconds <> 'NaN'::NUMERIC
                )
            );
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS experiment_scenarios_spark_run_id_idx
    ON streaming.experiment_scenarios (spark_run_id);

-- One stable row per scenario for Grafana. Micro-batch p95 values are kept
-- explicitly named; they are not a mathematically exact p95 for the whole scenario.
CREATE OR REPLACE VIEW streaming.experiment_scenario_metrics AS
SELECT
    s.scenario_id,
    s.run_id AS experiment_run_id,
    r.experiment_type,
    r.started_at AS experiment_started_at,
    r.finished_at AS experiment_finished_at,
    r.status AS experiment_status,
    s.sequence_number,
    s.scenario_name,
    s.topic,
    s.partition_count,
    s.active_partitions,
    s.partition_balance_cv_percent,
    s.target_rate_events_s,
    s.delivered_rate_events_s,
    s.rate_achievement_percent,
    s.target_late_ratio_percent,
    s.actual_late_ratio_percent,
    s.delivered,
    s.failed,
    s.delivery_success_percent,
    s.status AS scenario_status,
    s.started_at AS scenario_started_at,
    s.finished_at AS scenario_finished_at,
    s.spark_run_id,
    COALESCE(s.spark_query_id, p.query_id) AS spark_query_id,
    COALESCE(s.spark_checkpoint_location, p.checkpoint_location)
        AS spark_checkpoint_location,
    s.spark_baseline_batch_id,
    s.spark_final_batch_id,
    COALESCE(
        s.watermark_delay_seconds,
        CASE
            WHEN p.watermark_delay_seconds >= 0
             AND p.watermark_delay_seconds <> 'NaN'::NUMERIC
            THEN p.watermark_delay_seconds
        END
    )
        AS watermark_delay_seconds,
    s.result_data ->> 'spark_status' AS spark_status,
    NULLIF(s.result_data ->> 'spark_error', '') AS spark_error,
    p.effective_status AS pipeline_effective_status,
    p.heartbeat_age_seconds,
    (
        s.spark_run_id IS NOT NULL
        AND s.spark_baseline_batch_id IS NOT NULL
        AND s.spark_final_batch_id IS NOT NULL
        AND s.result_data ->> 'spark_status' = 'passed'
        AND metrics.metric_batch_count > 0
    ) AS metrics_bounds_complete,
    metrics.metric_batch_count,
    metrics.measured_input_batch_count,
    metrics.measured_dropped_batch_count,
    metrics.spark_input_rows,
    metrics.avg_input_rows_per_second,
    metrics.peak_input_rows_per_second,
    metrics.avg_processed_rows_per_second,
    metrics.peak_processed_rows_per_second,
    metrics.max_total_consumer_lag,
    metrics.max_partition_lag,
    metrics.avg_batch_avg_latency_ms,
    metrics.avg_batch_p95_latency_ms,
    metrics.max_batch_p95_latency_ms,
    metrics.max_latency_ms,
    metrics.avg_checkpoint_commit_ms,
    metrics.p95_checkpoint_commit_ms,
    metrics.max_checkpoint_commit_ms,
    metrics.rows_dropped_by_watermark,
    metrics.max_state_rows_total,
    metrics.max_state_memory_bytes,
    metrics.avg_batch_duration_ms,
    metrics.p95_batch_duration_ms,
    metrics.metric_started_at,
    metrics.metric_finished_at,
    s.late_delay_seconds,
    s.duration_seconds,
    s.attempted,
    s.simulated_late,
    p.window_duration_seconds,
    p.trigger_interval_seconds,
    metrics.min_watermark_at,
    metrics.max_watermark_at
FROM streaming.experiment_scenarios s
JOIN streaming.experiment_runs r ON r.run_id = s.run_id
LEFT JOIN streaming.pipeline_run_status p ON p.spark_run_id = s.spark_run_id
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) AS metric_batch_count,
        COUNT(m.num_input_rows) AS measured_input_batch_count,
        COUNT(m.rows_dropped_by_watermark) AS measured_dropped_batch_count,
        CASE
            WHEN COUNT(*) = COUNT(m.num_input_rows) THEN SUM(m.num_input_rows)
        END AS spark_input_rows,
        AVG(m.input_rows_per_second) AS avg_input_rows_per_second,
        MAX(m.input_rows_per_second) AS peak_input_rows_per_second,
        AVG(m.processed_rows_per_second) AS avg_processed_rows_per_second,
        MAX(m.processed_rows_per_second) AS peak_processed_rows_per_second,
        MAX(m.total_consumer_lag) AS max_total_consumer_lag,
        MAX(m.max_partition_lag) AS max_partition_lag,
        AVG(m.latency_avg_ms) AS avg_batch_avg_latency_ms,
        AVG(m.latency_p95_ms) AS avg_batch_p95_latency_ms,
        MAX(m.latency_p95_ms) AS max_batch_p95_latency_ms,
        MAX(m.latency_max_ms) AS max_latency_ms,
        AVG(m.checkpoint_commit_ms) AS avg_checkpoint_commit_ms,
        percentile_cont(0.95) WITHIN GROUP (
            ORDER BY m.checkpoint_commit_ms
        ) FILTER (
            WHERE m.checkpoint_commit_ms IS NOT NULL
        ) AS p95_checkpoint_commit_ms,
        MAX(m.checkpoint_commit_ms) AS max_checkpoint_commit_ms,
        CASE
            WHEN COUNT(*) = COUNT(m.rows_dropped_by_watermark)
            THEN SUM(m.rows_dropped_by_watermark)
        END AS rows_dropped_by_watermark,
        MAX(m.state_rows_total) AS max_state_rows_total,
        MAX(m.state_memory_bytes) AS max_state_memory_bytes,
        AVG(m.batch_duration_ms) AS avg_batch_duration_ms,
        percentile_cont(0.95) WITHIN GROUP (
            ORDER BY m.batch_duration_ms
        ) FILTER (
            WHERE m.batch_duration_ms IS NOT NULL
        ) AS p95_batch_duration_ms,
        MIN(m.observed_at) AS metric_started_at,
        MAX(m.observed_at) AS metric_finished_at,
        MIN(m.watermark_at) FILTER (
            WHERE m.watermark_at > '2000-01-01'::TIMESTAMPTZ
        ) AS min_watermark_at,
        MAX(m.watermark_at) FILTER (
            WHERE m.watermark_at > '2000-01-01'::TIMESTAMPTZ
        ) AS max_watermark_at
    FROM streaming.pipeline_batch_metrics m
    WHERE m.spark_run_id = s.spark_run_id
      AND s.spark_baseline_batch_id IS NOT NULL
      AND m.batch_id > s.spark_baseline_batch_id
      AND (
          (s.spark_final_batch_id IS NOT NULL
           AND m.batch_id <= s.spark_final_batch_id)
          OR (s.spark_final_batch_id IS NULL
              AND m.observed_at <= s.finished_at)
      )
      AND m.add_batch_ms IS NOT NULL
) metrics ON TRUE;

-- Nâng cấp bảng kết quả cũ chưa tách theo topic.
-- Không suy đoán topic của dữ liệu lịch sử từ cấu hình hiện tại.
ALTER TABLE streaming.transaction_window_results
    ADD COLUMN IF NOT EXISTS topic TEXT;

UPDATE streaming.transaction_window_results
SET topic = '<legacy-unknown>'
WHERE topic IS NULL;

ALTER TABLE streaming.transaction_window_results
    ALTER COLUMN topic SET NOT NULL;

DO $$
DECLARE
    primary_key_name TEXT;
    primary_key_columns TEXT[];
BEGIN
    SELECT c.conname, ARRAY(
        SELECT a.attname::TEXT
        FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum, position)
        JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
        ORDER BY k.position
    ) INTO primary_key_name, primary_key_columns
    FROM pg_constraint c
    WHERE c.conrelid = 'streaming.transaction_window_results'::REGCLASS
      AND c.contype = 'p';

    IF primary_key_columns IS DISTINCT FROM
        ARRAY['topic', 'window_start', 'window_end', 'merchant_category']::TEXT[] THEN
        IF primary_key_name IS NOT NULL THEN
            EXECUTE format(
                'ALTER TABLE streaming.transaction_window_results DROP CONSTRAINT %I',
                primary_key_name
            );
        END IF;
        ALTER TABLE streaming.transaction_window_results
            ADD CONSTRAINT transaction_window_results_pkey
            PRIMARY KEY (topic, window_start, window_end, merchant_category);
    END IF;
END;
$$;

-- Metric thiếu là chưa biết, kể cả trên bảng được tạo bởi phiên bản cũ.
ALTER TABLE streaming.pipeline_batch_metrics
    ALTER COLUMN num_input_rows DROP NOT NULL;

ALTER TABLE streaming.kafka_partition_metrics
    ALTER COLUMN start_offset DROP NOT NULL,
    ALTER COLUMN end_offset DROP NOT NULL,
    ALTER COLUMN latest_offset DROP NOT NULL,
    ALTER COLUMN rows_read DROP NOT NULL,
    ALTER COLUMN consumer_lag DROP NOT NULL;

-- ====================================================================
-- Kiểm chứng độ chính xác kết quả cửa sổ (Window Accuracy Verification)
-- ====================================================================

-- Preserve evidence created by the first, non-scenario-scoped implementation.
-- These generated rows are not used for new accuracy calculations.
DROP VIEW IF EXISTS streaming.watermark_accuracy_summary;
DROP VIEW IF EXISTS streaming.window_accuracy_detail;
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'streaming'
          AND c.relname = 'verification_events'
          AND c.relkind IN ('r', 'p')
    ) AND to_regclass('streaming.legacy_verification_events') IS NULL THEN
        ALTER TABLE streaming.verification_events
            RENAME TO legacy_verification_events;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'streaming'
          AND c.relname = 'window_ground_truth'
          AND c.relkind IN ('r', 'p')
    ) AND to_regclass('streaming.legacy_window_ground_truth') IS NULL THEN
        ALTER TABLE streaming.window_ground_truth
            RENAME TO legacy_window_ground_truth;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'streaming'
          AND c.relname = 'window_accuracy_results'
          AND c.relkind IN ('r', 'p')
    ) AND to_regclass('streaming.legacy_window_accuracy_results') IS NULL THEN
        ALTER TABLE streaming.window_accuracy_results
            RENAME TO legacy_window_accuracy_results;
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS streaming.experiment_verification_events (
    experiment_run_id UUID NOT NULL REFERENCES streaming.experiment_runs(run_id)
        ON DELETE CASCADE,
    scenario_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    transaction_id UUID NOT NULL,
    account_id TEXT NOT NULL,
    merchant_id TEXT NOT NULL,
    merchant_category TEXT NOT NULL,
    amount_vnd BIGINT NOT NULL,
    currency TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL,
    sequence BIGINT NOT NULL,
    simulated_late BOOLEAN NOT NULL,
    event_lateness_ms BIGINT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (experiment_run_id, scenario_name, transaction_id),
    CHECK (amount_vnd > 0),
    CHECK (sequence > 0),
    CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$')
);

-- A checkpoint experiment intentionally starts a second generator process
-- while Spark is down, so its local sequence restarts at 1. Transaction IDs
-- remain the event identity; remove the older sequence uniqueness assumption.
DO $$
DECLARE
    legacy_constraint TEXT;
BEGIN
    FOR legacy_constraint IN
        SELECT c.conname
        FROM pg_constraint c
        WHERE c.conrelid = 'streaming.experiment_verification_events'::REGCLASS
          AND c.contype = 'u'
          AND ARRAY(
              SELECT a.attname::TEXT
              FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum, position)
              JOIN pg_attribute a
                ON a.attrelid = c.conrelid AND a.attnum = k.attnum
              ORDER BY k.position
          ) = ARRAY[
              'experiment_run_id', 'scenario_name', 'sequence'
          ]::TEXT[]
    LOOP
        EXECUTE format(
            'ALTER TABLE streaming.experiment_verification_events '
            'DROP CONSTRAINT %I',
            legacy_constraint
        );
    END LOOP;
END;
$$;

CREATE TABLE IF NOT EXISTS streaming.experiment_window_ground_truth (
    experiment_run_id UUID NOT NULL REFERENCES streaming.experiment_runs(run_id)
        ON DELETE CASCADE,
    scenario_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    merchant_category TEXT NOT NULL,
    watermark_delay_seconds NUMERIC(12, 3) NOT NULL,
    window_duration_seconds NUMERIC(12, 3) NOT NULL,
    expected_count BIGINT NOT NULL,
    expected_total_amount_vnd BIGINT NOT NULL,
    expected_average_amount_vnd NUMERIC(18, 2) NOT NULL,
    expected_min_amount_vnd BIGINT NOT NULL,
    expected_max_amount_vnd BIGINT NOT NULL,
    expected_simulated_late_count BIGINT NOT NULL,
    expected_average_event_lateness_ms NUMERIC(18, 2) NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (
        experiment_run_id, scenario_name, topic, window_start, window_end,
        merchant_category
    ),
    CHECK (window_end > window_start),
    CHECK (watermark_delay_seconds >= 0),
    CHECK (window_duration_seconds > 0),
    CHECK (expected_count > 0),
    CHECK (expected_simulated_late_count BETWEEN 0 AND expected_count),
    CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS streaming.experiment_window_actuals (
    experiment_run_id UUID NOT NULL REFERENCES streaming.experiment_runs(run_id)
        ON DELETE CASCADE,
    scenario_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    merchant_category TEXT NOT NULL,
    actual_count BIGINT NOT NULL,
    actual_total_amount_vnd BIGINT NOT NULL,
    actual_average_amount_vnd NUMERIC(18, 2) NOT NULL,
    actual_min_amount_vnd BIGINT NOT NULL,
    actual_max_amount_vnd BIGINT NOT NULL,
    actual_simulated_late_count BIGINT NOT NULL,
    actual_average_event_lateness_ms NUMERIC(18, 2) NOT NULL,
    last_batch_id BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (
        experiment_run_id, scenario_name, topic, window_start, window_end,
        merchant_category
    ),
    CHECK (window_end > window_start),
    CHECK (actual_count > 0),
    CHECK (actual_simulated_late_count BETWEEN 0 AND actual_count),
    CHECK (last_batch_id >= 0)
);

-- FULL OUTER JOIN keeps both missing expected windows and unexpected actual
-- windows. Intersection-over-union accuracy penalizes both missing and extra.
CREATE OR REPLACE VIEW streaming.window_accuracy_results AS
SELECT
    COALESCE(g.experiment_run_id, a.experiment_run_id) AS experiment_run_id,
    COALESCE(g.scenario_name, a.scenario_name) AS scenario_name,
    COALESCE(g.topic, a.topic) AS topic,
    COALESCE(g.window_start, a.window_start) AS window_start,
    COALESCE(g.window_end, a.window_end) AS window_end,
    COALESCE(g.merchant_category, a.merchant_category) AS merchant_category,
    g.watermark_delay_seconds,
    g.window_duration_seconds,
    (g.experiment_run_id IS NOT NULL) AS expected_present,
    (a.experiment_run_id IS NOT NULL) AS actual_present,
    COALESCE(g.expected_count, 0) AS expected_count,
    COALESCE(a.actual_count, 0) AS actual_count,
    GREATEST(COALESCE(g.expected_count, 0) - COALESCE(a.actual_count, 0), 0)
        AS missing_count,
    GREATEST(COALESCE(a.actual_count, 0) - COALESCE(g.expected_count, 0), 0)
        AS extra_count,
    COALESCE(g.expected_total_amount_vnd, 0) AS expected_amount,
    COALESCE(a.actual_total_amount_vnd, 0) AS actual_amount,
    COALESCE(a.actual_total_amount_vnd, 0)
        - COALESCE(g.expected_total_amount_vnd, 0) AS amount_difference,
    ROUND(
        CASE
            WHEN GREATEST(COALESCE(g.expected_count, 0), COALESCE(a.actual_count, 0)) = 0
            THEN 100
            ELSE LEAST(COALESCE(g.expected_count, 0), COALESCE(a.actual_count, 0))::NUMERIC
                 / GREATEST(COALESCE(g.expected_count, 0), COALESCE(a.actual_count, 0))
                 * 100
        END,
        3
    ) AS count_accuracy_percent,
    ROUND(
        CASE
            WHEN GREATEST(
                COALESCE(g.expected_total_amount_vnd, 0),
                COALESCE(a.actual_total_amount_vnd, 0)
            ) = 0 THEN 100
            ELSE LEAST(
                COALESCE(g.expected_total_amount_vnd, 0),
                COALESCE(a.actual_total_amount_vnd, 0)
            )::NUMERIC / GREATEST(
                COALESCE(g.expected_total_amount_vnd, 0),
                COALESCE(a.actual_total_amount_vnd, 0)
            ) * 100
        END,
        3
    ) AS amount_accuracy_percent,
    COALESCE(g.expected_simulated_late_count, 0) AS expected_late_count,
    COALESCE(a.actual_simulated_late_count, 0) AS actual_late_count,
    LEAST(
        COALESCE(g.expected_simulated_late_count, 0),
        COALESCE(a.actual_simulated_late_count, 0)
    ) AS late_events_accepted,
    GREATEST(
        COALESCE(g.expected_simulated_late_count, 0)
            - COALESCE(a.actual_simulated_late_count, 0),
        0
    ) AS late_events_dropped,
    GREATEST(
        COALESCE(a.actual_simulated_late_count, 0)
            - COALESCE(g.expected_simulated_late_count, 0),
        0
    ) AS late_events_extra,
    g.manifest_sha256,
    a.last_batch_id,
    a.updated_at
FROM streaming.experiment_window_ground_truth g
FULL OUTER JOIN streaming.experiment_window_actuals a
    ON a.experiment_run_id = g.experiment_run_id
   AND a.scenario_name = g.scenario_name
   AND a.topic = g.topic
   AND a.window_start = g.window_start
   AND a.window_end = g.window_end
   AND a.merchant_category = g.merchant_category;

CREATE OR REPLACE VIEW streaming.experiment_scenario_accuracy AS
SELECT
    s.run_id AS experiment_run_id,
    s.sequence_number,
    s.scenario_name,
    s.topic,
    s.watermark_delay_seconds,
    s.ground_truth_status,
    s.ground_truth_manifest_sha256,
    s.ground_truth_event_count,
    s.ground_truth_window_count,
    s.ground_truth_error,
    COUNT(*) FILTER (WHERE a.expected_present) AS expected_window_count,
    COUNT(*) FILTER (WHERE a.actual_present) AS actual_window_count,
    COALESCE(SUM(a.expected_count), 0) AS total_expected_count,
    COALESCE(SUM(a.actual_count), 0) AS total_actual_count,
    COALESCE(SUM(a.missing_count), 0) AS total_missing_count,
    COALESCE(SUM(a.extra_count), 0) AS total_extra_count,
    COALESCE(SUM(a.expected_amount), 0) AS total_expected_amount,
    COALESCE(SUM(a.actual_amount), 0) AS total_actual_amount,
    CASE
        WHEN s.ground_truth_status = 'complete'
         AND s.status = 'passed'
         AND s.spark_run_id IS NOT NULL
        THEN ROUND(
            CASE
                WHEN COALESCE(SUM(GREATEST(a.expected_count, a.actual_count)), 0) = 0
                THEN 100
                ELSE SUM(LEAST(a.expected_count, a.actual_count))::NUMERIC
                     / SUM(GREATEST(a.expected_count, a.actual_count)) * 100
            END,
            3
        )
    END AS overall_count_accuracy_percent,
    CASE
        WHEN s.ground_truth_status = 'complete'
         AND s.status = 'passed'
         AND s.spark_run_id IS NOT NULL
        THEN ROUND(
            CASE
                WHEN COALESCE(SUM(GREATEST(a.expected_amount, a.actual_amount)), 0) = 0
                THEN 100
                ELSE SUM(LEAST(a.expected_amount, a.actual_amount))::NUMERIC
                     / SUM(GREATEST(a.expected_amount, a.actual_amount)) * 100
            END,
            3
        )
    END AS overall_amount_accuracy_percent,
    COALESCE(SUM(a.expected_late_count), 0) AS total_expected_late_events,
    COALESCE(SUM(a.late_events_accepted), 0) AS total_late_events_accepted,
    COALESCE(SUM(a.late_events_dropped), 0) AS total_late_events_dropped,
    CASE
        WHEN COALESCE(SUM(a.expected_late_count), 0) > 0
        THEN ROUND(
            SUM(a.late_events_accepted)::NUMERIC
                / SUM(a.expected_late_count) * 100,
            3
        )
    END AS late_event_acceptance_percent,
    (
        s.ground_truth_status = 'complete'
        AND s.status = 'passed'
        AND s.spark_run_id IS NOT NULL
        AND s.ground_truth_event_count = COALESCE(SUM(a.expected_count), 0)
        AND s.ground_truth_window_count = COUNT(*) FILTER (WHERE a.expected_present)
    ) AS accuracy_complete
FROM streaming.experiment_scenarios s
LEFT JOIN streaming.window_accuracy_results a
    ON a.experiment_run_id = s.run_id
   AND a.scenario_name = s.scenario_name
GROUP BY s.run_id, s.sequence_number, s.scenario_name, s.topic,
         s.watermark_delay_seconds, s.ground_truth_status,
         s.ground_truth_manifest_sha256, s.ground_truth_event_count,
         s.ground_truth_window_count, s.ground_truth_error,
         s.status, s.spark_run_id;

CREATE OR REPLACE VIEW streaming.checkpoint_recovery_results AS
SELECT
    s.run_id AS experiment_run_id,
    s.sequence_number,
    s.scenario_name,
    s.topic,
    s.status AS scenario_status,
    e.before_spark_run_id,
    e.after_spark_run_id,
    e.before_query_id,
    e.after_query_id,
    e.checkpoint_location,
    e.before_stop_offsets,
    e.downtime_end_offsets,
    e.recovered_offsets,
    e.backlog_event_count,
    e.phase1_delivered,
    e.downtime_delivered,
    e.query_id_preserved,
    e.spark_run_changed,
    e.checkpoint_reused,
    e.offsets_recovered,
    e.identity_status,
    a.total_expected_count,
    a.total_actual_count,
    a.total_missing_count,
    a.total_extra_count,
    a.overall_count_accuracy_percent,
    a.accuracy_complete,
    (
        COALESCE(a.accuracy_complete, FALSE)
        AND a.total_expected_count = a.total_actual_count
        AND a.total_missing_count = 0
        AND a.total_extra_count = 0
    ) AS no_loss_no_duplicates,
    (
        s.status = 'passed'
        AND e.before_spark_run_id IS NOT NULL
        AND e.after_spark_run_id IS NOT NULL
        AND e.query_id_preserved
        AND e.spark_run_changed
        AND e.checkpoint_reused
        AND e.offsets_recovered
        AND e.backlog_event_count > 0
        AND e.backlog_event_count = e.downtime_delivered
        AND COALESCE(a.accuracy_complete, FALSE)
        AND a.total_expected_count = a.total_actual_count
        AND a.total_missing_count = 0
        AND a.total_extra_count = 0
    ) AS checkpoint_recovery_verified,
    e.verified_at
FROM streaming.checkpoint_recovery_evidence e
JOIN streaming.experiment_scenarios s ON s.scenario_id = e.scenario_id
LEFT JOIN streaming.experiment_scenario_accuracy a
    ON a.experiment_run_id = s.run_id
   AND a.scenario_name = s.scenario_name;

CREATE OR REPLACE VIEW streaming.window_accuracy_detail AS
SELECT * FROM streaming.window_accuracy_results;

CREATE OR REPLACE VIEW streaming.watermark_accuracy_summary AS
SELECT *
FROM streaming.experiment_scenario_accuracy
WHERE watermark_delay_seconds IS NOT NULL;

CREATE OR REPLACE VIEW streaming.verification_events AS
SELECT * FROM streaming.experiment_verification_events;

CREATE OR REPLACE VIEW streaming.window_ground_truth AS
SELECT * FROM streaming.experiment_window_ground_truth;

COMMIT;
