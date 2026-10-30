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
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

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
    experiment_type TEXT NOT NULL CHECK (
        experiment_type IN ('custom', 'rate', 'late_ratio', 'partition')
    ),
    topic TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'passed', 'failed')),
    configuration JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

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
    status TEXT NOT NULL CHECK (
        status IN ('passed', 'failed', 'failed-no-summary')
    ),
    result_data JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id, sequence_number),
    UNIQUE (run_id, scenario_name),
    CHECK (partition_count IS NULL OR partition_count > 0),
    CHECK (finished_at >= started_at)
);

CREATE TABLE IF NOT EXISTS streaming.experiment_logs (
    scenario_id BIGINT PRIMARY KEY
        REFERENCES streaming.experiment_scenarios(scenario_id)
        ON DELETE CASCADE,
    log_text TEXT NOT NULL,
    stored_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
