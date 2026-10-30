-- Missing Spark metrics are unknown, not zero. Run before starting the new writer.
BEGIN;

ALTER TABLE streaming.pipeline_batch_metrics
    ALTER COLUMN num_input_rows DROP NOT NULL;

ALTER TABLE streaming.kafka_partition_metrics
    ALTER COLUMN start_offset DROP NOT NULL,
    ALTER COLUMN end_offset DROP NOT NULL,
    ALTER COLUMN latest_offset DROP NOT NULL,
    ALTER COLUMN rows_read DROP NOT NULL,
    ALTER COLUMN consumer_lag DROP NOT NULL;

COMMIT;
