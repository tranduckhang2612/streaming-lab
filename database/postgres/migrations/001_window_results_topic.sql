-- Stop Spark writers before running this migration.
-- Historical rows have no reliable topic information; preserve them separately
-- from real Kafka topics using a label that is not a valid Kafka topic name.
BEGIN;

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
BEGIN
    SELECT conname INTO primary_key_name
    FROM pg_constraint
    WHERE conrelid = 'streaming.transaction_window_results'::REGCLASS
      AND contype = 'p';

    IF primary_key_name IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE streaming.transaction_window_results DROP CONSTRAINT %I',
            primary_key_name
        );
    END IF;
END;
$$;

ALTER TABLE streaming.transaction_window_results
    ADD CONSTRAINT transaction_window_results_pkey
    PRIMARY KEY (topic, window_start, window_end, merchant_category);

COMMIT;
