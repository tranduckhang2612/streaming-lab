\set ON_ERROR_STOP on

DO $$
BEGIN
    IF (SELECT COUNT(*) FROM streaming.legacy_verification_events) <> 1
       OR (SELECT COUNT(*) FROM streaming.legacy_window_ground_truth) <> 1
       OR (SELECT COUNT(*) FROM streaming.legacy_window_accuracy_results) <> 1
       OR to_regclass('streaming.verification_events') IS NULL
       OR to_regclass('streaming.window_ground_truth') IS NULL
       OR to_regclass('streaming.window_accuracy_results') IS NULL THEN
        RAISE EXCEPTION 'legacy accuracy migration did not preserve evidence';
    END IF;
END;
$$;
