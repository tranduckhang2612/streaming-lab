\set ON_ERROR_STOP on

CREATE SCHEMA streaming;
CREATE TABLE streaming.verification_events (
    transaction_id UUID PRIMARY KEY
);
INSERT INTO streaming.verification_events
VALUES ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa');

CREATE TABLE streaming.window_ground_truth (
    legacy_id BIGINT PRIMARY KEY
);
INSERT INTO streaming.window_ground_truth VALUES (1);

CREATE TABLE streaming.window_accuracy_results (
    legacy_id BIGINT PRIMARY KEY
);
INSERT INTO streaming.window_accuracy_results VALUES (1);
