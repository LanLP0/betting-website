CREATE DATABASE IF NOT EXISTS metrics_schema;

CREATE TABLE IF NOT EXISTS metrics_schema.events_log (
    timestamp DateTime64(3) DEFAULT now(),
    event_id String NOT NULL,
    event_type String NOT NULL,
    value1 Dynamic, -- Depending on event_type, value 1 -> 3 can be specified for fast querying
    value2 Dynamic,
    value3 Dynamic,
    payload String DEFAULT '',
    trace_id String DEFAULT ''
) ENGINE = MergeTree()
ORDER BY (timestamp, event_id, event_type);
