CREATE TABLE IF NOT EXISTS event_log (
    timestamp DateTime DEFAULT now(),
    event_type String,
    payload String
) ENGINE = MergeTree()
ORDER BY (timestamp, event_type);
