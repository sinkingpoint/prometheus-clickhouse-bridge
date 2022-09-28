CREATE TABLE IF NOT EXISTS metrics (
    timestamp DateTime,
    name LowCardinality(String),
    value Float64,
    tags JSON
) ENGINE = MergeTree() PRIMARY KEY (timestamp, name);