CREATE TABLE IF NOT EXISTS metrics (
    timestamp DateTime CODEC(Delta(4), ZSTD),
    name LowCardinality(String) CODEC(ZSTD),
    tags Map(String, String) CODEC(ZSTD),
    value Float64 CODEC (Gorilla, ZSTD),
) ENGINE = MergeTree() PRIMARY KEY (name, tags, timestamp);
