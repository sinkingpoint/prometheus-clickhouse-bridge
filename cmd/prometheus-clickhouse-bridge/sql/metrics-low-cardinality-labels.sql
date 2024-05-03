CREATE TABLE IF NOT EXISTS metrics (
    timestamp DateTime CODEC(DoubleDelta, ZSTD),
    name LowCardinality(String) CODEC(ZSTD),
    tags Map(LowCardinality(String), LowCardinality(String)) CODEC(ZSTD),
    value Float64 CODEC (DoubleDelta, ZSTD),
) ENGINE = MergeTree() PRIMARY KEY (name, tags, timestamp);
