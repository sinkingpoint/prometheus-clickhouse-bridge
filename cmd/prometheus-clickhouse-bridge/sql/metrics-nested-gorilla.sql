CREATE TABLE IF NOT EXISTS metrics (
    timestamp DateTime CODEC(DoubleDelta, ZSTD),
    name String CODEC(ZSTD),
    tags Nested (
      key LowCardinality(String),
      value LowCardinality(String)
    ) CODEC(ZSTD),
    value Float64 CODEC (Gorilla, ZSTD),
) ENGINE = MergeTree() PRIMARY KEY (name, tags.key, tags.value, timestamp);
