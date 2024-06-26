CREATE TABLE IF NOT EXISTS metrics (
    timestamp DateTime CODEC(DoubleDelta, ZSTD),
    name String CODEC(ZSTD),
    tags Nested (
      key String,
      value String
    ) CODEC(ZSTD),
    value Float64 CODEC (DoubleDelta, ZSTD),
) ENGINE = MergeTree() PRIMARY KEY (name, tags.key, tags.value, timestamp);
