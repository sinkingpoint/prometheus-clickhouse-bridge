  CREATE TABLE IF NOT EXISTS metrics (
      timestamp DateTime CODEC(DoubleDelta, ZSTD),
      fingerprint UInt64,
      value Float64 CODEC (Gorilla, ZSTD),
  ) ENGINE = MergeTree() PRIMARY KEY (timestamp, fingerprint);

CREATE TABLE IF NOT EXISTS labels (
  fingerprint UInt64,
  labels Nested (
    key LowCardinality(String),
    value LowCardinality(String)
  ) CODEC(ZSTD)
) ENGINE = ReplacingMergeTree() ORDER BY fingerprint;