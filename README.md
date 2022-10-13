# Prometheus Clickhouse Bridge

This is a Go Daemon that provides a Prometheus Remote Write/Read interface on top of a Clickhouse database.

## Usage

```
Usage: prometheus-clickhouse-bridge <command>

Flags:
  -h, --help                                      Show context-sensitive help.
      --clickhouse-dsn="http://localhost:8123"    The DSN to connect to Clickhouse with

Commands:
  provision
    Provision the metrics table into a Clickhouse database

  server
    Start the remote Read/Write server
```

## Running

In order to run the bridge you'll need a Clickhouse server running. As a first step, you'll need to provision the metrics table into your Clickhouse database with the provision command:

```
prometheus-clickhouse-bridge provision
```

This command assumes that your Clickhouse server is running on localhost:8123. If it isn't, then you can provide a different Clickhouse address with the `--clickhouse-dsn` flag.

Once you've provisioned the table, then you're all set to start ingesting metrics. Run the server with:

```
prometheus-clickhouse-bridge server
```

once again, setting the `--clickhouse-dsn` flag if necessary.

You can then configure Prometheus to send remote read and write requests to the bridge:

```yaml
remote_write:
  - url: http://localhost:4278/api/write
remote_read:
  - url: http://localhost:4278/api/read
```

## Limitations

There's a few limitations here, that I might iron out in the future:

### We only support the single node version of Clickhouse

We provision a table with the `MergeTree` engine, which does not take advantage of Clickhouse's clustering abilities. We could move to a DistributedMergeTree and Replicated tables.

### We don't support Exemplars

The bridge is only concerned with metrics at this point. Any exemplars in the pushes will be dropped, and remote read requests will never return exemplars.
