#!/bin/bash

docker run --rm -d -v $(pwd)/testdata/prometheus-bench.yml:/etc/prometheus/prometheus.yml --net host --name prometheus-bench prom/prometheus --config.file /etc/prometheus/prometheus.yml --web.listen-address 0.0.0.0:9091 --enable-feature=remote-write-receiver
docker run --rm -v $(pwd)/testdata/prometheus.yml:/etc/prometheus/prometheus.yml --net host --name prometheus prom/prometheus
