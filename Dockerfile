FROM golang:1.18.7-alpine as builder
COPY . /workdir
RUN cd /workdir && go build ./cmd/prometheus-clickhouse-bridge

FROM alpine:3
COPY --from=builder /workdir/prometheus-clickhouse-bridge /usr/bin/prometheus-clickhouse-bridge
RUN chmod +x /usr/bin/prometheus-clickhouse-bridge && chown 1000:1000 /usr/bin/prometheus-clickhouse-bridge
USER 1000
ENTRYPOINT [ "/usr/bin/prometheus-clickhouse-bridge" ]
