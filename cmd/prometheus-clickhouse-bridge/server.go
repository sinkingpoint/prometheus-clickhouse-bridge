package main

import (
	"net/http"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gorilla/mux"
	"github.com/sinkingpoint/prometheus-clickhouse-bridge/cmd/prometheus-clickhouse-bridge/handlers"
)

func runServer(clickhouseConn driver.Conn, listen string) error {
	router := mux.NewRouter()
	router.Handle("/api/write", handlers.NewRemoteWriteHandler(clickhouseConn)).Methods(http.MethodPost)
	router.Handle("/api/read", handlers.NewRemoteReadHandler(clickhouseConn))

	return http.ListenAndServe(listen, router)
}
