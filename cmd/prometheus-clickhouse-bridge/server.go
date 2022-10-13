package main

import (
	"database/sql"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/sinkingpoint/prometheus-clickhouse-bridge/cmd/prometheus-clickhouse-bridge/handlers"
)

func runServer(clickhouseConn *sql.DB, listen string) error {
	router := mux.NewRouter()
	router.Handle("/api/write", handlers.NewRemoteWriteHandler(clickhouseConn)).Methods(http.MethodPost)
	router.Handle("/api/read", handlers.NewRemoteReadHandler(clickhouseConn))

	return http.ListenAndServe(listen, router)
}
