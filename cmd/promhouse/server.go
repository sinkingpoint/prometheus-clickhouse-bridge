package main

import (
	"database/sql"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/sinkingpoint/promhouse/cmd/promhouse/handlers"
)

func runServer(clickhouseConn *sql.DB, listen string) error {
	router := mux.NewRouter()

	router.Handle("/api/write", handlers.NewRemoteWriteHandler(clickhouseConn)).Methods(http.MethodPost)

	return http.ListenAndServe(listen, router)
}
