package main

import (
	"context"
	"embed"
	"io"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

//go:embed sql/*.sql
var sqlTemplates embed.FS

func getProvisionSQLs(fileName string) ([]byte, error) {
	file, err := sqlTemplates.Open(fileName)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(file)
}

func provision(conn driver.Conn, file string) error {
	sql, err := getProvisionSQLs(file)
	if err != nil {
		return err
	}

	if err := conn.Exec(context.Background(), string(sql)); err != nil {
		return err
	}

	return nil
}
