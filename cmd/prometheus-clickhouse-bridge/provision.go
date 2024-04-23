package main

import (
	"context"
	"embed"
	"io/fs"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

//go:embed sql/*.sql
var sqlTemplates embed.FS

func getProvisionSQLs() ([]string, error) {
	sqls := []string{}
	err := fs.WalkDir(sqlTemplates, ".", func(path string, d fs.DirEntry, err error) error {
		if d.Type().IsRegular() {
			sql, err := sqlTemplates.ReadFile("sql/" + d.Name())
			if err != nil {
				return err
			}

			sqls = append(sqls, string(sql))
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return sqls, nil
}

func provision(conn driver.Conn) error {
	sqls, err := getProvisionSQLs()
	if err != nil {
		return err
	}

	for _, sql := range sqls {
		if err := conn.Exec(context.Background(), sql); err != nil {
			return err
		}
	}

	return nil
}
