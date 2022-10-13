package main

import (
	"database/sql"
	"embed"
	"io/fs"
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

func provision(clickhouseConn *sql.DB) error {
	sqls, err := getProvisionSQLs()
	if err != nil {
		return err
	}

	txn, err := clickhouseConn.Begin()
	if err != nil {
		return err
	}

	for _, sql := range sqls {
		if _, err := txn.Exec(sql); err != nil {
			return err
		}
	}

	if err = txn.Commit(); err != nil {
		return err
	}

	return nil
}
