package main

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/alecthomas/kong"
	"github.com/rs/zerolog/log"
)

var CLI struct {
	ClickhouseDSN string `help:"The DSN to connect to Clickhouse with" default:"localhost:9000"`
	Provision     struct {
		File string `help:"The path to the SQL file to provision the database with" default:"./provision.sql"`
	} `cmd:"" help:"Provision the metrics table into a Clickhouse database"`
	Server struct {
		Listen string `default:"0.0.0.0:4278"`
	} `cmd:"" help:"Start the remote Read/Write server"`
}

func main() {
	ctx := kong.Parse(&CLI)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{CLI.ClickhouseDSN},
	})

	switch ctx.Command() {
	case "provision":
		if err := provision(conn, CLI.Provision.File); err != nil {
			log.Error().Err(err).Msg("failed provisioning")
		}
	case "server":
		if runServer(conn, CLI.Server.Listen); err != nil {
			log.Error().Err(err).Msg("failed running server")
		}
	default:
		panic("BUG: unhandled command: " + ctx.Command())
	}

	fmt.Println("Exiting")
}
