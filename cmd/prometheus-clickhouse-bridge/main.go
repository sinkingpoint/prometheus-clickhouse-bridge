package main

import (
	"database/sql"

	"github.com/alecthomas/kong"
	_ "github.com/mailru/go-clickhouse/v2"
	"github.com/rs/zerolog/log"
)

var CLI struct {
	ClickhouseDSN string `help:"The DSN to connect to Clickhouse with" default:"http://localhost:8123"`
	Provision     struct {
	} `cmd:""`
	Server struct {
		Listen string `arg:"" default:"0.0.0.0:4278"`
	} `cmd:""`
}

func main() {
	ctx := kong.Parse(&CLI)
	clickhouseConn, err := sql.Open("chhttp", CLI.ClickhouseDSN)
	if err != nil {
		log.Error().Err(err).Msg("failed to connect to Clickhouse")
		return
	}

	if err := clickhouseConn.Ping(); err != nil {
		log.Error().Err(err).Msg("failed to ping Clickhouse")
		return
	}

	switch ctx.Command() {
	case "provision":
		if err := provision(clickhouseConn); err != nil {
			log.Error().Err(err).Msg("failed provisioning")
		}
	case "server":
		if runServer(clickhouseConn, CLI.Server.Listen); err != nil {
			log.Error().Err(err).Msg("failed running server")
		}
	default:
		panic("BUG: unhandled command: " + ctx.Command())
	}
}
