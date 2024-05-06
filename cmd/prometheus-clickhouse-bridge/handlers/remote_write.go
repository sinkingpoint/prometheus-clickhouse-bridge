package handlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/prompb"
)

type RemoteWriteHandler struct {
	clickhouseConn driver.Conn
}

func NewRemoteWriteHandler(conn driver.Conn) *RemoteWriteHandler {
	return &RemoteWriteHandler{
		clickhouseConn: conn,
	}
}

func (h *RemoteWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestBytes, err := GetDecompressedBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(requestBytes, &req); err != nil {
		http.Error(w, "failed to parse request body", http.StatusBadRequest)
		return
	}

	if err := h.handleRemoteWrite(req); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Println("Successfully wrote", len(req.Timeseries), "timeseries")

	w.WriteHeader(http.StatusNoContent)
}

func labelsToJSON(labels []prompb.Label) (string, map[string]string) {
	labelsMap := make(map[string]string)
	name := ""
	for _, label := range labels {
		if label.Name == "__name__" {
			name = label.Value
			continue
		}

		labelsMap[label.Name] = label.Value
	}

	return name, labelsMap
}

func labelsToNested(labels []prompb.Label) (string, []string, []string) {
	keys := make([]string, 0)
	values := make([]string, 0)
	name := ""
	for _, label := range labels {
		if label.Name == "__name__" {
			name = label.Value
			continue
		}

		keys = append(keys, label.Name)
		values = append(values, label.Value)
	}

	return name, keys, values
}

func (h *RemoteWriteHandler) handleRemoteWrite(writeReq prompb.WriteRequest) error {
	batch, err := h.clickhouseConn.PrepareBatch(context.Background(), "INSERT INTO metrics (timestamp, name, value, tags.key, tags.value) VALUES (?, ?, ?, ?);")
	if err != nil {
		return err
	}

	rowCount := 0

	for i := range writeReq.Timeseries {
		timeseries := writeReq.Timeseries[i]
		name, keys, values := labelsToNested(timeseries.Labels)

		for _, series := range timeseries.Samples {
			rowCount += 1
			if err := batch.Append(series.Timestamp/1000, name, series.Value, keys, values); err != nil {
				batch.Abort()
				return err
			}
		}

		if rowCount > 1000000 {
			fmt.Println("Committing transaction with", rowCount, " rows")
			if err := batch.Send(); err != nil {
				batch.Abort()
				return err
			}

			batch, err = h.clickhouseConn.PrepareBatch(context.Background(), "INSERT INTO metrics (timestamp, name, value, tags.key, tags.value) VALUES (?, ?, ?, ?);")
			if err != nil {
				return err
			}

			rowCount = 0
		}
	}

	fmt.Println("Committing transaction with", rowCount, " rows")
	if err := batch.Send(); err != nil {
		batch.Abort()
		return err
	}

	return nil
}
