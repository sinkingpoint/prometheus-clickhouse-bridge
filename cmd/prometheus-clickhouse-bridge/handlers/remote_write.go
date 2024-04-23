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

func labelsToJSON(labels []prompb.Label) (string, map[string]string, error) {
	labelsMap := make(map[string]string)
	name := ""
	for _, label := range labels {
		if label.Name == "__name__" {
			name = label.Value
			continue
		}

		labelsMap[label.Name] = label.Value
	}

	return name, labelsMap, nil
}

func (h *RemoteWriteHandler) handleRemoteWrite(writeReq prompb.WriteRequest) error {
	batch, err := h.clickhouseConn.PrepareBatch(context.Background(), "INSERT INTO metrics (timestamp, name, value, tags) VALUES (?, ?, ?, ?);")
	if err != nil {
		return err
	}

	rowCount := 0

	for i := range writeReq.Timeseries {
		timeseries := writeReq.Timeseries[i]
		name, labels, err := labelsToJSON(timeseries.Labels)
		if err != nil {
			return err
		}

		for _, series := range timeseries.Samples {
			rowCount += 1
			if err := batch.Append(series.Timestamp/1000, name, series.Value, labels); err != nil {
				return err
			}
		}
	}

	fmt.Println("Committing transaction with", rowCount, " rows")
	return batch.Send()
}
