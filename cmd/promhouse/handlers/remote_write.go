package handlers

import (
	"database/sql"
	"io"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	ch "github.com/mailru/go-clickhouse/v2"
	"github.com/prometheus/prometheus/prompb"
)

type RemoteWriteHandler struct {
	clickhouseConn *sql.DB
}

func NewRemoteWriteHandler(conn *sql.DB) *RemoteWriteHandler {
	return &RemoteWriteHandler{
		clickhouseConn: conn,
	}
}

func (h *RemoteWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}

	if encoding := r.Header.Get("Content-Encoding"); encoding != "" {
		switch encoding {
		case "snappy":
			decoded, err := snappy.Decode(nil, requestBytes)
			if err != nil {
				http.Error(w, "failed to decode snappy", http.StatusBadRequest)
				return
			}

			requestBytes = decoded
		default:
			http.Error(w, "invalid content-encoding: "+encoding, http.StatusBadRequest)
			return
		}
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
	txn, err := h.clickhouseConn.Begin()
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare("INSERT INTO metrics (timestamp, name, value, tags) VALUES (?, ?, ?, ?);")
	if err != nil {
		return err
	}

	for i := range writeReq.Timeseries {
		timeseries := writeReq.Timeseries[i]
		name, labels, err := labelsToJSON(timeseries.Labels)
		if err != nil {
			return err
		}

		for _, series := range timeseries.Samples {
			if _, err := stmt.Exec(series.Timestamp, name, series.Value, ch.Map(labels)); err != nil {
				return err
			}
		}
	}

	return txn.Commit()
}
