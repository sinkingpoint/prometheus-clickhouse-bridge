package handlers

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"

	"github.com/gogo/protobuf/proto"
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

func labelsToJSON(labels []prompb.Label) (string, error) {
	labelsMap := make(map[string]string)
	for _, label := range labels {
		labelsMap[label.Name] = label.Value
	}

	labelsJSON, err := json.Marshal(labelsMap)
	if err != nil {
		return "", err
	}

	return string(labelsJSON), nil
}

func (h *RemoteWriteHandler) handleRemoteWrite(writeReq prompb.WriteRequest) error {
	txn, err := h.clickhouseConn.Begin()
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare("INSERT INTO metrics VALUES (?, ?, ?, ?);")
	if err != nil {
		return err
	}

	for i := range writeReq.Timeseries {
		meta := writeReq.Metadata[i]
		timeseries := writeReq.Timeseries[i]
		metricName := meta.MetricFamilyName
		labels, err := labelsToJSON(timeseries.Labels)
		if err != nil {
			return err
		}

		for _, series := range timeseries.Samples {
			if _, err := stmt.Exec(series.Timestamp, metricName, series.Value, labels); err != nil {
				return err
			}
		}
	}

	return txn.Commit()
}
