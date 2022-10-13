package handlers

import (
	"database/sql"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

// RemoteReadHandler is an http.Handler that processes RemoteRead requests
type RemoteReadHandler struct {
	clickhouseConn *sql.DB
}

func NewRemoteReadHandler(clickhouseConn *sql.DB) *RemoteReadHandler {
	return &RemoteReadHandler{
		clickhouseConn: clickhouseConn,
	}
}

func (h *RemoteReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestBytes, err := GetDecompressedBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(requestBytes, &req); err != nil {
		http.Error(w, "failed to parse request body", http.StatusBadRequest)
		return
	}

	response, err := h.handleRemoteRead(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	bytes, err := proto.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	bytes = snappy.Encode(nil, bytes)
	w.Header().Add("Content-Encoding", "snappy")

	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

// mapToLabels converts a map[string]string (or a prometheus labelset)
// into a slice of prompb.Label s, that can be inserted into protobuf responses
func mapToLabels(labels model.LabelSet, name string) []prompb.Label {
	keys := []string{"__name__"}
	for k := range labels {
		keys = append(keys, string(k))
	}

	// Sort the keys so that our resulting array
	// is always in alphabetical order by key
	sort.Strings(keys)

	outputLabels := make([]prompb.Label, 0, len(labels))
	for _, k := range keys {
		if k == "__name__" {
			outputLabels = append(outputLabels, prompb.Label{
				Name:  k,
				Value: name,
			})
		} else {
			outputLabels = append(outputLabels, prompb.Label{
				Name:  k,
				Value: string(labels[model.LabelName(k)]),
			})
		}
	}

	return outputLabels
}

// mapIntoLabelset does a conversion from a map[string]string
// into a prometheus labelset (which it technically already is)
func mapIntoLabelset(input map[string]string) model.LabelSet {
	ls := model.LabelSet{}
	for k, v := range input {
		ls[model.LabelName(k)] = model.LabelValue(v)
	}

	return ls
}

// handleRemoteRead takes a protobuf ReadRequest and returns a protobuf ReadResponse
// that can be returned to the client
func (h *RemoteReadHandler) handleRemoteRead(req prompb.ReadRequest) (*prompb.ReadResponse, error) {
	queryResults := []*prompb.QueryResult{}
	for i := range req.Queries {
		query := req.Queries[i]
		wheres := []string{}
		whereArgs := []interface{}{}
		for _, matcher := range query.Matchers {
			key := matcher.Name
			value := matcher.Value

			if key == "__name__" {
				key = "name"
			} else {
				whereArgs = append(whereArgs, key)
				key = "tags[?]"
			}

			switch matcher.Type {
			case prompb.LabelMatcher_EQ:
				wheres = append(wheres, fmt.Sprintf("%s = ?", key))
				whereArgs = append(whereArgs, value)
			case prompb.LabelMatcher_NEQ:
				wheres = append(wheres, fmt.Sprintf("%s != ?", key))
				whereArgs = append(whereArgs, value)
			case prompb.LabelMatcher_RE:
				wheres = append(wheres, "match(%s, ?)", key)
				whereArgs = append(whereArgs, value)
			case prompb.LabelMatcher_NRE:
				wheres = append(wheres, "NOT match(%s, ?)", key)
				whereArgs = append(whereArgs, value)
			}
		}

		wheres = append(wheres, "timestamp >= ? AND timestamp <= ?")
		whereArgs = append(whereArgs, query.StartTimestampMs/1000, query.EndTimestampMs/1000)

		queryString := fmt.Sprintf("SELECT timestamp, name, tags, value FROM metrics WHERE %s ORDER BY name, tags, timestamp", strings.Join(wheres, " AND "))
		rows, err := h.clickhouseConn.Query(queryString, whereArgs...)
		if err != nil {
			return nil, err
		}

		defer rows.Close()

		var timestamp time.Time
		var name string
		var labels map[string]string
		var value float64

		timeseriesMap := make(map[model.Fingerprint]*prompb.TimeSeries)

		for rows.Next() {
			if err := rows.Scan(&timestamp, &name, &labels, &value); err != nil {
				return nil, err
			}

			promLabels := mapIntoLabelset(labels)

			fingerprint := promLabels.Fingerprint()
			if _, ok := timeseriesMap[fingerprint]; !ok {
				timeseriesMap[fingerprint] = &prompb.TimeSeries{
					Labels:  mapToLabels(promLabels, name),
					Samples: []prompb.Sample{},
				}
			}

			current := timeseriesMap[fingerprint]
			current.Samples = append(timeseriesMap[fingerprint].Samples, prompb.Sample{
				Timestamp: timestamp.UnixMilli(),
				Value:     value,
			})

			timeseriesMap[fingerprint] = current
			for k := range labels {
				delete(labels, k)
			}
		}

		timeseries := []*prompb.TimeSeries{}
		for _, ts := range timeseriesMap {
			timeseries = append(timeseries, ts)
		}

		queryResults = append(queryResults, &prompb.QueryResult{
			Timeseries: timeseries,
		})
	}

	return &prompb.ReadResponse{
		Results: queryResults,
	}, nil
}
