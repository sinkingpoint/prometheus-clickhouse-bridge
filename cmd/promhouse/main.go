package main

import (
	"io"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/prompb"
)

func main() {

}

type ProtoPointer[T any] interface {
	*T
	proto.Message
}

var RemoteReadHandler = ProtoHandler[prompb.ReadRequest, prompb.ReadResponse]
var RemoteWriteHandler = ProtoHandler[prompb.WriteRequest, prompb.ReadResponse]

func ProtoHandler[Request any, Response any, RequestPtr ProtoPointer[Request], ResponsePtr ProtoPointer[Response]](handler func(req Request) (*Response, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}

		var readRequest Request
		if err := proto.Unmarshal(requestBytes, RequestPtr(&readRequest)); err != nil {
			http.Error(w, "failed to parse request body", http.StatusBadRequest)
			return
		}

		readRequestResponse, err := handler(readRequest)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		responseBytes, err := proto.Marshal(ResponsePtr(readRequestResponse))
		if err != nil {
			http.Error(w, "failed to marshal response", http.StatusInternalServerError)
			return
		}

		w.Header().Add("Content-Type", "application/protobuf")
		w.WriteHeader(http.StatusOK)
		w.Write(responseBytes)
	}
}
