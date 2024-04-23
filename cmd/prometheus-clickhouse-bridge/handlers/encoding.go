package handlers

import (
	"fmt"
	"io"
	"net/http"

	"github.com/golang/snappy"
)

func decompress(contentEncoding string, bytes []byte) ([]byte, error) {
	if contentEncoding == "" {
		return bytes, nil
	}

	switch contentEncoding {
	case "snappy":
		return snappy.Decode(nil, bytes)
	}

	return nil, fmt.Errorf("unknown encoding: %q", contentEncoding)
}

func GetDecompressedBody(r *http.Request) ([]byte, error) {
	bytes, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	return decompress(r.Header.Get("Content-Encoding"), bytes)
}
