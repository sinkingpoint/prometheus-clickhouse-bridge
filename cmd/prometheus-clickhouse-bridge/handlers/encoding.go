package handlers

import (
	"fmt"
	"io"
	"net/http"

	"github.com/golang/snappy"
)

func Decompress(contentEncoding string, bytes []byte) ([]byte, error) {
	if contentEncoding == "" {
		return bytes, nil
	}

	switch contentEncoding {
	case "snappy":
		decoded, err := snappy.Decode(nil, bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to decode bytes as snappy: %s", err.Error())
		}

		return decoded, nil
	}

	return nil, fmt.Errorf("unknown encoding: %q", contentEncoding)
}

func GetDecompressedBody(r *http.Request) ([]byte, error) {
	bytes, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	return Decompress(r.Header.Get("Content-Encoding"), bytes)
}
