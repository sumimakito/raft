package raft

import (
	"encoding/base64"
	"encoding/json"
	"net/http"

	"go.uber.org/zap"
)

type HandyEncoding string

const (
	HandyEncodingJSON   HandyEncoding = "json"
	HandyEncodingBase64 HandyEncoding = "base64"
	HandyEncodingRaw    HandyEncoding = "raw"
)

type HandyRespWriter struct {
	logger *zap.Logger
	http.ResponseWriter
}

func NewHandyRespWriter(w http.ResponseWriter, logger *zap.Logger) (h HandyRespWriter) {
	h.ResponseWriter = w
	h.logger = logger
	return
}

func (rw *HandyRespWriter) Encoded(v interface{}, e HandyEncoding, statusCode int) {
	if http.StatusText(statusCode) == "" {
		statusCode = http.StatusOK
	}
	if v == nil {
		rw.WriteHeader(statusCode)
		return
	}
	var respBody []byte
	switch e {
	case HandyEncodingJSON:
		if b, err := json.Marshal(v); err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			rw.logger.Warn("error occurred marshaling JSON", zap.Error(err))
			return
		} else {
			respBody = b
		}
	case HandyEncodingBase64:
		if b, ok := v.([]byte); !ok {
			rw.WriteHeader(http.StatusInternalServerError)
			rw.logger.Warn("base64 encoding requires v to be []byte")
			return
		} else {
			respBody = []byte(base64.StdEncoding.EncodeToString(b))
		}
	case HandyEncodingRaw:
		if b, ok := v.([]byte); !ok {
			rw.WriteHeader(http.StatusInternalServerError)
			rw.logger.Warn("raw encoding requires v to be []byte")
			return
		} else {
			respBody = b
		}
	}
	switch e {
	case HandyEncodingJSON:
		rw.Header().Set("Content-Type", "application/json")
	case HandyEncodingBase64, HandyEncodingRaw:
		rw.Header().Set("Content-Type", "text/plain")
	}
	if _, err := rw.Write(respBody); err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.logger.Warn("error occurred writing response body", zap.Error(err))
		return
	}
}

func (rw *HandyRespWriter) Error(err error) {
	rw.WriteHeader(http.StatusInternalServerError)
	rw.logger.Warn("internal error while processing the request", zap.Error(err))
}

func (rw *HandyRespWriter) JSON(v interface{}) {
	body, err := json.Marshal(v)
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.logger.Warn("error occurred marshaling JSON", zap.Error(err))
		return
	}
	if v == nil {
		rw.WriteHeader(http.StatusOK)
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	if _, err := rw.Write(body); err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.logger.Warn("error occurred writing response body", zap.Error(err))
		return
	}
}

func (rw *HandyRespWriter) JSONStatus(v interface{}, statusCode int) {
	body, err := json.Marshal(v)
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.logger.Warn("error occurred marshaling JSON", zap.Error(err))
		return
	}
	if http.StatusText(statusCode) == "" {
		statusCode = http.StatusOK
	}
	if v == nil {
		rw.WriteHeader(statusCode)
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(statusCode)
	if _, err := rw.Write(body); err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.logger.Warn("error occurred writing response body", zap.Error(err))
		return
	}
}

func (rw *HandyRespWriter) JSONFunc(fn func() (v interface{}, statusCode int, err error)) {
	v, statusCode, err := fn()
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.logger.Warn("function passed to JSONFunc returned with an error", zap.Error(err))
		return
	}
	if http.StatusText(statusCode) == "" {
		statusCode = http.StatusOK
	}
	if v == nil {
		rw.WriteHeader(statusCode)
		return
	}
	respBody, err := json.Marshal(v)
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.logger.Warn("error occurred marshaling JSON", zap.Error(err))
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(statusCode)
	if _, err := rw.Write(respBody); err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.logger.Warn("error occurred writing response body", zap.Error(err))
		return
	}
}
