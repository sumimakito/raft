package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/sumimakito/raft"
	"go.uber.org/zap"
)

type APIExtension struct {
	logger *zap.Logger
}

func NewAPIExtension(logger *zap.Logger) *APIExtension {
	return &APIExtension{logger: logger}
}

func (e *APIExtension) Setup(s *raft.Server, r *mux.Router) error {
	r.HandleFunc("/keys", func(rw http.ResponseWriter, r *http.Request) {
		h := raft.NewHandyRespWriter(rw, e.logger)
		h.Encoded(s.StateMachine().(*KVSM).Keys(), raft.HandyEncodingJSON, 0)
	}).Methods("GET")

	r.HandleFunc("/keys/{key}", func(rw http.ResponseWriter, r *http.Request) {
		h := raft.NewHandyRespWriter(rw, e.logger)
		var encoding raft.HandyEncoding
		switch r.URL.Query().Get("encoding") {
		case string(raft.HandyEncodingBase64), "":
			encoding = raft.HandyEncodingBase64
		case string(raft.HandyEncodingRaw):
			encoding = raft.HandyEncodingRaw
		default:
			h.WriteHeader(http.StatusBadRequest)
			return
		}
		vars := mux.Vars(r)
		v, ok := s.StateMachine().(*KVSM).Value(vars["key"])
		if !ok {
			h.WriteHeader(http.StatusNotFound)
			return
		}
		h.Encoded(v, encoding, 0)
	}).Methods("GET")

	r.HandleFunc("/keys/{key}", func(rw http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		key := vars["key"]
		value, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Println(err)
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		c := Command{Type: CommandSet, Key: key, Value: value}
		f := s.ApplyCommand(context.Background(), c.Encode())
		result, err := f.Result()
		if err != nil {
			log.Println(err)
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		respBody, err := json.Marshal(result)
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		if _, err := rw.Write(respBody); err != nil {
			log.Println(err)
		}
	}).Methods("PUT")

	r.HandleFunc("/keys/{key}", func(rw http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		key := vars["key"]
		c := Command{Type: CommandUnset, Key: key}
		f := s.ApplyCommand(context.Background(), c.Encode())
		result, err := f.Result()
		if err != nil {
			log.Println(err)
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		respBody, err := json.Marshal(result)
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		if _, err := rw.Write(respBody); err != nil {
			log.Println(err)
		}
	}).Methods("DELETE")

	r.HandleFunc("/keyvalues", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		snapshot := s.StateMachine().(*KVSM).KeyValues()
		out, err := json.Marshal(snapshot)
		if err != nil {
			log.Println(err)
		}
		if _, err := rw.Write(out); err != nil {
			log.Println(err)
		}
	}).Methods("GET")

	return nil
}
