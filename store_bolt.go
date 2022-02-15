package raft

import "go.etcd.io/bbolt"

type BoltStore struct {
	LogStore
	StateStore
}

func NewBoltStore(path string) (*BoltStore, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	logStore := NewBoltLogStore(db)
	stateStore := NewBoltStateStore(db)
	return &BoltStore{LogStore: logStore, StateStore: stateStore}, nil
}
