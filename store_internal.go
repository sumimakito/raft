package raft

type internalStore struct {
	LogStore
	StateStore
}

func newInternalStore() (*internalStore, error) {
	logStore := newInternalLogStore()
	stateStore := newInternalStateStore()
	return &internalStore{LogStore: logStore, StateStore: stateStore}, nil
}
