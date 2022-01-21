package raft

import "sync/atomic"

type SingleFlight struct {
	c atomic.Value
	v interface{}
}

func (s *SingleFlight) Do(f func() interface{}) interface{} {
	c := make(chan struct{}, 1)
	if s.c.CompareAndSwap(nil, c) {
		s.v = f()
		close(c)
	} else {
		<-s.c.Load().(chan struct{})
	}
	return s.v
}
