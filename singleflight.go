package raft

import "sync/atomic"

// SingleFlight executes the function only once on the first Do() call,
// returns the function return value and save it for future Do() calls.
type SingleFlight struct {
	c atomic.Value
	v interface{}
}

func (s *SingleFlight) Do(f func() interface{}) interface{} {
	c := make(chan struct{}, 1)
	if s.c.CompareAndSwap(nil, c) {
		s.v = f()
	} else {
		<-s.c.Load().(chan struct{})
	}
	close(c)
	return s.v
}
