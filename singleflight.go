package raft

import "sync/atomic"

// SingleFlight executes the function only once on the first Do() call,
// returns the function return value and save it for future Do() calls.
type SingleFlight[T any] struct {
	c atomic.Value
	v T
}

func (s *SingleFlight[T]) Do(f func() T) T {
	c := make(chan struct{}, 1)
	if s.c.CompareAndSwap(nil, c) {
		s.v = f()
	} else {
		<-s.c.Load().(chan struct{})
	}
	close(c)
	return s.v
}
