package raft

import (
	"sync"
	"sync/atomic"
)

type futureResult struct {
	value interface{}
	err   error
}

// Future represents an async task with an undetermined result.
type Future interface {
	Result() (interface{}, error)
	setResult(value interface{}, err error)
}

type anyFuture struct {
	result      atomic.Value // futureResult
	mu          sync.Mutex   // protects subscribers
	subscribers []chan futureResult
}

func newFuture() Future {
	return &anyFuture{subscribers: []chan futureResult{}}
}

func (f *anyFuture) Result() (interface{}, error) {
	if result, ok := f.result.Load().(futureResult); ok {
		return result.value, result.err
	}
	ch := make(chan futureResult, 1)
	f.mu.Lock()
	if f.subscribers == nil {
		// The result has been set and fanned out to previous subscribers
		f.mu.Unlock()
		// Here the result will not be nil
		result := f.result.Load().(futureResult)
		return result.value, result.err
	}
	f.subscribers = append(f.subscribers, ch)
	f.mu.Unlock()
	result := <-ch
	return result.value, result.err
}

func (f *anyFuture) setResult(value interface{}, err error) {
	if !f.result.CompareAndSwap(nil, futureResult{value: value, err: err}) {
		// Result has been set by previous calls.
		return
	}
	result := f.result.Load().(futureResult)
	f.mu.Lock()
	defer f.mu.Unlock()
	// Fan out to subscribers.
	for _, subscriber := range f.subscribers {
		subscriber <- result
		close(subscriber)
	}
	// Set subscribers to nil since future subscribers are not accepted.
	f.subscribers = nil
}

// newErrorFuture returns an anyFuture that only has an error set as result
func newErrorFuture(err error) Future {
	f := newFuture()
	f.setResult(nil, err)
	return f
}

type FutureTask interface {
	Future
	Task() interface{}
}

type anyFutureTask struct {
	Future
	task interface{}
}

func newFutureTask(task interface{}) FutureTask {
	return &anyFutureTask{Future: newFuture(), task: task}
}

func (t *anyFutureTask) Task() interface{} {
	return t.task
}
