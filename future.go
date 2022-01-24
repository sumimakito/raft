package raft

import (
	"sync"
	"sync/atomic"
)

type futureResult[T any] struct {
	value T
	err   error
}

// Future represents an async task with an undetermined result.
type Future[T any] interface {
	Result() (T, error)
	setResult(value T, err error)
}

type anyFuture[T any] struct {
	result      atomic.Value // futureResult[T]
	mu          sync.Mutex   // protects subscribers
	subscribers []chan futureResult[T]
}

func newFuture[T any]() Future[T] {
	return &anyFuture[T]{subscribers: []chan futureResult[T]{}}
}

func (f *anyFuture[T]) Result() (T, error) {
	if result, ok := f.result.Load().(futureResult[T]); ok {
		return result.value, result.err
	}
	ch := make(chan futureResult[T], 1)
	f.mu.Lock()
	if f.subscribers == nil {
		// The result has been set and fanned out to previous subscribers
		f.mu.Unlock()
		// Here the result will not be nil
		result := f.result.Load().(futureResult[T])
		return result.value, result.err
	}
	f.subscribers = append(f.subscribers, ch)
	f.mu.Unlock()
	result := <-ch
	return result.value, result.err
}

func (f *anyFuture[T]) setResult(value T, err error) {
	if !f.result.CompareAndSwap(nil, futureResult[T]{value: value, err: err}) {
		// Result has been set by previous calls.
		return
	}
	result := f.result.Load().(futureResult[T])
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
func newErrorFuture(err error) Future[any] {
	f := newFuture[any]()
	f.setResult(nil, err)
	return f
}

type FutureTask[FUTURE any, TASK any] interface {
	Future[FUTURE]
	Task() TASK
}

type anyFutureTask[FUTURE any, TASK any] struct {
	Future[FUTURE]
	task TASK
}

func newFutureTask[FUTURE any, TASK any](task TASK) FutureTask[FUTURE, TASK] {
	return &anyFutureTask[FUTURE, TASK]{Future: newFuture[FUTURE](), task: task}
}

func (t *anyFutureTask[FUTURE, TASK]) Task() TASK {
	return t.task
}
