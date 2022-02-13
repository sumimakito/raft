package raft

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFutureNoError(t *testing.T) {
	future := newFuture[int]()
	go func() {
		future.setResult(128, nil)
	}()
	r, err := future.Result()
	assert.NoError(t, err)
	assert.Equal(t, 128, r)
}

func TestFutureWithError(t *testing.T) {
	future := newFuture[any]()
	e := errors.New("error")
	go func() {
		future.setResult(nil, e)
	}()
	_, err := future.Result()
	assert.ErrorIs(t, err, e)
	assert.Nil(t, nil)
}
