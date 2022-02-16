package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func ƒAssertError2(v any, err error) func(t *testing.T) {
	return func(t *testing.T) {
		assert.Error(t, err)
	}
}

func ƒAssertNoError2[T any](v T, err error) func(t *testing.T) T {
	return func(t *testing.T) T {
		assert.NoError(t, err)
		return v
	}
}
