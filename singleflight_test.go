package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSingleFlight(t *testing.T) {
	var s SingleFlight[int]
	i := 1
	f := func() int { return i }
	assert.Equal(t, 1, s.Do(f))
	i = 2
	assert.Equal(t, 1, s.Do(f))
}
