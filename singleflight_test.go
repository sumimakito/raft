package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSingleFlight(t *testing.T) {
	var s SingleFlight
	i := 1
	f := func() interface{} { return i }
	assert.Equal(t, 1, s.Do(f).(int))
	i = 2
	assert.Equal(t, 1, s.Do(f).(int))
}
