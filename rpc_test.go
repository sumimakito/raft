package raft

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRPC(t *testing.T) {
	type testRequest struct{}
	type testResponse struct{}
	rpc := NewRPC(context.Background(), &testRequest{})
	rpc.Respond(&testResponse{}, nil)
	resp := ƒAssertNoError2(rpc.Response())(t)
	assert.IsType(t, &testResponse{}, resp)
}
