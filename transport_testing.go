package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sumimakito/raft/pb"
)

func testingTransportServe(t *testing.T, trans Transport) {
	if server, ok := trans.(TransportServer); ok {
		assert.NoError(t, server.Serve())
	}
}

func testingTransportRPCResponder(rpcCh <-chan *RPC) (stopCh chan struct{}) {
	stopCh = make(chan struct{}, 1)
	go func() {
		for {
			select {
			case rpc := <-rpcCh:
				switch rpc.Request().(type) {
				case *pb.AppendEntriesRequest:
					rpc.Respond(&pb.AppendEntriesResponse{}, nil)
				case *pb.RequestVoteRequest:
					rpc.Respond(&pb.RequestVoteResponse{}, nil)
				case *InstallSnapshotRequest:
					rpc.Respond(&pb.InstallSnapshotResponse{}, nil)
				case *pb.ApplyLogRequest:
					rpc.Respond(&pb.ApplyLogResponse{}, nil)
				default:
					rpc.Respond(nil, ErrUnknownRPC)
				}
			case <-stopCh:
				return
			}
		}
	}()
	return stopCh
}
