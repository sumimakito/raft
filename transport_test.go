package raft

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sumimakito/raft/pb"
)

func testTransport(t *testing.T, transFn func(peer *pb.Peer) (Transport, error), peerFn func() (*pb.Peer, error)) {
	peer1 := ƒAssertNoError2(peerFn())(t)
	peer2 := ƒAssertNoError2(peerFn())(t)
	trans1 := ƒAssertNoError2(transFn(peer1))(t)
	trans2 := ƒAssertNoError2(transFn(peer2))(t)

	testingTransportServe(t, trans1)

	appendEntriesRequest := &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "",
		LeaderCommit: 0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*pb.Log{},
	}
	_, err := trans1.AppendEntries(context.Background(), peer2, appendEntriesRequest)
	assert.True(t, errors.Is(err, ErrUnknownTransporClient))

	testingTransportServe(t, trans2)

	stopRespCh1 := testingTransportRPCResponder(trans1.RPC())
	defer close(stopRespCh1)

	ƒAssertNoError2(trans2.AppendEntries(context.Background(), peer1, appendEntriesRequest))(t)
}

func TestTransports(t *testing.T) {
	t.Run("Internal", func(t *testing.T) {
		lookup := newInternalTransClientLookup()
		transFn := func(peer *pb.Peer) (Transport, error) {
			return newInternalTransport(lookup, peer.Endpoint)
		}
		peerFn := func() (*pb.Peer, error) {
			oid := NewObjectID().Hex()
			return &pb.Peer{Id: oid, Endpoint: oid}, nil
		}
		testTransport(t, transFn, peerFn)
	})

}
