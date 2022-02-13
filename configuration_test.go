package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sumimakito/raft/pb"
)

func TestConfiguration(t *testing.T) {
	peer1 := &pb.Peer{Id: "node1", Endpoint: "endpoint1"}
	peer2 := &pb.Peer{Id: "node2", Endpoint: "endpoint2"}
	peer3 := &pb.Peer{Id: "node3", Endpoint: "endpoint3"}

	current := &pb.Config{Peers: []*pb.Peer{peer1}}
	next := &pb.Config{Peers: []*pb.Peer{peer1, peer2, peer3}}

	initialConf := newConfiguration(&pb.Configuration{Current: current}, 0)
	assert.Len(t, initialConf.Peers(), 1)
	_, ok := initialConf.Peer(peer1.Id)
	assert.True(t, ok)

	jointConf := newConfiguration(initialConf.CopyInitiateTransition(next), 1)
	assert.Len(t, jointConf.Peers(), 3)
	_, ok = jointConf.Peer(peer1.Id)
	assert.True(t, ok)
	_, ok = jointConf.Peer(peer2.Id)
	assert.True(t, ok)
	_, ok = jointConf.Peer(peer3.Id)
	assert.True(t, ok)
}
