package raft

import (
	"context"
	"io"

	"github.com/sumimakito/raft/pb"
)

type Transport interface {
	// Endpoint returns the endpoint used by current Transport instance
	Endpoint() string

	AppendEntries(ctx context.Context, peer *pb.Peer, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	RequestVote(ctx context.Context, peer *pb.Peer, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	InstallSnapshot(ctx context.Context, peer *pb.Peer, requestMeta *pb.InstallSnapshotRequestMeta, reader io.Reader) (*pb.InstallSnapshotResponse, error)
	ApplyLog(ctx context.Context, peer *pb.Peer, request *pb.ApplyLogRequest) (*pb.ApplyLogResponse, error)

	RPC() <-chan *RPC

	Serve() error
}

// TransportConnecter is an optional interface for those implementations
// that allow explicit connect and disconnect operations on a per peer basis.
type TransportConnecter interface {
	Connect(peer *pb.Peer) error
	Disconnect(peer *pb.Peer)
	DisconnectAll()
}

// TransportCloser is an optional interface for those implementations
// that allow explicit close operation on its underlying connections.
type TransportCloser interface {
	Close() error
}
