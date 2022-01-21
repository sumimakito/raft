package raft

import (
	"context"
)

type Transport interface {
	// Endpoint returns the endpoint used by current Transport instance
	Endpoint() ServerEndpoint

	AppendEntries(ctx context.Context, peer Peer, request *AppendEntriesRequest) (*AppendEntriesResponse, error)
	RequestVote(ctx context.Context, peer Peer, request *RequestVoteRequest) (*RequestVoteResponse, error)
	ApplyLog(ctx context.Context, peer Peer, request *ApplyLogRequest) (*ApplyLogResponse, error)

	RPC() <-chan *RPC

	Serve()
}

// TransportConnecter is an optional interface for those implementations
// that allow explicit connect and disconnect operations on a per peer basis.
type TransportConnecter interface {
	Connect(peer Peer) error
	Disconnect(peer Peer)
	DisconnectAll()
}

// TransportCloser is an optional interface for those implementations
// that allow explicit close operation on its underlying connections.
type TransportCloser interface {
	Close() error
}
