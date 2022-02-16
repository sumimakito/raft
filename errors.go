package raft

import "errors"

var (
	ErrDeadlineExceeded = errors.New("deadline exceeded")

	// ErrServerShutdown indicates that the server was already shutted
	// down or the server shutting down is in progress.
	ErrServerShutdown = errors.New("server shutdown")

	// ErrNonLeader indicates that the server received an RPC that cannot
	// be processed on non-leader server.
	ErrNonLeader = errors.New("not a leader")

	// ErrNonFollower indicates that the server received an RPC that cannot
	// be processed on non-follower server.
	ErrNonFollower = errors.New("not a follower")

	// ErrUnrecognizedRPC indicates that the server has received an
	// unrecongized RPC request.
	ErrUnrecognizedRPC = errors.New("unrecognized RPC request")

	// ErrInJointConsensus indicates that the server is already in a joint
	// consensus.
	ErrInJointConsensus = errors.New("already in a joint consensus")

	// ErrInJointConsensus indicates that the server is not in a joint consensus.
	ErrNotInJointConsensus = errors.New("not in a joint consensus")

	ErrUnknownTransporClient = errors.New("unknown transport client")

	ErrUnknownRPC = errors.New("unknown RPC")
)
