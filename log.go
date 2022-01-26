package raft

import (
	"github.com/sumimakito/raft/pb"
)

type LogStore interface {
	AppendLogs(logs []*pb.Log) error

	// TrimBefore trims the log store by evicting logs with indexes smaller
	// than the provided index.
	TrimBefore(index uint64) error

	// TrimAfter trims the log store by evicting logs with indexes bigger
	// than the provided index.
	TrimAfter(index uint64) error

	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)

	Entry(index uint64) (*pb.Log, error)
	LastEntry() (*pb.Log, error)
}

type LogStoreTypedFinder interface {
	FirstTypedIndex(t pb.LogType) (uint64, error)
	LastTypedIndex(t pb.LogType) (uint64, error)

	FirstTypedEntry(t pb.LogType) (*pb.Log, error)
	LastTypedEntry(t pb.LogType) (*pb.Log, error)
}
