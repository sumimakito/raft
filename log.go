package raft

import (
	"github.com/sumimakito/raft/pb"
)

type LogProvider interface {
	// AppendLogs is used to append logs to the log store.
	// It's recommended to use techniques like transaction processing to
	// avoid data inconsistency due to an error or interruption.
	AppendLogs(logs []*pb.Log) error

	// TrimPrefix trims the log store by evicting logs forwards from the
	// beginning until the log with the index is reached. Index is exclusive.
	TrimPrefix(index uint64) error

	// TrimSuffix trims the log store by evicting logs backwards from the
	// ending until the log with the index is reached. Index is exclusive.
	TrimSuffix(index uint64) error

	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)

	Entry(index uint64) (*pb.Log, error)
	LastEntry() (*pb.Log, error)

	LastTypedEntry(t pb.LogType) (*pb.Log, error)
}

type LogStoreTypedFinder interface {
	FirstTypedIndex(t pb.LogType) (uint64, error)
	LastTypedIndex(t pb.LogType) (uint64, error)

	FirstTypedEntry(t pb.LogType) (*pb.Log, error)
	LastTypedEntry(t pb.LogType) (*pb.Log, error)
}
