package raft

import (
	"github.com/sumimakito/raft/pb"
)

type LogStore interface {
	AppendLogs(logs []*pb.Log)
	DeleteAfter(firstIndex uint64)

	FirstIndex() uint64
	LastIndex() uint64

	Entry(index uint64) *pb.Log
	LastEntry() *pb.Log
	LastTermIndex() (term uint64, index uint64)
}

type LogStoreTypedFinder interface {
	FirstTypedEntry(t pb.LogType) *pb.Log
	LastTypedEntry(t pb.LogType) *pb.Log
}
