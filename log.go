package raft

import (
	"github.com/sumimakito/raft/pb"
)

// LogStore defines the interface for appending, trimming, and retrieving logs
// in a stable store.
// A LogStore implementation can also implement the optional io.Closer interface
// to allow releasing the underlying resources it has acquired.
type LogStore interface {
	// AppendLogs is used to append logs to the LogStore.
	// It's recommended to use techniques like transaction processing to
	// avoid data inconsistency due to an error or interruption.
	AppendLogs(logs []*pb.Log) error

	// TrimPrefix is used to trim the logs by evicting UNPACKED logs forwards from
	// the first log until the log with the index is reached. Index is exclusive.
	TrimPrefix(index uint64) error

	// TrimSuffix is used to trim the logs by evicting UNPACKED logs backwards from
	// the last log until the log with the index is reached. Index is exclusive.
	TrimSuffix(index uint64) error

	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)

	Entry(index uint64) (*pb.Log, error)

	// LastEntry is used to find the last log entry.
	// If t is not zero, a log type filter should be applied.
	LastEntry(t pb.LogType) (*pb.Log, error)
}

type logStoreOp interface {
	__logStoreOp()
}

type logStoreTrimType uint8

const (
	logStoreTrimPrefix logStoreTrimType = 1 + iota
	logStoreTrimSuffix
)

type logStoreAppendOp struct {
	FutureTask[[]*pb.LogMeta, []*pb.LogBody]
}

func (*logStoreAppendOp) __logStoreOp() {}

type logStoreTrimOp struct {
	Type logStoreTrimType
	FutureTask[any, uint64]
}

func (*logStoreTrimOp) __logStoreOp() {}

// logStoreProxy works as a proxy for the underlying LogStore.
type logStoreProxy struct {
	LogStore
	server       *Server
	snapshotMeta SnapshotMeta
}

func newLogStoreProxy(server *Server, logStore LogStore) *logStoreProxy {
	return &logStoreProxy{server: server, LogStore: logStore}
}

func (l *logStoreProxy) Restore(snapshotMeta SnapshotMeta) error {
	// Evict all logs with the logs that exist in the snapshot.
	if err := l.TrimPrefix(snapshotMeta.Index() + 1); err != nil {
		return err
	}
	l.snapshotMeta = snapshotMeta
	l.server.setLastLogIndex(Must2(l.LastIndex()))
	return nil
}

func (l *logStoreProxy) TrimPrefix(index uint64) error {
	if l.snapshotMeta != nil {
		// Ensure the index is not in the snapshot's range.
		// If so, we cannot do anything.
		if index <= l.snapshotMeta.Index() {
			l.server.logger.Panicw("called TrimPrefix() with an index exists in the snapshot", logFields(l.server)...)
		}
	}
	return l.LogStore.TrimPrefix(index)
}

func (l *logStoreProxy) TrimSuffix(index uint64) error {
	if l.snapshotMeta != nil {
		// Ensure the index is not in the snapshot's range.
		// If so, we cannot do anything.
		if index < l.snapshotMeta.Index() {
			l.server.logger.Panicw("called TrimSuffix() with an index exists in the snapshot", logFields(l.server)...)
		}
	}
	return l.LogStore.TrimSuffix(index)
}

func (l *logStoreProxy) LastIndex() (uint64, error) {
	underlyingLastIndex, err := l.LogStore.LastIndex()
	if err != nil {
		return 0, err
	}
	if underlyingLastIndex > 0 {
		// If the last index in the underlying LogStore > 0, it also should be
		// larger than the last index in the snapshot (if any).
		return underlyingLastIndex, nil
	}
	// The last index in the underlying being zero indicates that the underlying
	// LogStore is empty. Use the last index in the snapshot (if any) or return
	// zero.
	if l.snapshotMeta != nil {
		return l.snapshotMeta.Index(), nil
	}
	return 0, nil
}

func (l *logStoreProxy) Entry(index uint64) (*pb.Log, error) {
	if l.snapshotMeta != nil {
		// Ensure the index is not in the snapshot's range.
		// If so, we cannot do anything.
		if index < l.snapshotMeta.Index() {
			l.server.logger.Panicw("called Entry() with an index compacted by the snapshot", logFields(l.server)...)
		}
	}
	return l.LogStore.Entry(index)
}

// Meta is used to get the log meta at the index. A valid index should be in
// the range of the last log index in the snapshot, if any, or the first
// unpacked log index to the last unpacked log index, if any, or the last log
// index in the snapshot.
func (l *logStoreProxy) Meta(index uint64) (*pb.LogMeta, error) {
	if l.snapshotMeta != nil {
		if index == l.snapshotMeta.Index() {
			return &pb.LogMeta{Index: l.snapshotMeta.Index(), Term: l.snapshotMeta.Term()}, nil
		} else if index < l.snapshotMeta.Index() {
			l.server.logger.Panicw("called Meta() with an index compacted by the snapshot", logFields(l.server)...)
		}
	}
	e, err := l.LogStore.Entry(index)
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, nil
	}
	return e.Meta, nil
}

func (l *logStoreProxy) withinCompacted(index uint64) bool {
	if l.snapshotMeta == nil {
		return false
	}
	return index < l.snapshotMeta.Index()
}

func (l *logStoreProxy) withinSnapshot(index uint64) bool {
	if l.snapshotMeta == nil {
		return false
	}
	return index <= l.snapshotMeta.Index()
}
