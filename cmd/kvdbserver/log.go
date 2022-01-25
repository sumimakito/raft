package main

import (
	"github.com/sumimakito/raft"
	"github.com/sumimakito/raft/pb"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

const (
	logStoreBucketLogs        = "logs"
	logStoreBucketCmdIndexes  = "cmd_indexes"
	logStoreBucketConfIndexes = "conf_indexes"
)

func encodeLog(log *pb.Log) (out []byte) {
	return raft.Must2(proto.Marshal(log)).([]byte)
}

func decodeLog(in []byte) *pb.Log {
	var pbLog pb.Log
	proto.Unmarshal(in, &pbLog)
	return &pbLog
}

// LogStore is a log store that uses bbolt as backend
type LogStore struct {
	db *bbolt.DB
}

func NewLogStore(path string) *LogStore {
	return &LogStore{db: raft.Must2(bbolt.Open(path, 0600, nil)).(*bbolt.DB)}
}

func (s *LogStore) putLogIndex(tx *bbolt.Tx, t pb.LogType, index uint64) error {
	var bucket *bbolt.Bucket
	var err error
	switch t {
	case pb.LogType_COMMAND:
		bucket, err = tx.CreateBucketIfNotExists([]byte(logStoreBucketCmdIndexes))
	case pb.LogType_CONFIGURATION:
		bucket, err = tx.CreateBucketIfNotExists([]byte(logStoreBucketCmdIndexes))
	}
	if err != nil {
		return err
	}
	return bucket.Put(raft.EncodeUint64(index), nil)
}

func (s *LogStore) deleteLogIndex(tx *bbolt.Tx, t pb.LogType, index uint64) error {
	var bucket *bbolt.Bucket
	var err error
	switch t {
	case pb.LogType_COMMAND:
		bucket, err = tx.CreateBucketIfNotExists([]byte(logStoreBucketCmdIndexes))
	case pb.LogType_CONFIGURATION:
		bucket, err = tx.CreateBucketIfNotExists([]byte(logStoreBucketCmdIndexes))
	}
	if err != nil {
		return err
	}
	return bucket.Delete(raft.EncodeUint64(index))
}

func (s *LogStore) AppendLogs(logs []*pb.Log) {
	raft.Must1(s.db.Update(func(t *bbolt.Tx) error {
		bucket, err := t.CreateBucketIfNotExists([]byte(logStoreBucketLogs))
		if err != nil {
			return err
		}
		key, _ := bucket.Cursor().Last()
		var index uint64 = 1
		if key != nil {
			index = raft.DecodeUint64(key) + 1
		}
		for i := range logs {
			if err := bucket.Put(raft.EncodeUint64(index), encodeLog(logs[i])); err != nil {
				return err
			}
			if err := s.putLogIndex(t, logs[i].Body.Type, index); err != nil {
				return err
			}
			index++
		}
		return nil
	}))
}

func (s *LogStore) DeleteAfter(firstIndex uint64) {
	raft.Must1(s.db.Update(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(logStoreBucketLogs))
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		key, value := c.Seek(raft.EncodeUint64(firstIndex))
		if key == nil {
			return nil
		}
		log := decodeLog(value)
		if err := s.deleteLogIndex(t, log.Body.Type, raft.DecodeUint64(key)); err != nil {
			return err
		}
		if err := c.Delete(); err != nil {
			return err
		}
		return nil
	}))
}

func (s *LogStore) FirstIndex() (index uint64) {
	raft.Must1(s.db.View(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(logStoreBucketLogs))
		if bucket == nil {
			return nil
		}
		key, _ := bucket.Cursor().First()
		if key == nil {
			return nil
		}
		index = raft.DecodeUint64(key)
		return nil
	}))
	return
}

func (s *LogStore) LastIndex() (index uint64) {
	raft.Must1(s.db.View(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(logStoreBucketLogs))
		if bucket == nil {
			return nil
		}
		key, _ := bucket.Cursor().Last()
		if key == nil {
			return nil
		}
		index = raft.DecodeUint64(key)
		return nil
	}))
	return
}

func (s *LogStore) Entry(index uint64) (log *pb.Log) {
	raft.Must1(s.db.View(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(logStoreBucketLogs))
		if bucket == nil {
			return nil
		}
		key, value := bucket.Cursor().Seek(raft.EncodeUint64(index))
		if key == nil {
			return nil
		}
		log = decodeLog(value)
		return nil
	}))
	return
}

func (s *LogStore) LastEntry() (log *pb.Log) {
	raft.Must1(s.db.View(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(logStoreBucketLogs))
		if bucket == nil {
			return nil
		}
		key, value := bucket.Cursor().Last()
		if key == nil {
			return nil
		}
		log = decodeLog(value)
		return nil
	}))
	return
}

func (s *LogStore) LastTermIndex() (term uint64, index uint64) {
	if log := s.LastEntry(); log != nil {
		return log.Meta.Term, log.Meta.Index
	}
	return 0, 0
}

func (s *LogStore) FirstTypedEntry(t pb.LogType) (log *pb.Log) {
	raft.Must1(s.db.View(func(tx *bbolt.Tx) error {
		var bucket *bbolt.Bucket
		switch t {
		case pb.LogType_COMMAND:
			bucket = tx.Bucket([]byte(logStoreBucketCmdIndexes))
		case pb.LogType_CONFIGURATION:
			bucket = tx.Bucket([]byte(logStoreBucketConfIndexes))
		}
		if bucket == nil {
			return nil
		}
		key, value := bucket.Cursor().First()
		if key == nil {
			return nil
		}
		log = decodeLog(value)
		return nil
	}))
	return
}

func (s *LogStore) LastTypedEntry(t pb.LogType) (log *pb.Log) {
	raft.Must1(s.db.View(func(tx *bbolt.Tx) error {
		var bucket *bbolt.Bucket
		switch t {
		case pb.LogType_COMMAND:
			bucket = tx.Bucket([]byte(logStoreBucketCmdIndexes))
		case pb.LogType_CONFIGURATION:
			bucket = tx.Bucket([]byte(logStoreBucketConfIndexes))
		}
		if bucket == nil {
			return nil
		}
		key, value := bucket.Cursor().Last()
		if key == nil {
			return nil
		}
		log = decodeLog(value)
		return nil
	}))
	return
}
