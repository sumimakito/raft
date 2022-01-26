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

func encodeLog(log *pb.Log) ([]byte, error) {
	b, err := proto.Marshal(log)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func decodeLog(in []byte) (*pb.Log, error) {
	var pbLog pb.Log
	if err := proto.Unmarshal(in, &pbLog); err != nil {
		return nil, err
	}
	return &pbLog, nil
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

func (s *LogStore) AppendLogs(logs []*pb.Log) error {
	return s.db.Update(func(t *bbolt.Tx) error {
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
			logBytes, err := encodeLog(logs[i])
			if err != nil {
				return err
			}
			if err := bucket.Put(raft.EncodeUint64(index), logBytes); err != nil {
				return err
			}
			if err := s.putLogIndex(t, logs[i].Body.Type, index); err != nil {
				return err
			}
			index++
		}
		return nil
	})
}

func (s *LogStore) TrimBefore(index uint64) error {
	return s.db.Update(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(logStoreBucketLogs))
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		c.Seek(raft.EncodeUint64(index))
		key, value := c.Prev()
		for key != nil {
			log, err := decodeLog(value)
			if err != nil {
				return err
			}
			if err := s.deleteLogIndex(t, log.Body.Type, raft.DecodeUint64(key)); err != nil {
				return err
			}
			if err := c.Delete(); err != nil {
				return err
			}
			key, value = c.Prev()
		}
		return nil
	})
}

func (s *LogStore) TrimAfter(index uint64) error {
	return s.db.Update(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(logStoreBucketLogs))
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		c.Seek(raft.EncodeUint64(index))
		key, value := c.Next()
		for key != nil {
			log, err := decodeLog(value)
			if err != nil {
				return err
			}
			if err := s.deleteLogIndex(t, log.Body.Type, raft.DecodeUint64(key)); err != nil {
				return err
			}
			if err := c.Delete(); err != nil {
				return err
			}
			key, value = c.Next()
		}
		return nil
	})
}

func (s *LogStore) FirstIndex() (uint64, error) {
	var index uint64
	return index, s.db.View(func(t *bbolt.Tx) error {
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
	})
}

func (s *LogStore) LastIndex() (uint64, error) {
	var index uint64
	return index, s.db.View(func(t *bbolt.Tx) error {
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
	})
}

func (s *LogStore) Entry(index uint64) (*pb.Log, error) {
	var log *pb.Log
	return log, s.db.View(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(logStoreBucketLogs))
		if bucket == nil {
			return nil
		}
		key, value := bucket.Cursor().Seek(raft.EncodeUint64(index))
		if key == nil {
			return nil
		}
		if l, err := decodeLog(value); err != nil {
			return err
		} else {
			log = l
		}
		return nil
	})
}

func (s *LogStore) LastEntry() (*pb.Log, error) {
	var log *pb.Log
	return log, s.db.View(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(logStoreBucketLogs))
		if bucket == nil {
			return nil
		}
		key, value := bucket.Cursor().Last()
		if key == nil {
			return nil
		}
		if l, err := decodeLog(value); err != nil {
			return err
		} else {
			log = l
		}
		return nil
	})
}

func (s *LogStore) FirstTypedIndex(t pb.LogType) (uint64, error) {
	var index uint64
	return index, s.db.View(func(tx *bbolt.Tx) error {
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
		key, _ := bucket.Cursor().First()
		if key == nil {
			return nil
		}
		index = raft.DecodeUint64(key)
		return nil
	})
}

func (s *LogStore) LastTypedIndex(t pb.LogType) (uint64, error) {
	var index uint64
	return index, s.db.View(func(tx *bbolt.Tx) error {
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
		key, _ := bucket.Cursor().Last()
		if key == nil {
			return nil
		}
		index = raft.DecodeUint64(key)
		return nil
	})
}

func (s *LogStore) FirstTypedEntry(t pb.LogType) (*pb.Log, error) {
	var log *pb.Log
	return log, s.db.View(func(tx *bbolt.Tx) error {
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
		if l, err := decodeLog(value); err != nil {
			return err
		} else {
			log = l
		}
		return nil
	})
}

func (s *LogStore) LastTypedEntry(t pb.LogType) (*pb.Log, error) {
	var log *pb.Log
	return log, s.db.View(func(tx *bbolt.Tx) error {
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
		if l, err := decodeLog(value); err != nil {
			return err
		} else {
			log = l
		}
		return nil
	})
}
