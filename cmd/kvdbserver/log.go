package main

import (
	"fmt"

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

// LogProvider is a log store that uses bbolt as backend
type LogProvider struct {
	db *bbolt.DB
}

func NewLogProvider(path string) *LogProvider {
	return &LogProvider{db: raft.Must2(bbolt.Open(path, 0600, nil))}
}

func (s *LogProvider) putLogIndex(tx *bbolt.Tx, t pb.LogType, index uint64) error {
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

func (s *LogProvider) deleteLogIndex(tx *bbolt.Tx, t pb.LogType, index uint64) error {
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

func (s *LogProvider) AppendLogs(logs []*pb.Log) error {
	return s.db.Update(func(t *bbolt.Tx) error {
		bucket, err := t.CreateBucketIfNotExists([]byte(logStoreBucketLogs))
		if err != nil {
			return err
		}
		for i := range logs {
			logBytes, err := encodeLog(logs[i])
			if err != nil {
				return err
			}
			if err := bucket.Put(raft.EncodeUint64(logs[i].Meta.Index), logBytes); err != nil {
				return err
			}
			if err := s.putLogIndex(t, logs[i].Body.Type, logs[i].Meta.Index); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *LogProvider) TrimPrefix(index uint64) error {
	return s.db.Update(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(logStoreBucketLogs))
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		c.Seek(raft.EncodeUint64(index))
		key, value := c.First()
		for key != nil && raft.DecodeUint64(key) < index {
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

func (s *LogProvider) TrimSuffix(index uint64) error {
	return s.db.Update(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(logStoreBucketLogs))
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		c.Seek(raft.EncodeUint64(index))
		key, value := c.Last()
		for key != nil && raft.DecodeUint64(key) > index {
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

func (s *LogProvider) FirstIndex() (uint64, error) {
	var index uint64
	return index, s.db.View(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(logStoreBucketLogs))
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		key, _ := c.First()
		if key == nil {
			return nil
		}
		index = raft.DecodeUint64(key)
		return nil
	})
}

func (s *LogProvider) LastIndex() (uint64, error) {
	var index uint64
	return index, s.db.View(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(logStoreBucketLogs))
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		key, _ := c.Last()
		if key == nil {
			return nil
		}
		index = raft.DecodeUint64(key)
		return nil
	})
}

func (s *LogProvider) Entry(index uint64) (*pb.Log, error) {
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

func (s *LogProvider) LastEntry(t pb.LogType) (*pb.Log, error) {
	var log *pb.Log
	return log, s.db.View(func(tx *bbolt.Tx) error {
		var lastKey []byte
		if t != 0 {
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
			lastKey = key
		}
		bucket := tx.Bucket([]byte(logStoreBucketLogs))
		if bucket == nil {
			return nil
		}
		var lastValue []byte
		if lastKey != nil {
			lastValue = bucket.Get(lastKey)
		} else {
			key, value := bucket.Cursor().Last()
			if key == nil {
				return nil
			}
			lastValue = value
		}
		if l, err := decodeLog(lastValue); err != nil {
			return err
		} else {
			log = l
		}
		return nil
	})
}

func (s *LogProvider) DebugPrint() {
	if err := s.db.View(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(logStoreBucketLogs))
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		key, value := c.First()
		for key != nil {
			log, err := decodeLog(value)
			if err != nil {
				return err
			}
			fmt.Println(raft.DecodeUint64(key), log)
			key, value = c.Next()
		}
		fmt.Println()
		return nil
	}); err != nil {
		panic(err)
	}
}
