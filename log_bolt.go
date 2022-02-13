package raft

import (
	"fmt"

	"github.com/sumimakito/raft/pb"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

const (
	boltLogProviderBucketLogs        = "logs"
	boltLogProviderBucketCmdIndexes  = "cmd_indexes"
	boltLogProviderBucketConfIndexes = "conf_indexes"
)

// BoltLogProvider is a log provider that uses bbolt as a backend.
type BoltLogProvider struct {
	db *bbolt.DB
}

func NewBoltLogProvider(path string) *BoltLogProvider {
	return &BoltLogProvider{db: Must2(bbolt.Open(path, 0600, nil))}
}

func (s *BoltLogProvider) encodeLog(log *pb.Log) ([]byte, error) {
	b, err := proto.Marshal(log)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (s *BoltLogProvider) decodeLog(in []byte) (*pb.Log, error) {
	var pbLog pb.Log
	if err := proto.Unmarshal(in, &pbLog); err != nil {
		return nil, err
	}
	return &pbLog, nil
}

func (s *BoltLogProvider) putLogIndex(tx *bbolt.Tx, t pb.LogType, index uint64) error {
	var bucket *bbolt.Bucket
	var err error
	switch t {
	case pb.LogType_COMMAND:
		bucket, err = tx.CreateBucketIfNotExists([]byte(boltLogProviderBucketCmdIndexes))
	case pb.LogType_CONFIGURATION:
		bucket, err = tx.CreateBucketIfNotExists([]byte(boltLogProviderBucketConfIndexes))
	}
	if err != nil {
		return err
	}
	return bucket.Put(EncodeUint64(index), nil)
}

func (s *BoltLogProvider) deleteLogIndex(tx *bbolt.Tx, t pb.LogType, index uint64) error {
	var bucket *bbolt.Bucket
	var err error
	switch t {
	case pb.LogType_COMMAND:
		bucket, err = tx.CreateBucketIfNotExists([]byte(boltLogProviderBucketCmdIndexes))
	case pb.LogType_CONFIGURATION:
		bucket, err = tx.CreateBucketIfNotExists([]byte(boltLogProviderBucketCmdIndexes))
	}
	if err != nil {
		return err
	}
	return bucket.Delete(EncodeUint64(index))
}

func (s *BoltLogProvider) AppendLogs(logs []*pb.Log) error {
	return s.db.Update(func(t *bbolt.Tx) error {
		bucket, err := t.CreateBucketIfNotExists([]byte(boltLogProviderBucketLogs))
		if err != nil {
			return err
		}
		for i := range logs {
			logBytes, err := s.encodeLog(logs[i])
			if err != nil {
				return err
			}
			if err := bucket.Put(EncodeUint64(logs[i].Meta.Index), logBytes); err != nil {
				return err
			}
			if err := s.putLogIndex(t, logs[i].Body.Type, logs[i].Meta.Index); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BoltLogProvider) TrimPrefix(index uint64) error {
	return s.db.Update(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(boltLogProviderBucketLogs))
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		c.Seek(EncodeUint64(index))
		key, value := c.First()
		for key != nil && DecodeUint64(key) < index {
			log, err := s.decodeLog(value)
			if err != nil {
				return err
			}
			if err := s.deleteLogIndex(t, log.Body.Type, DecodeUint64(key)); err != nil {
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

func (s *BoltLogProvider) TrimSuffix(index uint64) error {
	return s.db.Update(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(boltLogProviderBucketLogs))
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		c.Seek(EncodeUint64(index))
		key, value := c.Last()
		for key != nil && DecodeUint64(key) > index {
			log, err := s.decodeLog(value)
			if err != nil {
				return err
			}
			if err := s.deleteLogIndex(t, log.Body.Type, DecodeUint64(key)); err != nil {
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

func (s *BoltLogProvider) FirstIndex() (uint64, error) {
	var index uint64
	return index, s.db.View(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(boltLogProviderBucketLogs))
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		key, _ := c.First()
		if key == nil {
			return nil
		}
		index = DecodeUint64(key)
		return nil
	})
}

func (s *BoltLogProvider) LastIndex() (uint64, error) {
	var index uint64
	return index, s.db.View(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(boltLogProviderBucketLogs))
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		key, _ := c.Last()
		if key == nil {
			return nil
		}
		index = DecodeUint64(key)
		return nil
	})
}

func (s *BoltLogProvider) Entry(index uint64) (*pb.Log, error) {
	var log *pb.Log
	return log, s.db.View(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(boltLogProviderBucketLogs))
		if bucket == nil {
			return nil
		}
		value := bucket.Get(EncodeUint64(index))
		if value == nil {
			return nil
		}
		if l, err := s.decodeLog(value); err != nil {
			return err
		} else {
			log = l
		}
		return nil
	})
}

func (s *BoltLogProvider) LastEntry(t pb.LogType) (*pb.Log, error) {
	var log *pb.Log
	return log, s.db.View(func(tx *bbolt.Tx) error {
		var lastKey []byte
		if t != 0 {
			var bucket *bbolt.Bucket
			switch t {
			case pb.LogType_COMMAND:
				bucket = tx.Bucket([]byte(boltLogProviderBucketCmdIndexes))
			case pb.LogType_CONFIGURATION:
				bucket = tx.Bucket([]byte(boltLogProviderBucketConfIndexes))
			default:
				return nil
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
		bucket := tx.Bucket([]byte(boltLogProviderBucketLogs))
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
		if l, err := s.decodeLog(lastValue); err != nil {
			return err
		} else {
			log = l
		}
		return nil
	})
}

func (s *BoltLogProvider) DebugPrint() {
	if err := s.db.View(func(t *bbolt.Tx) error {
		bucket := t.Bucket([]byte(boltLogProviderBucketLogs))
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		key, value := c.First()
		for key != nil {
			log, err := s.decodeLog(value)
			if err != nil {
				return err
			}
			fmt.Println(DecodeUint64(key), log)
			key, value = c.Next()
		}
		fmt.Println()
		return nil
	}); err != nil {
		panic(err)
	}
}

func (p *BoltLogProvider) Close() error {
	return p.db.Close()
}
