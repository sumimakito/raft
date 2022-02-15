package raft

import (
	"encoding/binary"

	"github.com/ugorji/go/codec"
	"go.etcd.io/bbolt"
)

const (
	boltStateStoreBucketStates   = "states"
	boltStateStoreKeyCurrentTerm = "current_term"
	boltStateStoreKeyLastVote    = "last_vote"
)

type BoltStateStore struct {
	db *bbolt.DB
}

func NewBoltStateStore(db *bbolt.DB) *BoltStateStore {
	return &BoltStateStore{db: db}
}

func (s *BoltStateStore) CurrentTerm() (uint64, error) {
	currentTerm := uint64(0)
	if err := s.db.View(func(t *bbolt.Tx) error {
		if bucket := t.Bucket([]byte(boltStateStoreBucketStates)); bucket != nil {
			if b := bucket.Get([]byte(boltStateStoreKeyCurrentTerm)); b != nil {
				currentTerm = binary.BigEndian.Uint64(b)
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return currentTerm, nil
}

func (s *BoltStateStore) SetCurrentTerm(currentTerm uint64) error {
	return s.db.Update(func(t *bbolt.Tx) error {
		bucket, err := t.CreateBucketIfNotExists([]byte(boltStateStoreBucketStates))
		if err != nil {
			return nil
		}
		return bucket.Put([]byte(boltStateStoreKeyCurrentTerm), EncodeUint64(currentTerm))
	})
}

func (s *BoltStateStore) LastVote() (voteSummary, error) {
	summary := nilVoteSummary
	if err := s.db.View(func(t *bbolt.Tx) error {
		if bucket := t.Bucket([]byte(boltStateStoreBucketStates)); bucket != nil {
			if b := bucket.Get([]byte(boltStateStoreKeyLastVote)); b != nil {
				if err := codec.NewDecoderBytes(b, &codec.MsgpackHandle{}).Decode(&summary); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return nilVoteSummary, err
	}
	return summary, nil
}

func (s *BoltStateStore) SetLastVote(summary voteSummary) error {
	return s.db.Update(func(t *bbolt.Tx) error {
		bucket, err := t.CreateBucketIfNotExists([]byte(boltStateStoreBucketStates))
		if err != nil {
			return nil
		}
		var b []byte
		if err := codec.NewEncoderBytes(&b, &codec.MsgpackHandle{}).Encode(b); err != nil {
			return err
		}
		return bucket.Put([]byte(boltStateStoreKeyLastVote), b)
	})
}
