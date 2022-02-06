package raft

import (
	"encoding/binary"

	"github.com/ugorji/go/codec"
	"go.etcd.io/bbolt"
)

const (
	stableStoreBucketStates   = "states"
	stableStoreKeyCurrentTerm = "current_term"
	stableStoreKeyLastVote    = "last_vote"
)

type stableStore struct {
	server *Server
	db     *bbolt.DB
}

func newStableStore(server *Server) *stableStore {
	return &stableStore{
		server: server,
		db:     Must2(bbolt.Open(server.opts.stableStorePath, 0600, nil)),
	}
}

func (s *stableStore) CurrentTerm() (uint64, error) {
	currentTerm := uint64(0)
	if err := s.db.View(func(t *bbolt.Tx) error {
		if bucket := t.Bucket([]byte(stableStoreBucketStates)); bucket != nil {
			if b := bucket.Get([]byte(stableStoreKeyCurrentTerm)); b != nil {
				currentTerm = binary.BigEndian.Uint64(b)
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return currentTerm, nil
}

func (s *stableStore) SetCurrentTerm(currentTerm uint64) error {
	return s.db.Update(func(t *bbolt.Tx) error {
		bucket, err := t.CreateBucketIfNotExists([]byte(stableStoreBucketStates))
		if err != nil {
			return nil
		}
		return bucket.Put([]byte(stableStoreKeyCurrentTerm), EncodeUint64(currentTerm))
	})
}

func (s *stableStore) LastVote() (voteSummary, error) {
	summary := nilVoteSummary
	if err := s.db.View(func(t *bbolt.Tx) error {
		if bucket := t.Bucket([]byte(stableStoreBucketStates)); bucket != nil {
			if b := bucket.Get([]byte(stableStoreKeyLastVote)); b != nil {
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

func (s *stableStore) SetLastVote(summary voteSummary) error {
	return s.db.Update(func(t *bbolt.Tx) error {
		bucket, err := t.CreateBucketIfNotExists([]byte(stableStoreBucketStates))
		if err != nil {
			return nil
		}
		var b []byte
		if err := codec.NewEncoderBytes(&b, &codec.MsgpackHandle{}).Encode(b); err != nil {
			return err
		}
		return bucket.Put([]byte(stableStoreKeyLastVote), b)
	})
}
