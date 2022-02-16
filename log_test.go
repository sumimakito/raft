package raft

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sumimakito/raft/pb"
)

func testLogStoreAppendLogs(t *testing.T, p LogStore) {
	log1 := &pb.Log{Meta: &pb.LogMeta{Index: 1, Term: 1}, Body: &pb.LogBody{Type: pb.LogType_COMMAND}}
	log2 := &pb.Log{Meta: &pb.LogMeta{Index: 2, Term: 1}, Body: &pb.LogBody{Type: pb.LogType_COMMAND}}
	log3 := &pb.Log{Meta: &pb.LogMeta{Index: 3, Term: 1}, Body: &pb.LogBody{Type: pb.LogType_CONFIGURATION}}
	log4 := &pb.Log{Meta: &pb.LogMeta{Index: 4, Term: 1}, Body: &pb.LogBody{Type: pb.LogType_COMMAND}}

	i, err := p.FirstIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), i)

	i, err = p.LastIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), i)

	p.AppendLogs([]*pb.Log{log2, log3})

	i, err = p.FirstIndex()
	assert.NoError(t, err)
	assert.Equal(t, log2.Meta.Index, i)

	i, err = p.LastIndex()
	assert.NoError(t, err)
	assert.Equal(t, log3.Meta.Index, i)

	p.AppendLogs([]*pb.Log{log1, log4})

	i, err = p.FirstIndex()
	assert.NoError(t, err)
	assert.Equal(t, log1.Meta.Index, i)

	i, err = p.LastIndex()
	assert.NoError(t, err)
	assert.Equal(t, log4.Meta.Index, i)
}

func testLogStoreTrim(t *testing.T, p LogStore) {
	log1 := &pb.Log{Meta: &pb.LogMeta{Index: 1, Term: 1}, Body: &pb.LogBody{Type: pb.LogType_COMMAND}}
	log3 := &pb.Log{Meta: &pb.LogMeta{Index: 3, Term: 1}, Body: &pb.LogBody{Type: pb.LogType_COMMAND}}
	log5 := &pb.Log{Meta: &pb.LogMeta{Index: 5, Term: 1}, Body: &pb.LogBody{Type: pb.LogType_COMMAND}}
	log7 := &pb.Log{Meta: &pb.LogMeta{Index: 7, Term: 1}, Body: &pb.LogBody{Type: pb.LogType_COMMAND}}
	p.AppendLogs([]*pb.Log{log1, log3, log5, log7})

	i, err := p.FirstIndex()
	assert.NoError(t, err)
	assert.Equal(t, log1.Meta.Index, i)

	i, err = p.LastIndex()
	assert.NoError(t, err)
	assert.Equal(t, log7.Meta.Index, i)

	// Actually trim nothing
	err = p.TrimPrefix(0)
	assert.NoError(t, err)
	i, err = p.FirstIndex()
	assert.NoError(t, err)
	assert.Equal(t, log1.Meta.Index, i)

	// Actually trim nothing
	err = p.TrimPrefix(1)
	assert.NoError(t, err)
	i, err = p.FirstIndex()
	assert.NoError(t, err)
	assert.Equal(t, log1.Meta.Index, i)

	// Actually trim nothing
	err = p.TrimSuffix(7)
	assert.NoError(t, err)
	i, err = p.LastIndex()
	assert.NoError(t, err)
	assert.Equal(t, log7.Meta.Index, i)

	// Actually trim nothing
	err = p.TrimSuffix(9)
	assert.NoError(t, err)
	i, err = p.LastIndex()
	assert.NoError(t, err)
	assert.Equal(t, log7.Meta.Index, i)

	// Trim the first entry
	err = p.TrimPrefix(2)
	assert.NoError(t, err)
	i, err = p.FirstIndex()
	assert.NoError(t, err)
	assert.Equal(t, log3.Meta.Index, i)

	// Trim the last entry
	err = p.TrimSuffix(6)
	assert.NoError(t, err)
	i, err = p.LastIndex()
	assert.NoError(t, err)
	assert.Equal(t, log5.Meta.Index, i)
}

func testLogStoreEntry(t *testing.T, p LogStore) {
	log1 := &pb.Log{Meta: &pb.LogMeta{Index: 1, Term: 1}, Body: &pb.LogBody{Type: pb.LogType_COMMAND}}
	log3 := &pb.Log{Meta: &pb.LogMeta{Index: 3, Term: 1}, Body: &pb.LogBody{Type: pb.LogType_COMMAND}}
	log5 := &pb.Log{Meta: &pb.LogMeta{Index: 5, Term: 1}, Body: &pb.LogBody{Type: pb.LogType_CONFIGURATION}}
	log7 := &pb.Log{Meta: &pb.LogMeta{Index: 7, Term: 1}, Body: &pb.LogBody{Type: pb.LogType_COMMAND}}

	e, err := p.Entry(1)
	assert.NoError(t, err)
	assert.Nil(t, e)

	e, err = p.LastEntry(0)
	assert.NoError(t, err)
	assert.Nil(t, e)

	p.AppendLogs([]*pb.Log{log1, log3, log5, log7})

	e, err = p.Entry(log1.Meta.Index)
	assert.NoError(t, err)
	assert.NotNil(t, e)
	assert.Equal(t, log1.Meta.Index, e.Meta.Index)

	e, err = p.Entry(log3.Meta.Index)
	assert.NoError(t, err)
	assert.NotNil(t, e)
	assert.Equal(t, log3.Meta.Index, e.Meta.Index)

	e, err = p.Entry(0)
	assert.NoError(t, err)
	assert.Nil(t, e)

	e, err = p.Entry(9)
	assert.NoError(t, err)
	assert.Nil(t, e)

	e, err = p.LastEntry(0)
	assert.NoError(t, err)
	assert.NotNil(t, e)
	assert.Equal(t, log7.Meta.Index, e.Meta.Index)

	e, err = p.LastEntry(pb.LogType_COMMAND)
	assert.NoError(t, err)
	assert.NotNil(t, e)
	assert.Equal(t, log7.Meta.Index, e.Meta.Index)

	e, err = p.LastEntry(pb.LogType_CONFIGURATION)
	assert.NoError(t, err)
	assert.NotNil(t, e)
	assert.Equal(t, log5.Meta.Index, e.Meta.Index)

	// Type that does not exist
	e, err = p.LastEntry(255)
	assert.NoError(t, err)
	assert.Nil(t, e)
}

func testLogStore(t *testing.T, storeFn func() (StableStore, error)) {
	t.Run("AppendLogs", func(t *testing.T) {
		store, err := storeFn()
		assert.NoError(t, err)
		if closer, ok := store.(io.Closer); ok {
			defer closer.Close()
		}
		testLogStoreAppendLogs(t, store)
	})

	t.Run("Trim", func(t *testing.T) {
		store, err := storeFn()
		assert.NoError(t, err)
		if closer, ok := store.(io.Closer); ok {
			defer closer.Close()
		}
		testLogStoreTrim(t, store)
	})

	t.Run("Entry", func(t *testing.T) {
		store, err := storeFn()
		assert.NoError(t, err)
		if closer, ok := store.(io.Closer); ok {
			defer closer.Close()
		}
		testLogStoreEntry(t, store)
	})
}

func TestLogStores(t *testing.T) {
	t.Run("Internal", func(t *testing.T) {
		storeFn := func() (StableStore, error) {
			store, err := newInternalStore()
			if err != nil {
				return nil, err
			}
			return store, nil
		}
		testLogStore(t, storeFn)
	})

	t.Run("Bolt", func(t *testing.T) {
		storeFn := func() (StableStore, error) {
			b := make([]byte, 8)
			if _, err := rand.Read(b); err != nil {
				return nil, err
			}
			dbPath := filepath.Join(t.TempDir(), fmt.Sprintf("test_%s.db", base64.URLEncoding.EncodeToString(b)))
			store, err := NewBoltStore(dbPath)
			if err != nil {
				return nil, err
			}
			return store, nil
		}
		testLogStore(t, storeFn)
	})
}
