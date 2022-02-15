package raft

import (
	"sort"

	"github.com/sumimakito/raft/pb"
)

type internalLogStore struct {
	logs []*pb.Log
}

func newInternalLogStore() *internalLogStore {
	return &internalLogStore{}
}

func (s *internalLogStore) putLog(log *pb.Log) {
	i := sort.Search(len(s.logs), func(i int) bool { return s.logs[i].Meta.Index > log.Meta.Index })
	if i == len(s.logs) {
		s.logs = append(s.logs, log.Copy())
		return
	}
	s.logs = append(s.logs, nil)
	copy(s.logs[i+1:], s.logs[i:])
	s.logs[i] = log.Copy()
}

func (s *internalLogStore) AppendLogs(logs []*pb.Log) error {
	for _, log := range logs {
		s.putLog(log)
	}
	return nil
}

func (s *internalLogStore) TrimPrefix(index uint64) error {
	i := sort.Search(len(s.logs), func(i int) bool { return s.logs[i].Meta.Index >= index })
	if i == 0 {
		return nil
	}
	s.logs = append([]*pb.Log(nil), s.logs[i:]...)
	return nil
}

func (s *internalLogStore) TrimSuffix(index uint64) error {
	i := sort.Search(len(s.logs), func(i int) bool { return s.logs[i].Meta.Index >= index })
	if i == len(s.logs) {
		return nil
	}
	if s.logs[i].Meta.Index > index {
		// We did not find the exact entry.
		if i == 0 {
			s.logs = []*pb.Log{}
			return nil
		}
		i -= 1
	}
	s.logs = append([]*pb.Log(nil), s.logs[:i+1]...)
	return nil
}

func (s *internalLogStore) FirstIndex() (uint64, error) {
	if len(s.logs) == 0 {
		return 0, nil
	}
	return s.logs[0].Meta.Index, nil
}

func (s *internalLogStore) LastIndex() (uint64, error) {
	if len(s.logs) == 0 {
		return 0, nil
	}
	return s.logs[len(s.logs)-1].Meta.Index, nil
}

func (s *internalLogStore) Entry(index uint64) (*pb.Log, error) {
	if len(s.logs) == 0 {
		return nil, nil
	}
	i := sort.Search(len(s.logs), func(i int) bool { return s.logs[i].Meta.Index >= index })
	if i == len(s.logs) || s.logs[i].Meta.Index != index {
		return nil, nil
	}
	return s.logs[i], nil
}

func (s *internalLogStore) LastEntry(t pb.LogType) (*pb.Log, error) {
	if len(s.logs) == 0 {
		return nil, nil
	}
	if t == 0 {
		return s.logs[len(s.logs)-1], nil
	}
	for i := len(s.logs) - 1; i >= 0; i-- {
		if s.logs[i].Body.Type == t {
			return s.logs[i], nil
		}
	}
	return nil, nil
}
