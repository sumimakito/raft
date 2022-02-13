package raft

import (
	"sort"

	"github.com/sumimakito/raft/pb"
)

type internalLogProvider struct {
	logs []*pb.Log
}

func newInternalLogProvider() *internalLogProvider {
	return &internalLogProvider{}
}

func (p *internalLogProvider) putLog(log *pb.Log) {
	i := sort.Search(len(p.logs), func(i int) bool { return p.logs[i].Meta.Index > log.Meta.Index })
	if i == len(p.logs) {
		p.logs = append(p.logs, log.Copy())
		return
	}
	p.logs = append(p.logs, nil)
	copy(p.logs[i+1:], p.logs[i:])
	p.logs[i] = log.Copy()
}

func (p *internalLogProvider) AppendLogs(logs []*pb.Log) error {
	for _, log := range logs {
		p.putLog(log)
	}
	return nil
}

func (p *internalLogProvider) TrimPrefix(index uint64) error {
	i := sort.Search(len(p.logs), func(i int) bool { return p.logs[i].Meta.Index >= index })
	if i == 0 {
		return nil
	}
	p.logs = append([]*pb.Log(nil), p.logs[i:]...)
	return nil
}

func (p *internalLogProvider) TrimSuffix(index uint64) error {
	i := sort.Search(len(p.logs), func(i int) bool { return p.logs[i].Meta.Index >= index })
	if i == len(p.logs) {
		return nil
	}
	p.logs = append([]*pb.Log(nil), p.logs[:i+1]...)
	return nil
}

func (p *internalLogProvider) FirstIndex() (uint64, error) {
	if len(p.logs) == 0 {
		return 0, nil
	}
	return p.logs[0].Meta.Index, nil
}

func (p *internalLogProvider) LastIndex() (uint64, error) {
	if len(p.logs) == 0 {
		return 0, nil
	}
	return p.logs[len(p.logs)-1].Meta.Index, nil
}

func (p *internalLogProvider) Entry(index uint64) (*pb.Log, error) {
	if len(p.logs) == 0 {
		return nil, nil
	}
	i := sort.Search(len(p.logs), func(i int) bool { return p.logs[i].Meta.Index >= index })
	if i == len(p.logs) || p.logs[i].Meta.Index != index {
		return nil, nil
	}
	return p.logs[i], nil
}

func (p *internalLogProvider) LastEntry(t pb.LogType) (*pb.Log, error) {
	if len(p.logs) == 0 {
		return nil, nil
	}
	if t == 0 {
		return p.logs[len(p.logs)-1], nil
	}
	for i := len(p.logs) - 1; i >= 0; i-- {
		if p.logs[i].Body.Type == t {
			return p.logs[i], nil
		}
	}
	return nil, nil
}
