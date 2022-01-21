package main

import (
	"sync"

	"github.com/sumimakito/raft"
)

type KVSM struct {
	states sync.Map // map[string][]byte
}

func NewKVSM() *KVSM {
	return &KVSM{}
}

func (m *KVSM) Apply(command raft.Command) {
	cmd := DecodeCommand(command)
	switch cmd.Type {
	case CommandSet:
		m.states.Store(cmd.Key, cmd.Value)
	case CommandUnset:
		m.states.Delete(cmd.Key)
	}
}

func (m *KVSM) Keys() []string {
	keys := make([]string, 0)
	m.states.Range(func(key, value any) bool {
		keys = append(keys, key.(string))
		return true
	})
	return keys
}

func (m *KVSM) Value(key string) ([]byte, bool) {
	if v, ok := m.states.Load(key); ok {
		return v.([]byte), true
	}
	return nil, false
}

func (m *KVSM) KeyValues() map[string][]byte {
	snapshot := map[string][]byte{}
	m.states.Range(func(key, value any) bool {
		snapshot[key.(string)] = value.([]byte)
		return true
	})
	return snapshot
}

func (m *KVSM) Snapshot() {}
