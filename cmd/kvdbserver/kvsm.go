package main

import (
	"sync"

	"github.com/sumimakito/raft"
	"github.com/ugorji/go/codec"
)

type KVSM struct {
	mu     sync.RWMutex
	index  uint64
	term   uint64
	states map[string][]byte
}

func NewKVSM() *KVSM {
	return &KVSM{states: map[string][]byte{}}
}

func (m *KVSM) Apply(command raft.Command) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cmd := DecodeCommand(command)
	switch cmd.Type {
	case CommandSet:
		m.states[cmd.Key] = cmd.Value
	case CommandUnset:
		delete(m.states, cmd.Key)
	}
}

func (m *KVSM) Keys() (keys []string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for key := range m.states {
		keys = append(keys, key)
	}
	return
}

func (m *KVSM) Value(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.states[key]
	return v, ok
}

func (m *KVSM) KeyValues() map[string][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keyValues := map[string][]byte{}
	for key, value := range m.states {
		keyValues[key] = append(([]byte)(nil), value...)
	}
	return keyValues
}

func (m *KVSM) Snapshot() (raft.StateMachineSnapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keyValues := map[string][]byte{}
	for key, value := range m.states {
		keyValues[key] = append(([]byte)(nil), value...)
	}
	return &KVSMSnapshot{index: m.index, term: m.term, keyValues: keyValues}, nil
}

func (m *KVSM) Restore(snapshot raft.Snapshot) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keyValues := map[string][]byte{}
	snapshotReader, err := snapshot.Reader()
	if err != nil {
		return err
	}
	if err := codec.NewDecoder(snapshotReader, &codec.MsgpackHandle{}).Decode(&keyValues); err != nil {
		return err
	}
	m.states = keyValues
	return nil
}

type KVSMSnapshot struct {
	index     uint64
	term      uint64
	keyValues map[string][]byte
}

func (s *KVSMSnapshot) Index() uint64 {
	return s.index
}

func (s *KVSMSnapshot) Term() uint64 {
	return s.term
}

func (s *KVSMSnapshot) Write(sink raft.SnapshotSink) error {
	var out []byte
	if err := codec.NewEncoder(sink, &codec.MsgpackHandle{}).Encode(s.keyValues); err != nil {
		return err
	}
	_, err := sink.Write(out)
	return err
}
