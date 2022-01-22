package main

import (
	"sync"

	"github.com/sumimakito/raft"
	"github.com/ugorji/go/codec"
)

type KVSM struct {
	statesMu sync.RWMutex // protects states
	states   map[string][]byte
}

func NewKVSM() *KVSM {
	return &KVSM{states: map[string][]byte{}}
}

func (m *KVSM) Apply(command raft.Command) {
	m.statesMu.Lock()
	defer m.statesMu.Unlock()
	cmd := DecodeCommand(command)
	switch cmd.Type {
	case CommandSet:
		m.states[cmd.Key] = cmd.Value
	case CommandUnset:
		delete(m.states, cmd.Key)
	}
}

func (m *KVSM) Keys() (keys []string) {
	m.statesMu.RLock()
	defer m.statesMu.RUnlock()
	for key := range m.states {
		keys = append(keys, key)
	}
	return
}

func (m *KVSM) Value(key string) ([]byte, bool) {
	m.statesMu.RLock()
	defer m.statesMu.RUnlock()
	v, ok := m.states[key]
	return v, ok
}

func (m *KVSM) KeyValues() map[string][]byte {
	m.statesMu.RLock()
	defer m.statesMu.RUnlock()
	keyValues := map[string][]byte{}
	for key, value := range m.states {
		keyValues[key] = append(([]byte)(nil), value...)
	}
	return keyValues
}

func (m *KVSM) Snapshot() raft.StateMachineSnapshot {
	m.statesMu.RLock()
	defer m.statesMu.RUnlock()
	keyValues := map[string][]byte{}
	for key, value := range m.states {
		keyValues[key] = append(([]byte)(nil), value...)
	}
	return &KVSMSnapshot{keyValues: keyValues}
}

func (m *KVSM) Restore() {}

type KVSMSnapshot struct {
	keyValues map[string][]byte
}

func (s *KVSMSnapshot) Write(sink raft.SnapshotSink) error {
	var out []byte
	if err := codec.NewEncoderBytes(&out, &codec.MsgpackHandle{}).Encode(s.keyValues); err != nil {
		return err
	}
	_, err := sink.Write(out)
	return err
}
