package raft

import (
	"io"

	"github.com/ugorji/go/codec"
)

type Snapshot interface {
	Meta() SnapshotMeta
}

type SnapshotSink interface {
	io.WriteCloser
	ID() string
	Cancel() error
}

type SnapshotMeta struct {
	ID            string         `json:"id" codec:"id"`
	Index         uint64         `json:"index" codec:"index"`
	Term          uint64         `json:"term" codec:"term"`
	Configuration *Configuration `json:"configuration" codec:"configuration"`
}

func (m *SnapshotMeta) Encode() (out []byte) {
	codec.NewEncoderBytes(&out, &codec.MsgpackHandle{}).MustEncode(m)
	return
}

func (m *SnapshotMeta) Decode(in []byte) {
	codec.NewDecoderBytes(in, &codec.MsgpackHandle{}).MustDecode(m)
}

type SnapshotStore interface {
	Create(index, term uint64, c *Configuration) (SnapshotSink, error)
	List() ([]*SnapshotMeta, error)
	Open(snapshotID string) (*SnapshotMeta, io.ReadCloser, error)
}
