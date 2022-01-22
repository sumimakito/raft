package main

import (
	"bufio"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sumimakito/raft"
	"github.com/ugorji/go/codec"
)

type SnapshotMeta struct {
	raft.SnapshotMeta
	Size int `json:"size" codec:"size"`
}

type SnapshotSink struct {
	wipDir   string
	finalDir string

	meta SnapshotMeta

	snapshotFile   *os.File
	snapshotWriter *bufio.Writer
}

func newSnapshotSink(wipDir, finalDir string, m raft.SnapshotMeta) *SnapshotSink {
	return &SnapshotSink{
		wipDir:   wipDir,
		finalDir: finalDir,
		meta: SnapshotMeta{
			SnapshotMeta: m,
			Size:         0,
		},
	}
}

func (s *SnapshotSink) writeMeta() error {
	file, err := os.OpenFile(filepath.Join(s.wipDir, "meta"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	var b []byte
	if err := codec.NewEncoderBytes(&b, &codec.MsgpackHandle{}).Encode(s.meta); err != nil {
		return err
	}
	if _, err := writer.Write(b); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return nil
}

func (s *SnapshotSink) close() error {
	if s.snapshotFile != nil {
		if err := s.snapshotWriter.Flush(); err != nil {
			return err
		}
		if err := s.snapshotFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *SnapshotSink) Write(p []byte) (n int, err error) {
	if s.snapshotFile == nil {
		file, err := os.OpenFile(filepath.Join(s.wipDir, "snapshot"), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return 0, err
		}
		s.snapshotFile = file
		s.snapshotWriter = bufio.NewWriter(s.snapshotFile)
	}

	n, err = s.snapshotWriter.Write(p)
	if err != nil {
		return n, err
	}
	s.meta.Size += n
	return n, nil
}

func (s *SnapshotSink) Close() error {
	if err := s.close(); err != nil {
		return err
	}
	if err := s.writeMeta(); err != nil {
		return err
	}
	if err := os.Rename(s.wipDir, s.finalDir); err != nil {
		return err
	}
	return nil
}

func (s *SnapshotSink) ID() string {
	return s.meta.ID
}

func (s *SnapshotSink) Cancel() error {
	if err := s.close(); err != nil {
		return err
	}
	if err := os.RemoveAll(s.wipDir); err != nil {
		return err
	}
	return nil
}

type SnapshotStore struct {
	storeDir string
}

func NewSnapshotStore(storeDir string) *SnapshotStore {
	return &SnapshotStore{storeDir: storeDir}
}

func (s *SnapshotStore) Create(index, term uint64, c *raft.Configuration) (raft.SnapshotSink, error) {
	id := raft.NewObjectID().Hex()

	wipDir := filepath.Join(s.storeDir, fmt.Sprintf("inprogress-%s", id))
	finalDir := filepath.Join(s.storeDir, id)

	if err := os.MkdirAll(wipDir, 0755); err != nil {
		return nil, err
	}

	sink := newSnapshotSink(wipDir, finalDir, raft.SnapshotMeta{
		ID:            id,
		Index:         index,
		Term:          term,
		Configuration: c.Copy(),
	})

	return sink, nil
}

func (s *SnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	ids := []string{}
	if err := filepath.WalkDir(s.storeDir, func(path string, d fs.DirEntry, err error) error {
		if path == s.storeDir || !d.IsDir() {
			return nil
		}
		if !strings.HasPrefix(d.Name(), "inprogress-") {
			ids = append(ids, d.Name())
		}
		return filepath.SkipDir
	}); err != nil {
		return nil, err
	}
	metaList := []*raft.SnapshotMeta{}
	for _, id := range ids {
		file, err := os.Open(filepath.Join(s.storeDir, id, "meta"))
		if err != nil {
			return nil, err
		}
		reader := bufio.NewReader(file)
		var m SnapshotMeta
		if err := codec.NewDecoder(reader, &codec.MsgpackHandle{}).Decode(&m); err != nil {
			file.Close()
			return nil, err
		}
		metaList = append(metaList, &m.SnapshotMeta)
		file.Close()
	}
	// Sort by index in descending order
	sort.SliceStable(metaList, func(i, j int) bool {
		return metaList[i].Index > metaList[j].Index
	})
	return metaList, nil
}

func (s *SnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	file, err := os.Open(filepath.Join(s.storeDir, id, "meta"))
	if err != nil {
		return nil, nil, err
	}
	reader := bufio.NewReader(file)
	var m SnapshotMeta
	if err := codec.NewDecoder(reader, &codec.MsgpackHandle{}).Decode(&m); err != nil {
		file.Close()
		return nil, nil, err
	}
	file.Close()
	file, err = os.Open(filepath.Join(s.storeDir, id, "snapshot"))
	if err != nil {
		return nil, nil, err
	}
	return &m.SnapshotMeta, raft.NewBufferedReadCloser(file), nil
}
