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
	kvdbpb "github.com/sumimakito/raft/cmd/kvdbserver/pb"
	"github.com/sumimakito/raft/pb"
	"google.golang.org/protobuf/proto"
)

type SnapshotMeta struct {
	metadata *kvdbpb.SnapshotMeta
}

func (m *SnapshotMeta) Id() string {
	return m.metadata.Id
}

func (m *SnapshotMeta) SetId(id string) {
	m.metadata.Id = id
}

func (m *SnapshotMeta) Index() uint64 {
	return m.metadata.Index
}

func (m *SnapshotMeta) SetIndex(index uint64) {
	m.metadata.Index = index
}

func (m *SnapshotMeta) Term() uint64 {
	return m.metadata.Term
}

func (m *SnapshotMeta) SetTerm(term uint64) {
	m.metadata.Term = term
}

func (m *SnapshotMeta) Configuration() *pb.Configuration {
	return m.metadata.Configuration
}

func (m *SnapshotMeta) SetConfiguration(configuration *pb.Configuration) {
	m.metadata.Configuration = configuration
}

func (m *SnapshotMeta) Size() uint64 {
	return m.metadata.Size
}

func (m *SnapshotMeta) SetSize(size uint64) {
	m.metadata.Size = size
}

func (m *SnapshotMeta) CRC64() uint64 {
	return m.metadata.Crc64
}

func (m *SnapshotMeta) SetCRC64(crc64 uint64) {
	m.metadata.Crc64 = crc64
}

func (m *SnapshotMeta) Encode() ([]byte, error) {
	return proto.Marshal(m.metadata)
}

type SnapshotSink struct {
	wipDir   string
	finalDir string

	metadata *SnapshotMeta

	snapshotFile   *os.File
	snapshotWriter *bufio.Writer
}

func newSnapshotSink(wipDir, finalDir string, metadata *SnapshotMeta) *SnapshotSink {
	return &SnapshotSink{
		wipDir:   wipDir,
		finalDir: finalDir,
		metadata: metadata,
	}
}

func (s *SnapshotSink) writeMeta() error {
	file, err := os.OpenFile(filepath.Join(s.wipDir, "metadata"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	writer := raft.NewBufferedWriteCloser(file)
	defer writer.Close()
	metadataBytes, err := proto.Marshal(s.metadata.metadata)
	if err != nil {
		return err
	}
	if _, err := writer.Write(metadataBytes); err != nil {
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
	s.metadata.SetSize(s.metadata.Size() + uint64(n))
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

func (s *SnapshotSink) Id() string {
	return s.metadata.Id()
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

func (s *SnapshotStore) Create(index, term uint64, c *pb.Configuration) (raft.SnapshotSink, error) {
	id := raft.NewObjectID().Hex()

	wipDir := filepath.Join(s.storeDir, fmt.Sprintf("inprogress-%s", id))
	finalDir := filepath.Join(s.storeDir, id)

	if err := os.MkdirAll(wipDir, 0755); err != nil {
		return nil, err
	}

	sink := newSnapshotSink(wipDir, finalDir, &SnapshotMeta{
		metadata: &kvdbpb.SnapshotMeta{
			Id:            id,
			Index:         index,
			Term:          term,
			Configuration: c.Copy(),
		},
	})

	return sink, nil
}

func (s *SnapshotStore) List() ([]raft.SnapshotMeta, error) {
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
	metadataList := []raft.SnapshotMeta{}
	for _, id := range ids {
		file, err := os.Open(filepath.Join(s.storeDir, id, "metadata"))
		if err != nil {
			return nil, err
		}
		metadataBytes, err := io.ReadAll(file)
		file.Close()
		metadata, err := s.DecodeMeta(metadataBytes)
		if err != nil {
			return nil, err
		}
		metadataList = append(metadataList, metadata)
	}
	// Sort by index in descending order
	sort.SliceStable(metadataList, func(i, j int) bool {
		return metadataList[i].Index() > metadataList[j].Index()
	})
	return metadataList, nil
}

func (s *SnapshotStore) Open(id string) (*raft.Snapshot, error) {
	file, err := os.Open(filepath.Join(s.storeDir, id, "metadata"))
	if err != nil {
		return nil, err
	}
	metadataBytes, err := io.ReadAll(file)
	file.Close()
	var metadata kvdbpb.SnapshotMeta
	proto.Unmarshal(metadataBytes, &metadata)
	file, err = os.Open(filepath.Join(s.storeDir, id, "snapshot"))
	if err != nil {
		return nil, err
	}
	return &raft.Snapshot{
		Meta:   &SnapshotMeta{metadata: &metadata},
		Reader: raft.NewBufferedReadCloser(file),
	}, nil
}

func (s *SnapshotStore) DecodeMeta(b []byte) (raft.SnapshotMeta, error) {
	var metadata kvdbpb.SnapshotMeta
	proto.Unmarshal(b, &metadata)
	return &SnapshotMeta{metadata: &metadata}, nil
}
