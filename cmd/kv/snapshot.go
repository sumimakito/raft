package main

import (
	"bufio"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sumimakito/raft"
	kvpb "github.com/sumimakito/raft/cmd/kv/pb"
	"github.com/sumimakito/raft/pb"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

type Snapshot struct {
	metadata *SnapshotMeta
	reader   io.ReadCloser
}

func newSnapshot(snapshotDir string) (*Snapshot, error) {
	metadataFile, err := os.OpenFile(filepath.Join(snapshotDir, "metadata"), os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}
	metadataBytes, err := ioutil.ReadAll(metadataFile)
	if err := metadataFile.Close(); err != nil {
		return nil, err
	}
	var pbMetadata kvpb.SnapshotMeta
	proto.Unmarshal(metadataBytes, &pbMetadata)
	snapshotFile, err := os.OpenFile(filepath.Join(snapshotDir, "snapshot"), os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		metadata: &SnapshotMeta{pbMetadata: &pbMetadata},
		reader:   raft.NewBufferedReadCloser(snapshotFile),
	}, nil
}

func (s *Snapshot) Meta() (raft.SnapshotMeta, error) {
	return s.metadata, nil
}

func (s *Snapshot) Reader() (io.Reader, error) {
	return s.reader, nil
}

func (s *Snapshot) Close() error {
	return s.reader.Close()
}

type SnapshotMeta struct {
	pbMetadata *kvpb.SnapshotMeta
}

func (m *SnapshotMeta) Id() string {
	return m.pbMetadata.Id
}

func (m *SnapshotMeta) SetId(id string) {
	m.pbMetadata.Id = id
}

func (m *SnapshotMeta) Index() uint64 {
	return m.pbMetadata.Index
}

func (m *SnapshotMeta) SetIndex(index uint64) {
	m.pbMetadata.Index = index
}

func (m *SnapshotMeta) Term() uint64 {
	return m.pbMetadata.Term
}

func (m *SnapshotMeta) SetTerm(term uint64) {
	m.pbMetadata.Term = term
}

func (m *SnapshotMeta) Configuration() *pb.Configuration {
	return m.pbMetadata.Configuration
}

func (m *SnapshotMeta) SetConfiguration(configuration *pb.Configuration) {
	m.pbMetadata.Configuration = configuration
}

func (m *SnapshotMeta) ConfigurationIndex() uint64 {
	return m.pbMetadata.ConfigurationIndex
}

func (m *SnapshotMeta) Size() uint64 {
	return m.pbMetadata.Size
}

func (m *SnapshotMeta) SetSize(size uint64) {
	m.pbMetadata.Size = size
}

func (m *SnapshotMeta) CRC64() uint64 {
	return m.pbMetadata.Crc64
}

func (m *SnapshotMeta) SetCRC64(crc64 uint64) {
	m.pbMetadata.Crc64 = crc64
}

func (m *SnapshotMeta) Encode() ([]byte, error) {
	return proto.Marshal(m.pbMetadata)
}

func (m *SnapshotMeta) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return m.pbMetadata.MarshalLogObject(e)
}

type SnapshotSink struct {
	wipDir   string
	finalDir string

	metadata *SnapshotMeta

	metadataFile   *os.File
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
	metadataBytes, err := proto.Marshal(s.metadata.pbMetadata)
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

func (s *SnapshotSink) Meta() raft.SnapshotMeta {
	return s.metadata
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

func (s *SnapshotSink) Cancel() error {
	if err := s.close(); err != nil {
		return err
	}
	if err := os.RemoveAll(s.wipDir); err != nil {
		return err
	}
	return nil
}

type SnapshotProvider struct {
	storeDir string
}

func NewSnapshotProvider(storeDir string) *SnapshotProvider {
	return &SnapshotProvider{storeDir: storeDir}
}

func (s *SnapshotProvider) listDirnames() ([]string, []string, error) {
	complete := []string{}
	inprogress := []string{}
	if err := filepath.WalkDir(s.storeDir, func(path string, d fs.DirEntry, err error) error {
		if path == s.storeDir || !d.IsDir() {
			return nil
		}
		if !strings.HasPrefix(d.Name(), "inprogress-") {
			complete = append(complete, d.Name())
		} else {
			inprogress = append(inprogress, d.Name())
		}
		return filepath.SkipDir
	}); err != nil {
		return nil, nil, err
	}
	return complete, inprogress, nil
}

func (s *SnapshotProvider) sortMeta(dirnames []string) ([]raft.SnapshotMeta, error) {
	metadataList := []raft.SnapshotMeta{}
	for _, dirname := range dirnames {
		file, err := os.Open(filepath.Join(s.storeDir, dirname, "metadata"))
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

func (s *SnapshotProvider) Create(index, term uint64, c *pb.Configuration, cIndex uint64) (raft.SnapshotSink, error) {
	id := raft.NewObjectID().Hex()

	wipDir := filepath.Join(s.storeDir, fmt.Sprintf("inprogress-%s", id))
	finalDir := filepath.Join(s.storeDir, id)

	if err := os.MkdirAll(wipDir, 0755); err != nil {
		return nil, err
	}

	sink := newSnapshotSink(wipDir, finalDir, &SnapshotMeta{
		pbMetadata: &kvpb.SnapshotMeta{
			Id:                 id,
			Index:              index,
			Term:               term,
			Configuration:      c.Copy(),
			ConfigurationIndex: cIndex,
		},
	})

	return sink, nil
}

func (s *SnapshotProvider) List() ([]raft.SnapshotMeta, error) {
	complete, _, err := s.listDirnames()
	if err != nil {
		return nil, err
	}
	return s.sortMeta(complete)
}

func (s *SnapshotProvider) Open(id string) (raft.Snapshot, error) {
	return newSnapshot(filepath.Join(s.storeDir, id))
}

func (s *SnapshotProvider) DecodeMeta(b []byte) (raft.SnapshotMeta, error) {
	var pbMetadata kvpb.SnapshotMeta
	proto.Unmarshal(b, &pbMetadata)
	return &SnapshotMeta{pbMetadata: &pbMetadata}, nil
}

// TODO: Refactor this
func (s *SnapshotProvider) Trim() error {
	complete, inprogress, err := s.listDirnames()
	if err != nil {
		return err
	}
	// Evict in-progress snapshots
	for _, dirname := range inprogress {
		if err := os.RemoveAll(filepath.Join(s.storeDir, dirname)); err != nil {
			return err
		}
	}
	// Evict complete snapshots
	metadataList, err := s.sortMeta(complete)
	if err != nil {
		return err
	}
	for _, metadata := range metadataList[1:] {
		if err := os.RemoveAll(filepath.Join(s.storeDir, metadata.Id())); err != nil {
			return err
		}
	}
	return nil
}
