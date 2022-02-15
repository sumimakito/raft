package raft

import (
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sumimakito/raft/pb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Snapshot is a descriptor that holds the snapshot file.
type Snapshot interface {
	Meta() (SnapshotMeta, error)
	Reader() (io.Reader, error)

	// Close is used to close the snapshot's underlying file descriptors or handles.
	Close() error
}

type SnapshotPolicy struct {
	Applies  int
	Interval time.Duration
}

type SnapshotMeta interface {
	Id() string
	Index() uint64
	Term() uint64
	Configuration() *pb.Configuration
	ConfigurationIndex() uint64
	Encode() ([]byte, error)
}

type SnapshotSink interface {
	io.WriteCloser
	Meta() SnapshotMeta
	Cancel() error
}

type SnapshatStore interface {
	Create(index, term uint64, c *pb.Configuration, cIndex uint64) (SnapshotSink, error)
	List() ([]SnapshotMeta, error)
	Open(id string) (Snapshot, error)
	DecodeMeta(b []byte) (SnapshotMeta, error)
	Trim() error
}

type snapshotScheduler struct {
	server  *Server
	service *snapshotService

	stopCh chan struct{}

	counterTimerMu sync.Mutex
	counterTimer   *CounterTimer
}

func newSnapshotScheduler(server *Server, service *snapshotService) *snapshotScheduler {
	s := &snapshotScheduler{
		server:  server,
		service: service,
		stopCh:  make(chan struct{}, 1),
		counterTimer: NewCounterTimer(
			server.opts.snapshotPolicy.Applies,
			server.opts.snapshotPolicy.Interval,
		),
	}

	go func() {
		s.server.logger.Infow("snapshotScheduler started")
		defer s.server.logger.Infow("snapshotScheduler stopped")
		for {
			select {
			case <-s.counterTimer.C():
				select {
				case s.service.snapshotCh <- struct{}{}:
				default:
				}
			case <-s.stopCh:
				s.counterTimer.Stop()
				return
			}
		}
	}()

	return s
}

// CountApply is called when a command has been applied to the StateMachine.
func (s *snapshotScheduler) CountApply() {
	s.counterTimer.Count()
}

func (s *snapshotScheduler) Stop() {
	close(s.stopCh)
}

// snapshotService is responsible for triggering snapshot creations under
// the SnapshotPolicy.
type snapshotService struct {
	server *Server

	startOnce sync.Once
	stopOnce  sync.Once

	schedulerMu sync.RWMutex
	scheduler   *snapshotScheduler

	snapshotCh chan struct{}
	stopCh     chan struct{}

	lastSnapshotConf *pb.Configuration
	lastSnapshotMeta SnapshotMeta
}

func newSnapshotService(server *Server) *snapshotService {
	s := &snapshotService{
		server:     server,
		snapshotCh: make(chan struct{}, 16),
		stopCh:     make(chan struct{}, 1),
	}

	return s
}

func (s *snapshotService) Start() {
	s.startOnce.Do(func() {
		go func() {
			for {
				select {
				case <-s.snapshotCh:
					s.TakeSnapshot()
				case <-s.stopCh:
					s.server.logger.Infow("snapshotService stopped")
					return
				}
			}
		}()
	})
}

func (s *snapshotService) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
}

func (s *snapshotService) Scheduler() *snapshotScheduler {
	return s.scheduler
}

func (s *snapshotService) StartScheduler() {
	s.schedulerMu.Lock()
	defer s.schedulerMu.Unlock()

	if s.scheduler != nil {
		s.server.logger.Panic("called StartScheduler() on a running snapshotService")
	}

	s.scheduler = newSnapshotScheduler(s.server, s)
}

func (s *snapshotService) StopScheduler() {
	s.schedulerMu.Lock()
	defer s.schedulerMu.Unlock()

	if s.scheduler == nil {
		s.server.logger.Panic("called StopScheduler() on an idle snapshotService")
	}
	s.scheduler.Stop()
	s.scheduler = nil
}

// TakeSnapshot is used to take a snapshot and trim log entries.
func (s *snapshotService) TakeSnapshot() (SnapshotMeta, error) {
	c := s.server.confStore.Committed()

	lastApplied := s.server.lastApplied()
	if lastApplied.Index == 0 {
		// It's unnecessary to take a snapshot since there're no applied logs.
		s.server.logger.Debugw("snapshot skipped: no applied logs", logFields(s.server)...)
		return nil, nil
	}

	// Check if our latest snapshot is stale
	if m := s.lastSnapshotMeta; m != nil {
		// Skip if the snapshot index and configuration are identical to current values.
		if m.Index() >= lastApplied.Index && proto.Equal(m.Configuration(), c.Configuration) {
			s.server.logger.Debugw("snapshot skipped: snapshot is not stale", logFields(s.server)...)
			return nil, nil
		}
	}

	stateMachineSnapshotFuture := newFutureTask[*stateMachineSnapshot, any](nil)
	s.server.stateMachineSnapshotCh <- stateMachineSnapshotFuture
	s.server.logger.Infow("enqueued state machine snapshot request", logFields(s.server)...)

	stmsSnapshot, err := stateMachineSnapshotFuture.Result()
	if err != nil {
		return nil, err
	}

	sink, err := s.server.snapshotStore.Create(stmsSnapshot.Index, stmsSnapshot.Term, c.Configuration, c.LogIndex())
	if err != nil {
		return nil, err
	}
	snapshotMeta := sink.Meta()

	if err := stmsSnapshot.Write(sink); err != nil {
		if cancelError := sink.Cancel(); cancelError != nil {
			return nil, errors.Wrap(cancelError, err.Error())
		}
		return nil, err
	}
	if err := sink.Close(); err != nil {
		return nil, err
	}

	restoreFuture := newFutureTask[any](snapshotMeta)
	s.server.logRestoreCh <- restoreFuture
	if _, err := restoreFuture.Result(); err != nil {
		return nil, err
	}

	s.lastSnapshotMeta = snapshotMeta

	s.server.logger.Infow("snapshot has been taken",
		logFields(s.server,
			zap.String("snapshot_id", snapshotMeta.Id()),
			zap.Uint64("snapshot_index", sink.Meta().Index()),
			zap.Uint64("snapshot_term", sink.Meta().Term()))...)

	return snapshotMeta, nil
}

// Restore must be called in a channel select branch
func (s *snapshotService) Restore(snapshotId string) (bool, error) {
	s.server.logger.Infow("ready to restore snapshot",
		logFields(s.server, zap.String("snapshot_id", snapshotId))...)
	snapshot, err := s.server.snapshotStore.Open(snapshotId)
	if err != nil {
		// It's recoverable if errors happen here.
		return false, err
	}

	snapshotMeta, err := snapshot.Meta()
	if err != nil {
		return false, err
	}

	// Check if the restoration is necessary.
	if snapshotMeta.Index() < s.server.firstLogIndex()-1 {
		// Restoration is not necessary.
		return false, nil
	}

	if err := s.server.stateMachine.Restore(snapshot); err != nil {
		return false, err
	}

	if err := s.server.logStore.Restore(snapshotMeta); err != nil {
		s.server.logger.Panicw("error occurred while triming logs during restoration",
			logFields(s.server, zap.Error(err))...)
	}

	s.server.setFirstLogIndex(Must2(s.server.logStore.FirstIndex()))
	s.server.setLastLogIndex(Must2(s.server.logStore.LastIndex()))

	s.server.commitAndApply(snapshotMeta.Index())

	s.server.alterConfiguration(newConfiguration(snapshotMeta.Configuration(), snapshotMeta.ConfigurationIndex()))
	return true, nil
}
