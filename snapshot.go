package raft

import (
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sumimakito/raft/pb"
	"go.uber.org/zap"
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
	Encode() ([]byte, error)
}

type SnapshotSink interface {
	io.WriteCloser
	Meta() SnapshotMeta
	Cancel() error
}

type SnapshotProvider interface {
	Create(index, term uint64, c *pb.Configuration) (SnapshotSink, error)
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

	schedulerMu sync.RWMutex
	scheduler   *snapshotScheduler

	snapshotCh chan struct{}
	stopCh     chan struct{}

	lastSnapshotMeta SnapshotMeta
}

func newSnapshotService(server *Server) *snapshotService {
	s := &snapshotService{
		server:     server,
		snapshotCh: make(chan struct{}, 16),
		stopCh:     make(chan struct{}, 1),
	}

	go func() {
		for {
			select {
			case <-s.snapshotCh:
				s.TakeSnapshot()
			case <-s.stopCh:
				server.logger.Infow("snapshotService stopped")
				return
			}
		}
	}()

	return s
}

func (s *snapshotService) Stop() {
	close(s.stopCh)
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

func (s *snapshotService) TakeSnapshot() (SnapshotMeta, error) {
	c := s.server.confStore.Latest()

	// Check if our latest snapshot is stale
	if m := s.lastSnapshotMeta; m != nil && m.Index() >= s.server.lastAppliedIndex() {
		s.server.logger.Debugw("snapshot skipped", logFields(s.server)...)
		return nil, nil
	}

	stateMachineSnapshotFuture := newFutureTask[StateMachineSnapshot, any](nil)

	s.server.stateMachineSnapshotCh <- stateMachineSnapshotFuture
	s.server.logger.Infow("enqueued state machine snapshot request", logFields(s.server)...)

	smSnapshot, err := stateMachineSnapshotFuture.Result()
	if err != nil {
		return nil, err
	}

	sink, err := s.server.snapshotProvider.Create(smSnapshot.Index(), smSnapshot.Term(), c.Configuration)
	if err != nil {
		return nil, err
	}
	snapshotMeta := sink.Meta()

	if err := smSnapshot.Write(sink); err != nil {
		if cancelError := sink.Cancel(); cancelError != nil {
			return nil, errors.Wrap(cancelError, err.Error())
		}
		return nil, err
	}
	if err := sink.Close(); err != nil {
		return nil, err
	}

	// Trim the logs which were included in the snapshot.
	trimOp := &logProviderTrimOp{
		Type:       logProviderTrimPrefix,
		FutureTask: newFutureTask[any](smSnapshot.Index() + 1),
	}
	s.server.logOpsCh <- trimOp
	if _, err := trimOp.Result(); err != nil {
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
	snapshot, err := s.server.snapshotProvider.Open(snapshotId)
	if err != nil {
		// It's recoverable if errors happen here.
		return false, err
	}

	snapshotMeta, err := snapshot.Meta()
	if err != nil {
		return false, err
	}

	// Check if the restoration is necessary.
	if snapshotMeta.Index() < s.server.firstLogIndex() {
		// Restoration is not necessary.
		return false, nil
	}

	if err := s.server.stateMachine.Restore(snapshot); err != nil {
		return false, err
	}
	// We can directly call TrimPrefix() since there're no concurrent writes to th log provider
	if err := s.server.logProvider.TrimPrefix(snapshotMeta.Index() + 1); err != nil {
		s.server.logger.Panicw("error occurred while triming logs during restoration",
			logFields(s.server, zap.Error(err))...)
	}

	s.server.setFirstLogIndex(Must2(s.server.logProvider.FirstIndex()))
	s.server.setLastLogIndex(Must2(s.server.logProvider.LastIndex()))

	s.server.alterConfiguration(newConfiguration(snapshotMeta.Configuration()))
	s.server.reselectLoop()
	return true, nil
}
