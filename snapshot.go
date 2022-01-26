package raft

import (
	"io"
	"sync"
	"time"

	"github.com/sumimakito/raft/pb"
)

type Snapshot struct {
	Meta   SnapshotMeta
	Reader io.ReadCloser
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
	Id() string
	Cancel() error
}

type SnapshotStore interface {
	Create(index, term uint64, c *pb.Configuration) (SnapshotSink, error)
	List() ([]SnapshotMeta, error)
	Open(id string) (*Snapshot, error)
	DecodeMeta(b []byte) (SnapshotMeta, error)

	// Trim trims the snapshot store by evicting snapshots with indexes smaller
	// than the provided index.
	Trim(index uint64) error
}

// snapshotScheduler is responsible for triggering snapshot creations under
// the SnapshotPolicy.
type snapshotScheduler struct {
	server *Server
	mu     sync.Mutex

	interval  time.Duration
	applies   int
	appliesCh chan struct{}

	ctl *asyncCtl

	applyCounter int
}

func newSnapshotScheduler(server *Server) *snapshotScheduler {
	return &snapshotScheduler{
		server:    server,
		interval:  server.opts.snapshotPolicy.Interval,
		applies:   server.opts.snapshotPolicy.Applies,
		appliesCh: make(chan struct{}, 1),
	}
}

func (s *snapshotScheduler) schedule(ctl *asyncCtl) {
	defer ctl.Release()

	var ticker *time.Ticker
	var tickerCh <-chan time.Time

	if s.interval > 0 {
		ticker = time.NewTicker(s.interval)
		defer ticker.Stop()
		tickerCh = ticker.C
	}

	for {
		select {
		case <-tickerCh:
			s.server.snapshotCh <- nil
		case <-s.appliesCh:
			ticker.Reset(s.interval)
			s.server.snapshotCh <- nil
		case <-ctl.Cancelled():
			return
		}
	}
}

// RecordApply is called when a command has been applied to the StateMachine.
func (s *snapshotScheduler) RecordApply() {
	if s.applies <= 0 {
		// "Applies" in the SnapshotPolicy is disabled.
		return
	}
	s.mu.Lock()

	s.applyCounter += 1
	if s.applyCounter >= s.applies {
		s.applyCounter = 0
		s.mu.Unlock()
		select {
		case s.appliesCh <- struct{}{}:
		case <-time.NewTimer(s.interval).C:
		case <-s.ctl.Cancelled():
		}
	} else {
		s.mu.Unlock()
	}
}

func (s *snapshotScheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ctl != nil {
		select {
		case <-s.ctl.Cancelled():
			s.server.logger.Panic("attempt to reuse a stopped snapshotScheduler")
		default:
			s.server.logger.Panic("attempt to start a started snapshotScheduler")
		}
	}

	s.ctl = newAsyncCtl()

	go s.schedule(s.ctl)
}

func (s *snapshotScheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ctl == nil {
		s.server.logger.Panic("attempt to stop a snapshotScheduler which is not started")
	}

	select {
	case <-s.ctl.Cancelled():
		s.server.logger.Panic("attempt to stop a stopped snapshotScheduler")
	default:
		s.ctl.Cancel()
	}

	<-s.ctl.WaitRelease()
}
