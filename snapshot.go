package raft

import (
	"io"
	"sync"
	"time"

	"github.com/ugorji/go/codec"
)

type Snapshot interface {
	Meta() SnapshotMeta
}

type SnapshotPolicy struct {
	Applies  int
	Interval time.Duration
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
		ctl:       newAsyncCtl(),
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
		s.server.logger.Panic("attempt to start a started snapshotScheduler")
	}

	select {
	case <-s.ctl.Cancelled():
		s.server.logger.Panic("attempt to reuse a stopped snapshotScheduler")
	default:
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
