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

type snapshotScheduler struct {
	server *Server

	ctlMu   sync.Mutex // protects ctl, started and stopped
	ctl     *asyncCtl
	started bool
	stopped bool

	applyCounterMu sync.Mutex // protects applyCounter
	applyCounter   int

	applies   int
	appliesCh chan struct{}

	interval       time.Duration
	intervalTicker *time.Ticker
}

func newSnapshotScheduler(server *Server) *snapshotScheduler {
	return &snapshotScheduler{
		server:    server,
		ctl:       newAsyncCtl(),
		applies:   server.opts.snapshotPolicy.Applies,
		appliesCh: make(chan struct{}, 1),
		interval:  server.opts.snapshotPolicy.Interval,
	}
}

func (s *snapshotScheduler) latestSnapshotID() (string, error) {
	metaList, err := s.server.snapshot.List()
	if err != nil {
		return "", err
	}
	if len(metaList) == 0 {
		return "", nil
	}
	return metaList[0].ID, nil
}

func (s *snapshotScheduler) RecordStateMachineApply() {
	if s.applies <= 0 {
		return
	}
	s.applyCounterMu.Lock()
	defer s.applyCounterMu.Unlock()
	s.applyCounter += 1
	if s.applyCounter >= s.applies {
		select {
		case s.appliesCh <- struct{}{}:
		case <-time.NewTimer(s.interval).C:
		}
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
			ticker.Reset(s.interval)
			s.server.snapshotCh <- nil
		case <-s.appliesCh:
			s.server.snapshotCh <- nil
		case <-ctl.Cancelled():
			return
		}
	}
}

func (s *snapshotScheduler) Start() {
	s.ctlMu.Lock()
	defer s.ctlMu.Unlock()

	if s.started {
		s.server.logger.Panic("attempt to start a started snapshotScheduler")
	}

	if s.stopped {
		s.server.logger.Panic("attempt to reuse a stopped snapshotScheduler")
	}

	s.started = true

	go s.schedule(s.ctl)
}

func (s *snapshotScheduler) Stop() {
	s.ctlMu.Lock()
	defer s.ctlMu.Unlock()

	if !s.started {
		s.server.logger.Panic("attempt to stop a snapshotScheduler which is not started")
	}

	if s.stopped {
		s.server.logger.Panic("attempt to stop a stopped snapshotScheduler")
	}

	s.ctl.Cancel()
	s.ctl.WaitRelease()
}
