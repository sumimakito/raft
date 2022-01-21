package raft

import (
	"context"
	"sort"
	"sync"
)

type replCtl struct {
	replID ObjectID
	ctx    context.Context
	cancel context.CancelFunc
	c      chan struct{}
}

type replState struct {
	sched         *replScheduler
	peer          Peer
	configuration *Configuration

	nextIndex uint64

	ctlMu   sync.Mutex // protects ctl and stopped
	ctl     *replCtl
	stopped bool
}

func (s *replState) replicate(ctl *replCtl, resCh chan<- *AppendEntriesResponse) {
	defer close(ctl.c)
	defer ctl.cancel()

	var requestID ObjectID
	var request *AppendEntriesRequest
	var lastLogIndex uint64

	s.sched.server.logger.Infow("replication/heartbeat started",
		logFields(s.sched.server, "replication_id", ctl.replID, "peer", s.peer)...)
	defer s.sched.server.logger.Infow("replication/heartbeat stopped",
		logFields(s.sched.server, "replication_id", ctl.replID, "peer", s.peer)...)

	if s.peer.ID == s.sched.server.id {
	SELF_CHECK_INDEX:
		select {
		case <-ctl.ctx.Done():
			return
		default:
		}

		lastLogIndex = s.sched.server.lastLogIndex()
		// Check if there are more entries to replicate.
		matchIndex, ok := s.sched.matchIndexes.Load(s.peer.ID)
		if !ok {
			s.sched.server.logger.Panicw(
				"confusing condition: missing an entry in matchIndexes",
				logFields(s.sched.server, "missing_server_id", s.peer.ID)...,
			)
		}
		if lastLogIndex <= matchIndex.(uint64) {
			select {
			case <-ctl.ctx.Done():
				return
			case <-s.sched.server.randomTimer(s.sched.server.opts.followerTimeout / 10).C:
				goto SELF_CHECK_INDEX
			}
		}

		select {
		case <-ctl.ctx.Done():
			return
		default:
		}

		s.nextIndex = lastLogIndex + 1
		s.sched.updateMatchIndex(s.peer.ID, lastLogIndex)

		s.sched.server.logger.Infow("self replication state updated",
			logFields(s.sched.server, "replication_id", ctl.replID, "peer", s.peer)...)

		select {
		case <-ctl.ctx.Done():
			return
		case <-s.sched.server.randomTimer(s.sched.server.opts.followerTimeout / 10).C:
			goto SELF_CHECK_INDEX
		}
	}

CHECK_INDEX:
	select {
	case <-ctl.ctx.Done():
		return
	default:
	}

	lastLogIndex = s.sched.server.lastLogIndex()
	// Check if there are more entries to replicate.
	if lastLogIndex >= s.nextIndex {
		goto REPLICATE
	}
	s.sched.server.logger.Infow("no entries to replicate",
		logFields(s.sched.server,
			"replication_id", ctl.replID,
			"peer", s.peer,
			"request_id", requestID,
			"request", request)...)

	select {
	case <-ctl.ctx.Done():
		return
	default:
	}

	requestID, request = s.sched.prepareHeartbeat()
	s.sched.server.logger.Debugw("send heartbeat request",
		logFields(s.sched.server,
			"replication_id", ctl.replID,
			"peer", s.peer,
			"request_id", requestID,
			"request", request)...)
	if response, err := s.sched.server.trans.AppendEntries(ctl.ctx, s.peer, request); err != nil {
		s.sched.server.logger.Infow("error sending heartbeat request",
			logFields(s.sched.server,
				"replication_id", ctl.replID,
				"peer", s.peer,
				"request_id", requestID,
				"request", request,
				"error", err)...)
	} else {
		resCh <- response
	}

	select {
	case <-ctl.ctx.Done():
		return
	case <-s.sched.server.randomTimer(s.sched.server.opts.followerTimeout / 10).C:
		goto CHECK_INDEX
	}

REPLICATE:
	select {
	case <-ctl.ctx.Done():
		return
	default:
	}

	requestID, request = s.sched.prepareRequest(s.nextIndex, lastLogIndex)
	s.sched.server.logger.Debugw("send replication request",
		logFields(s.sched.server,
			"replication_id", ctl.replID,
			"peer", s.peer,
			"request_id", requestID,
			"request", request)...)
	if response, err := s.sched.server.trans.AppendEntries(ctl.ctx, s.peer, request); err != nil {
		s.sched.server.logger.Infow("error sending replication request",
			logFields(s.sched.server,
				"replication_id", ctl.replID,
				"peer", s.peer,
				"request_id", requestID,
				"request", request,
				"error", err)...)
	} else {
		if response.Success {
			s.sched.server.logger.Debugw("successful replication response",
				logFields(s.sched.server,
					"replication_id", ctl.replID,
					"peer", s.peer,
					"request_id", requestID,
					"response", response)...)
			s.nextIndex = lastLogIndex + 1
			s.sched.updateMatchIndex(s.peer.ID, lastLogIndex)
		} else {
			s.sched.server.logger.Debugw("unsuccessful replication repsonse",
				logFields(s.sched.server,
					"replication_id", ctl.replID,
					"peer", s.peer,
					"request_id", requestID,
					"response", response,
					"next_next_index", s.nextIndex-1)...)
			s.nextIndex = s.nextIndex - 1
		}
		resCh <- response
	}

	select {
	case <-ctl.ctx.Done():
		return
	case <-s.sched.server.randomTimer(s.sched.server.opts.followerTimeout / 10).C:
		goto CHECK_INDEX
	}
}

func (s *replState) Replicate(replID ObjectID, resCh chan<- *AppendEntriesResponse) {
	s.ctlMu.Lock()
	defer s.ctlMu.Unlock()

	if s.stopped {
		s.sched.server.logger.Panic("attempt to reuse a stopped replState")
	}

	ctx, cancel := context.WithCancel(context.Background())
	newCtl := &replCtl{replID: replID, ctx: ctx, cancel: cancel, c: make(chan struct{}, 1)}
	oldCtl := s.ctl
	s.ctl = newCtl
	if oldCtl != nil {
		oldCtl.cancel()
		<-oldCtl.c
	}
	go s.replicate(newCtl, resCh)
}

func (s *replState) Stop() {
	s.ctlMu.Lock()
	defer s.ctlMu.Unlock()

	if s.stopped {
		s.sched.server.logger.Panic("attempt to stop a stopped replState")
	}

	if s.ctl != nil {
		s.ctl.cancel()
		<-s.ctl.c
	}
}

type replScheduler struct {
	server *Server

	statesMu sync.Mutex // protects states
	states   map[ServerID]*replState

	matchIndexes sync.Map // map[ServerID]uint64
}

func newReplScheduler(server *Server) *replScheduler {
	return &replScheduler{
		server: server,
		states: map[ServerID]*replState{},
	}
}

func (r *replScheduler) prepareHeartbeat() (ObjectID, *AppendEntriesRequest) {
	return NewObjectID(), &AppendEntriesRequest{
		Term:         r.server.currentTerm(),
		LeaderID:     r.server.id,
		LeaderCommit: r.server.commitIndex(),
		PrevLogTerm:  0,
		PrevLogIndex: 0,
		Entries:      []*Log{},
	}
}

func (r *replScheduler) prepareRequest(firstIndex, lastIndex uint64) (ObjectID, *AppendEntriesRequest) {
	requestID := NewObjectID()

	request := &AppendEntriesRequest{
		Term:         r.server.currentTerm(),
		LeaderID:     r.server.id,
		LeaderCommit: r.server.commitIndex(),
		PrevLogTerm:  0,
		PrevLogIndex: 0,
		Entries:      []*Log{},
	}

	if prevLogIndex := firstIndex - 1; prevLogIndex > 0 {
		log := r.server.logStore.Entry(prevLogIndex)
		request.PrevLogTerm = log.Term
		request.PrevLogIndex = log.Index
	}

	lastLogIndex := r.server.lastLogIndex()
	if firstIndex > lastLogIndex || (firstIndex == lastLogIndex && firstIndex == 0) {
		return requestID, request
	}

	request.Entries = make([]*Log, 0, lastLogIndex-firstIndex+1)
	for i := firstIndex; i <= lastLogIndex; i++ {
		request.Entries = append(request.Entries, r.server.logStore.Entry(i))
	}

	return requestID, request
}

func (r *replScheduler) updateMatchIndex(serverID ServerID, matchIndex uint64) {
	c := r.server.confStore.Latest()
	r.matchIndexes.Store(serverID, matchIndex)
	r.server.updateCommitIndex(r.nextCommitIndexLocked(c))
}

func (r *replScheduler) nextCommitIndexLocked(c *Configuration) uint64 {
	matchIndexes := map[ServerID]uint64{}
	r.matchIndexes.Range(func(key, value any) bool {
		matchIndexes[key.(ServerID)] = value.(uint64)
		return true
	})
	r.server.logger.Info(matchIndexes)

	if !c.Joint() {
		currentIndexes := make([]uint64, 0, len(c.Current.Peers))
		for _, p := range c.Current.Peers {
			if index, ok := matchIndexes[p.ID]; ok {
				currentIndexes = append(currentIndexes, index)
			} else {
				r.server.logger.Panicw(
					"confusing condition: found a server ID that does not belong to current configuration",
					logFields(r.server, "orphan_server_id", p.ID)...,
				)
			}
		}
		sort.SliceStable(currentIndexes, func(i, j int) bool { return currentIndexes[i] < currentIndexes[j] })
		commitIndex := currentIndexes[len(currentIndexes)-c.Current.Quorum()]
		r.server.logger.Infow("next commit index", logFields(r.server, "next_commit_index", commitIndex)...)
		return commitIndex
	} else {
		currentIndexes := make([]uint64, 0, len(c.Current.Peers))
		nextIndexes := make([]uint64, 0, len(c.Next.Peers))
		for _, p := range c.Peers() {
			if c.Current.Contains(p.ID) {
				if index, ok := matchIndexes[p.ID]; ok {
					currentIndexes = append(currentIndexes, index)
				} else {
					r.server.logger.Panicw(
						"confusing condition: found a server ID that does not belong to current configuration",
						logFields(r.server, "orphan_server_id", p.ID)...,
					)
				}
			} else if c.Next.Contains(p.ID) {
				if index, ok := matchIndexes[p.ID]; ok {
					nextIndexes = append(nextIndexes, index)
				} else {
					r.server.logger.Panicw(
						"confusing condition: found a server ID that does not belong to next configuration",
						logFields(r.server, "orphan_server_id", p.ID)...,
					)
				}
			} else {
				r.server.logger.Panicw(
					"confusing condition: found a server ID that does not belong to both any configuration",
					logFields(r.server, "orphan_server_id", p.ID)...,
				)
			}
		}
		sort.SliceStable(currentIndexes, func(i, j int) bool {
			return currentIndexes[i] < currentIndexes[j]
		})
		sort.SliceStable(nextIndexes, func(i, j int) bool {
			return nextIndexes[i] < nextIndexes[j]
		})
		commitIndex := currentIndexes[len(currentIndexes)-c.Current.Quorum()]
		if index := currentIndexes[len(currentIndexes)-c.Current.Quorum()]; index < commitIndex {
			commitIndex = index
		}
		r.server.logger.Infow("next commit index", logFields(r.server, "next_commit_index", commitIndex)...)
		return commitIndex
	}
}

func (r *replScheduler) Start() <-chan *AppendEntriesResponse {
	c := r.server.confStore.Latest()

	replID := NewObjectID()
	r.server.logger.Infow("replication/heartbeat scheduled",
		logFields(r.server, "replication_id", replID)...)

	resCh := make(chan *AppendEntriesResponse, len(c.Peers())*2)

	r.statesMu.Lock()
	r.states = map[ServerID]*replState{}
	for _, p := range c.Peers() {
		if p.ID == r.server.id {
			r.states[p.ID] = &replState{
				sched:         r,
				peer:          p,
				configuration: c,
				nextIndex:     r.server.lastLogIndex() + 1,
			}
			r.matchIndexes.Store(p.ID, r.server.lastLogIndex())
		} else {
			r.states[p.ID] = &replState{
				sched:         r,
				peer:          p,
				configuration: c,
				nextIndex:     r.server.lastLogIndex(),
			}
			r.matchIndexes.Store(p.ID, 0)
		}
	}
	for _, s := range r.states {
		s.Replicate(replID, resCh)
	}
	r.statesMu.Unlock()

	return resCh
}

func (r *replScheduler) Stop() {
	r.server.logger.Infow("ready to stop all replications", logFields(r.server)...)
	r.statesMu.Lock()
	defer r.statesMu.Unlock()

	var w sync.WaitGroup
	w.Add(len(r.states))
	for _, s := range r.states {
		go func(s *replState) { s.Stop(); w.Done() }(s)
	}
	r.states = map[ServerID]*replState{}
	w.Wait()
	r.server.logger.Infow("all replications stopped", logFields(r.server)...)
}
