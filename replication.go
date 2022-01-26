package raft

import (
	"sort"
	"sync"

	"github.com/sumimakito/raft/pb"
	"go.uber.org/zap"
)

type replCtl struct {
	*asyncCtl
	replId string
}

type replState struct {
	sched         *replScheduler
	peer          *pb.Peer
	configuration *Configuration

	nextIndex uint64

	ctlMu   sync.Mutex // protects ctl and stopped
	ctl     *replCtl
	stopped bool
}

func (s *replState) replicate(ctl *replCtl, resCh chan<- *pb.AppendEntriesResponse) {
	defer ctl.Release()

	var lastLogIndex uint64

	goto ENTRY

WAIT_NEXT:
	select {
	case <-ctl.Cancelled():
		return
	case <-s.sched.server.randomTimer(s.sched.server.opts.followerTimeout / 10).C:
		goto CHECK_INDEX
	}

ENTRY:
	s.sched.server.logger.Infow("replication/heartbeat started",
		logFields(s.sched.server,
			zap.String("replication_id", ctl.replId),
			zap.Object("peer", s.peer))...)
	defer s.sched.server.logger.Infow("replication/heartbeat stopped",
		logFields(s.sched.server,
			zap.String("replication_id", ctl.replId),
			zap.Object("peer", s.peer))...)

	if s.peer.Id == s.sched.server.id {
	SELF_CHECK_INDEX:
		select {
		case <-ctl.Cancelled():
			return
		default:
		}

		lastLogIndex = s.sched.server.lastLogIndex()
		// Check if there are more entries to replicate.
		matchIndex, ok := s.sched.matchIndexes.Load(s.peer.Id)
		if !ok {
			s.sched.server.logger.Panicw(
				"confusing condition: missing an entry in matchIndexes",
				logFields(s.sched.server, "missing_server_id", s.peer.Id)...,
			)
		}
		if lastLogIndex <= matchIndex.(uint64) {
			select {
			case <-ctl.Cancelled():
				return
			case <-s.sched.server.randomTimer(s.sched.server.opts.followerTimeout / 10).C:
				goto SELF_CHECK_INDEX
			}
		}

		select {
		case <-ctl.Cancelled():
			return
		default:
		}

		s.nextIndex = lastLogIndex + 1
		s.sched.updateMatchIndex(s.peer.Id, lastLogIndex)

		s.sched.server.logger.Infow("self replication state updated",
			logFields(s.sched.server,
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer))...)

		select {
		case <-ctl.Cancelled():
			return
		case <-s.sched.server.randomTimer(s.sched.server.opts.followerTimeout / 10).C:
			goto SELF_CHECK_INDEX
		}
	}

CHECK_INDEX:
	select {
	case <-ctl.Cancelled():
		return
	default:
	}

	var heartbeatRequestId string
	var heartbeaRequest *pb.AppendEntriesRequest

	lastLogIndex = s.sched.server.lastLogIndex()
	// Check if there are more entries to replicate.
	if lastLogIndex >= s.nextIndex {
		goto REPLICATE
	}
	s.sched.server.logger.Infow("no entries to replicate",
		logFields(s.sched.server,
			zap.String("replication_id", ctl.replId),
			zap.Object("peer", s.peer))...)

	select {
	case <-ctl.Cancelled():
		return
	default:
	}

	heartbeatRequestId, heartbeaRequest = s.sched.prepareHeartbeat()
	s.sched.server.logger.Debugw("send heartbeat request",
		logFields(s.sched.server,
			zap.String("replication_id", ctl.replId),
			zap.Object("peer", s.peer),
			zap.String("request_id", heartbeatRequestId),
			zap.Reflect("request", heartbeaRequest))...)
	if heartbeatResponse, err := s.sched.server.trans.AppendEntries(ctl.Context(), s.peer, heartbeaRequest); err != nil {
		s.sched.server.logger.Infow("error sending heartbeat request",
			logFields(s.sched.server,
				zap.Error(err),
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer),
				zap.String("request_id", heartbeatRequestId),
				zap.Reflect("request", heartbeaRequest))...)
	} else {
		resCh <- heartbeatResponse
	}

	goto WAIT_NEXT

REPLICATE:
	select {
	case <-ctl.Cancelled():
		return
	default:
	}

	replicationRequestId, replicationRequest, err := s.sched.prepareRequest(s.nextIndex, lastLogIndex)
	if err != nil {
		s.sched.server.logger.Infow("error preparing replication request",
			logFields(s.sched.server,
				zap.Error(err),
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer),
				zap.String("request_id", replicationRequestId),
				zap.Reflect("request", replicationRequest))...)
		goto WAIT_NEXT
	}
	s.sched.server.logger.Debugw("send replication request",
		logFields(s.sched.server,
			zap.String("replication_id", ctl.replId),
			zap.Object("peer", s.peer),
			zap.String("request_id", replicationRequestId),
			zap.Reflect("request", replicationRequest))...)
	replicationResponse, err := s.sched.server.trans.AppendEntries(ctl.Context(), s.peer, replicationRequest)
	if err != nil {
		s.sched.server.logger.Infow("error sending replication request",
			logFields(s.sched.server,
				zap.Error(err),
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer),
				zap.String("request_id", replicationRequestId),
				zap.Reflect("request", replicationRequest))...)
		goto WAIT_NEXT
	}

	if replicationResponse.Success {
		s.sched.server.logger.Debugw("successful replication response",
			logFields(s.sched.server,
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer),
				zap.String("request_id", replicationRequestId),
				zap.Reflect("request", replicationResponse))...)
		s.nextIndex = lastLogIndex + 1
		s.sched.updateMatchIndex(s.peer.Id, lastLogIndex)
	} else {
		s.sched.server.logger.Debugw("unsuccessful replication repsonse",
			logFields(s.sched.server,
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer),
				zap.String("request_id", replicationRequestId),
				zap.Reflect("request", replicationResponse))...)
		goto INSTALL_SNAPSHOT
		// s.nextIndex = s.nextIndex - 1
	}
	resCh <- replicationResponse

	goto WAIT_NEXT

INSTALL_SNAPSHOT:
	select {
	case <-ctl.Cancelled():
		return
	default:
	}

	s.sched.server.logger.Infow("ready to install snapshot",
		logFields(s.sched.server,
			zap.String("replication_id", ctl.replId),
			zap.Object("peer", s.peer))...)

	metadataList, err := s.sched.server.snapshot.List()
	if err != nil {
		s.sched.server.logger.Infow("error listing snapshots",
			logFields(s.sched.server,
				zap.Error(err),
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer))...)
		goto WAIT_NEXT
	}

	if len(metadataList) == 0 {
		s.sched.server.logger.Infow("no snapshots to install",
			logFields(s.sched.server,
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer))...)
		goto WAIT_NEXT
	}

	snapshot, err := s.sched.server.snapshot.Open(metadataList[0].Id())
	if err != nil {
		s.sched.server.logger.Infow("error opening snapshot",
			logFields(s.sched.server,
				zap.Error(err),
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer),
				zap.String("snapshot_id", snapshot.Meta.Id()))...)
		goto WAIT_NEXT
	}

	snapshotMetaBytes, err := snapshot.Meta.Encode()
	if err != nil {
		s.sched.server.logger.Infow("error encoding snapshot metadata",
			logFields(s.sched.server,
				zap.Error(err),
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer),
				zap.String("snapshot_id", snapshot.Meta.Id()))...)
		goto WAIT_NEXT
	}

	requestMeta := &pb.InstallSnapshotRequestMeta{
		Term:              s.sched.server.currentTerm(),
		LeaderId:          s.sched.server.Leader().Id,
		LastIncludedIndex: snapshot.Meta.Index(),
		LastIncludedTerm:  snapshot.Meta.Term(),
		SnapshotMetadata:  snapshotMetaBytes,
	}

	if _, err := s.sched.server.trans.InstallSnapshot(ctl.Context(), s.peer, requestMeta, snapshot.Reader); err != nil {
		s.sched.server.logger.Infow("error installing snapshot",
			logFields(s.sched.server,
				zap.Error(err),
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer),
				zap.String("snapshot_id", snapshot.Meta.Id()))...)
		goto WAIT_NEXT
	}

	s.sched.server.logger.Infow("snapshot installed",
		logFields(s.sched.server,
			zap.String("replication_id", ctl.replId),
			zap.Object("peer", s.peer),
			zap.String("snapshot_id", snapshot.Meta.Id()))...)

	goto WAIT_NEXT
}

func (s *replState) Replicate(replID string, resCh chan<- *pb.AppendEntriesResponse) {
	s.ctlMu.Lock()
	defer s.ctlMu.Unlock()

	if s.stopped {
		s.sched.server.logger.Panic("attempt to reuse a stopped replState")
	}

	newCtl := &replCtl{asyncCtl: newAsyncCtl(), replId: replID}
	oldCtl := s.ctl
	s.ctl = newCtl
	if oldCtl != nil {
		oldCtl.Cancel()
		<-oldCtl.WaitRelease()
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
		s.ctl.Cancel()
		<-s.ctl.WaitRelease()
	}
}

type replScheduler struct {
	server *Server

	statesMu sync.Mutex // protects states
	states   map[string]*replState

	matchIndexes sync.Map // map[ServerID]uint64
}

func newReplScheduler(server *Server) *replScheduler {
	return &replScheduler{
		server: server,
		states: map[string]*replState{},
	}
}

func (r *replScheduler) prepareHeartbeat() (string, *pb.AppendEntriesRequest) {
	return NewObjectID().Hex(), &pb.AppendEntriesRequest{
		Term:         r.server.currentTerm(),
		LeaderId:     r.server.id,
		LeaderCommit: r.server.commitIndex(),
		PrevLogTerm:  0,
		PrevLogIndex: 0,
		Entries:      []*pb.Log{},
	}
}

func (r *replScheduler) prepareRequest(firstIndex, lastIndex uint64) (string, *pb.AppendEntriesRequest, error) {
	requestId := NewObjectID().Hex()

	request := &pb.AppendEntriesRequest{
		Term:         r.server.currentTerm(),
		LeaderId:     r.server.id,
		LeaderCommit: r.server.commitIndex(),
		PrevLogTerm:  0,
		PrevLogIndex: 0,
		Entries:      []*pb.Log{},
	}

	if prevLogIndex := firstIndex - 1; prevLogIndex > 0 {
		log, err := r.server.logStore.Entry(prevLogIndex)
		if err != nil {
			return "", nil, err
		}
		if log != nil {
			request.PrevLogTerm = log.Meta.Term
			request.PrevLogIndex = log.Meta.Index
		}
	}

	lastLogIndex := r.server.lastLogIndex()
	if firstIndex > lastLogIndex || (firstIndex == lastLogIndex && firstIndex == 0) {
		return requestId, request, nil
	}

	request.Entries = make([]*pb.Log, 0, lastLogIndex-firstIndex+1)
	for i := firstIndex; i <= lastLogIndex; i++ {
		e, err := r.server.logStore.Entry(i)
		if err != nil {
			return "", nil, nil
		}
		request.Entries = append(request.Entries, e.Copy())
	}

	return requestId, request, nil
}

func (r *replScheduler) updateMatchIndex(serverID string, matchIndex uint64) {
	c := r.server.confStore.Latest()
	r.matchIndexes.Store(serverID, matchIndex)
	r.server.updateCommitIndex(r.nextCommitIndexLocked(c))
}

func (r *replScheduler) nextCommitIndexLocked(c *Configuration) uint64 {
	matchIndexes := map[string]uint64{}
	r.matchIndexes.Range(func(key, value any) bool {
		matchIndexes[key.(string)] = value.(uint64)
		return true
	})
	r.server.logger.Info(matchIndexes)

	if !c.Joint() {
		currentIndexes := make([]uint64, 0, len(c.Current.Peers))
		for _, p := range c.Current.Peers {
			if index, ok := matchIndexes[p.Id]; ok {
				currentIndexes = append(currentIndexes, index)
			} else {
				r.server.logger.Panicw(
					"confusing condition: found a server ID that does not belong to current configuration",
					logFields(r.server, "orphan_server_id", p.Id)...,
				)
			}
		}
		sort.SliceStable(currentIndexes, func(i, j int) bool { return currentIndexes[i] < currentIndexes[j] })
		commitIndex := currentIndexes[len(currentIndexes)-c.CurrentConfig().Quorum()]
		r.server.logger.Infow("next commit index", logFields(r.server, "next_commit_index", commitIndex)...)
		return commitIndex
	} else {
		currentIndexes := make([]uint64, 0, len(c.Current.Peers))
		nextIndexes := make([]uint64, 0, len(c.Next.Peers))
		for _, p := range c.Peers() {
			if c.CurrentConfig().Contains(p.Id) {
				if index, ok := matchIndexes[p.Id]; ok {
					currentIndexes = append(currentIndexes, index)
				} else {
					r.server.logger.Panicw(
						"confusing condition: found a server ID that does not belong to current configuration",
						logFields(r.server, "orphan_server_id", p.Id)...,
					)
				}
			} else if c.NextConfig().Contains(p.Id) {
				if index, ok := matchIndexes[p.Id]; ok {
					nextIndexes = append(nextIndexes, index)
				} else {
					r.server.logger.Panicw(
						"confusing condition: found a server ID that does not belong to next configuration",
						logFields(r.server, "orphan_server_id", p.Id)...,
					)
				}
			} else {
				r.server.logger.Panicw(
					"confusing condition: found a server ID that does not belong to both any configuration",
					logFields(r.server, "orphan_server_id", p.Id)...,
				)
			}
		}
		sort.SliceStable(currentIndexes, func(i, j int) bool {
			return currentIndexes[i] < currentIndexes[j]
		})
		sort.SliceStable(nextIndexes, func(i, j int) bool {
			return nextIndexes[i] < nextIndexes[j]
		})
		commitIndex := currentIndexes[len(currentIndexes)-c.CurrentConfig().Quorum()]
		if index := currentIndexes[len(currentIndexes)-c.CurrentConfig().Quorum()]; index < commitIndex {
			commitIndex = index
		}
		r.server.logger.Infow("next commit index", logFields(r.server, "next_commit_index", commitIndex)...)
		return commitIndex
	}
}

func (r *replScheduler) Start() <-chan *pb.AppendEntriesResponse {
	c := r.server.confStore.Latest()

	replID := NewObjectID().Hex()
	r.server.logger.Infow("replication/heartbeat scheduled",
		logFields(r.server, "replication_id", replID)...)

	resCh := make(chan *pb.AppendEntriesResponse, len(c.Peers())*2)

	r.statesMu.Lock()
	r.states = map[string]*replState{}
	for _, p := range c.Peers() {
		if p.Id == r.server.id {
			r.states[p.Id] = &replState{
				sched:         r,
				peer:          p,
				configuration: c,
				nextIndex:     r.server.lastLogIndex() + 1,
			}
			r.matchIndexes.Store(p.Id, r.server.lastLogIndex())
		} else {
			r.states[p.Id] = &replState{
				sched:         r,
				peer:          p,
				configuration: c,
				nextIndex:     r.server.lastLogIndex(),
			}
			r.matchIndexes.Store(p.Id, uint64(0))
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
	r.states = map[string]*replState{}
	w.Wait()
	r.server.logger.Infow("all replications stopped", logFields(r.server)...)
}
