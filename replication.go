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
	r             *replScheduler
	peer          *pb.Peer
	configuration *Configuration

	nextIndex uint64

	ctlMu   sync.Mutex // protects ctl and stopped
	ctl     *replCtl
	stopped bool
}

func (s *replState) replicate(ctl *replCtl, stepdownCh serverStepdownChan) {
	defer ctl.Release()
	goto ENTRY

NEXT_MOVE_FORWARD:
	{
		nextIndex := s.nextIndex - 1
		if nextIndex < s.r.server.firstLogIndex() {
			nextIndex = s.r.server.firstLogIndex()
		}
	}

RESET_LOOP:
	select {
	case <-ctl.Cancelled():
		return
	case <-s.r.server.randomTimer(s.r.server.opts.followerTimeout / 10).C:
		goto CHECK_INDEX
	}

ENTRY:
	s.r.server.logger.Infow("replication/heartbeat started",
		logFields(s.r.server,
			zap.String("replication_id", ctl.replId),
			zap.Object("peer", s.peer))...)
	defer s.r.server.logger.Infow("replication/heartbeat stopped",
		logFields(s.r.server,
			zap.String("replication_id", ctl.replId),
			zap.Object("peer", s.peer))...)

	if s.peer.Id == s.r.server.id {
		// If the target peer is ourself, perform a "fake" replication to ourself.
	SELF_CHECK_INDEX:
		select {
		case <-ctl.Cancelled():
			return
		default:
		}

		lastLogIndex := s.r.server.lastLogIndex()
		// Check if there are more entries to replicate.
		matchIndex, ok := s.r.matchIndexes.Load(s.peer.Id)
		if !ok {
			s.r.server.logger.Panicw(
				"confusing condition: missing an entry in matchIndexes",
				logFields(s.r.server, "missing_server_id", s.peer.Id)...,
			)
		}
		if lastLogIndex <= matchIndex.(uint64) {
			select {
			case <-ctl.Cancelled():
				return
			case <-s.r.server.randomTimer(s.r.server.opts.followerTimeout / 10).C:
				goto SELF_CHECK_INDEX
			}
		}

		select {
		case <-ctl.Cancelled():
			return
		default:
		}

		s.nextIndex = lastLogIndex + 1
		s.r.setMatchIndex(s.peer.Id, lastLogIndex)

		s.r.server.logger.Infow("self replication state updated",
			logFields(s.r.server,
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer))...)

		select {
		case <-ctl.Cancelled():
			return
		case <-s.r.server.randomTimer(s.r.server.opts.followerTimeout / 10).C:
			goto SELF_CHECK_INDEX
		}
	}

CHECK_INDEX:
	select {
	case <-ctl.Cancelled():
		return
	default:
	}

	lastLogIndex := s.r.server.lastLogIndex()
	// Check if there are more entries to replicate.
	if lastLogIndex >= s.nextIndex {
		goto REPLICATE
	}
	s.r.server.logger.Infow("no entries to replicate",
		logFields(s.r.server,
			zap.String("replication_id", ctl.replId),
			zap.Object("peer", s.peer))...)

	// HEARTBEAT
	{
		select {
		case <-ctl.Cancelled():
			return
		default:
		}

		heartbeatRequestId, heartbeaRequest := s.r.prepareHeartbeat()
		s.r.server.logger.Debugw("send heartbeat request",
			logFields(s.r.server,
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer),
				zap.String("request_id", heartbeatRequestId),
				zap.Reflect("request", heartbeaRequest))...)
		heartbeatResponse, err := s.r.server.trans.AppendEntries(ctl.Context(), s.peer, heartbeaRequest)
		if err != nil {
			s.r.server.logger.Infow("error sending heartbeat request",
				logFields(s.r.server,
					zap.Error(err),
					zap.String("replication_id", ctl.replId),
					zap.Object("peer", s.peer),
					zap.String("request_id", heartbeatRequestId),
					zap.Reflect("request", heartbeaRequest))...)
			goto RESET_LOOP
		}

		if heartbeatResponse.Term > heartbeaRequest.Term {
			// Local term is stale
			stepdownCh <- heartbeatResponse.Term
			return
		}
	}
	goto RESET_LOOP

REPLICATE:
	{
		select {
		case <-ctl.Cancelled():
			return
		default:
		}

		replicationRequestId, replicationRequest, err := s.r.prepareRequest(s.nextIndex, lastLogIndex)
		if err != nil {
			s.r.server.logger.Infow("error preparing replication request",
				logFields(s.r.server,
					zap.Error(err),
					zap.String("replication_id", ctl.replId),
					zap.Object("peer", s.peer),
					zap.String("request_id", replicationRequestId),
					zap.Reflect("request", replicationRequest))...)
			goto RESET_LOOP
		}
		s.r.server.logger.Debugw("send replication request",
			logFields(s.r.server,
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer),
				zap.String("request_id", replicationRequestId),
				zap.Reflect("request", replicationRequest))...)
		replicationResponse, err := s.r.server.trans.AppendEntries(ctl.Context(), s.peer, replicationRequest)
		if err != nil {
			s.r.server.logger.Infow("error sending replication request",
				logFields(s.r.server,
					zap.Error(err),
					zap.String("replication_id", ctl.replId),
					zap.Object("peer", s.peer),
					zap.String("request_id", replicationRequestId),
					zap.Reflect("request", replicationRequest))...)
			goto RESET_LOOP
		}

		if replicationResponse.Term > replicationRequest.Term {
			// Local term is stale
			stepdownCh <- replicationResponse.Term
			return
		}

		switch replicationResponse.Status {
		case pb.ReplStatus_REPL_OK:
			s.r.server.logger.Debugw("successful replication response",
				logFields(s.r.server,
					zap.String("replication_id", ctl.replId),
					zap.Object("peer", s.peer),
					zap.String("request_id", replicationRequestId),
					zap.Reflect("response", replicationResponse))...)
			s.nextIndex = lastLogIndex + 1
			s.r.setMatchIndex(s.peer.Id, lastLogIndex)
			goto RESET_LOOP
		case pb.ReplStatus_REPL_ERR_NO_LOG:
			// If snapshot is disabled:
			// s.nextIndex = s.nextIndex - 1
			// Or, we should consider installing snapshots
			s.r.server.logger.Debugw("unsuccessful replication repsonse: no log",
				logFields(s.r.server,
					zap.String("replication_id", ctl.replId),
					zap.Object("peer", s.peer),
					zap.String("request_id", replicationRequestId),
					zap.Reflect("response", replicationResponse))...)
		default:
			// We have nothing to do here
			s.r.server.logger.Debugw("unsuccessful replication repsonse",
				logFields(s.r.server,
					zap.String("replication_id", ctl.replId),
					zap.Object("peer", s.peer),
					zap.String("request_id", replicationRequestId),
					zap.Reflect("response", replicationResponse))...)
			goto RESET_LOOP
		}
	}

	// TRY & INSTALL SNAPSHOT
	{
		// Check if we have snapshots available
		metadataList, err := s.r.server.snapshotProvider.List()
		if err != nil {
			s.r.server.logger.Infow("failed listing snapshots",
				logFields(s.r.server,
					zap.Error(err),
					zap.String("replication_id", ctl.replId),
					zap.Object("peer", s.peer))...)
			goto NEXT_MOVE_FORWARD
		}
		if len(metadataList) == 0 {
			s.r.server.logger.Infow("no snapshots",
				logFields(s.r.server,
					zap.Error(err),
					zap.String("replication_id", ctl.replId),
					zap.Object("peer", s.peer))...)
			goto NEXT_MOVE_FORWARD
		}

		if metadataList[0].Index() <= s.r.matchIndex(s.peer.Id) {
			// Installing this snapshot is meaningless since the peer has more
			// logs than the snapshot.
			s.r.server.logger.Infow("no eliible snapshots",
				logFields(s.r.server,
					zap.Error(err),
					zap.String("replication_id", ctl.replId),
					zap.Object("peer", s.peer))...)
			goto NEXT_MOVE_FORWARD
		}

		snapshot, err := s.r.server.snapshotProvider.Open(metadataList[0].Id())
		if err != nil {
			s.r.server.logger.Infow("failed opening the latest snapshot",
				logFields(s.r.server,
					zap.Error(err),
					zap.String("replication_id", ctl.replId),
					zap.Object("peer", s.peer))...)
			goto NEXT_MOVE_FORWARD
		}

		select {
		case <-ctl.Cancelled():
			snapshot.Close()
			return
		default:
		}

		// Install snapshot
		s.r.server.logger.Infow("ready to install snapshot",
			logFields(s.r.server,
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer),
				zap.Reflect("snapshot_meta", snapshot.Meta))...)

		snapshotMeta, err := snapshot.Meta()
		if err != nil {
			s.r.server.logger.Infow("error getting snapshot metadata",
				logFields(s.r.server,
					zap.Error(err),
					zap.String("replication_id", ctl.replId),
					zap.Object("peer", s.peer))...)
			snapshot.Close()
			goto NEXT_MOVE_FORWARD
		}

		snapshotMetaBytes, err := snapshotMeta.Encode()
		if err != nil {
			s.r.server.logger.Infow("error encoding snapshot metadata",
				logFields(s.r.server,
					zap.Error(err),
					zap.String("replication_id", ctl.replId),
					zap.Object("peer", s.peer),
					zap.String("snapshot_id", snapshotMeta.Id()))...)
			snapshot.Close()
			goto NEXT_MOVE_FORWARD
		}

		installSnapshotRequestMeta := &pb.InstallSnapshotRequestMeta{
			Term:              s.r.server.currentTerm(),
			LeaderId:          s.r.server.Leader().Id,
			LastIncludedIndex: snapshotMeta.Index(),
			LastIncludedTerm:  snapshotMeta.Term(),
			SnapshotMetadata:  snapshotMetaBytes,
		}

		snapshotReader, err := snapshot.Reader()
		if err != nil {
			s.r.server.logger.Infow("error getting snapshot reader",
				logFields(s.r.server,
					zap.Error(err),
					zap.String("replication_id", ctl.replId),
					zap.Object("peer", s.peer),
					zap.String("snapshot_id", snapshotMeta.Id()))...)
			snapshot.Close()
			goto NEXT_MOVE_FORWARD
		}

		installSnapshotResponse, err := s.r.server.trans.InstallSnapshot(
			ctl.Context(), s.peer, installSnapshotRequestMeta, snapshotReader,
		)
		if err != nil {
			s.r.server.logger.Infow("error installing snapshot",
				logFields(s.r.server,
					zap.Error(err),
					zap.String("replication_id", ctl.replId),
					zap.Object("peer", s.peer),
					zap.String("snapshot_id", snapshotMeta.Id()))...)
			snapshot.Close()
			goto NEXT_MOVE_FORWARD
		}
		snapshot.Close()

		if installSnapshotResponse.Term > installSnapshotRequestMeta.Term {
			stepdownCh <- installSnapshotResponse.Term
			return
		}

		s.r.server.logger.Infow("snapshot installed",
			logFields(s.r.server,
				zap.String("replication_id", ctl.replId),
				zap.Object("peer", s.peer),
				zap.String("snapshot_id", snapshotMeta.Id()))...)

		s.nextIndex = snapshotMeta.Index() + 1
		s.r.setMatchIndex(s.peer.Id, snapshotMeta.Index())

		goto RESET_LOOP
	}
}

func (s *replState) Replicate(replID string, stepdownCh serverStepdownChan) {
	s.ctlMu.Lock()
	defer s.ctlMu.Unlock()

	if s.stopped {
		s.r.server.logger.Panic("attempt to reuse a stopped replState")
	}

	newCtl := &replCtl{asyncCtl: newAsyncCtl(), replId: replID}
	oldCtl := s.ctl
	s.ctl = newCtl
	if oldCtl != nil {
		oldCtl.Cancel()
		<-oldCtl.WaitRelease()
	}
	go s.replicate(newCtl, stepdownCh)
}

func (s *replState) Stop() {
	s.ctlMu.Lock()
	defer s.ctlMu.Unlock()

	if s.stopped {
		s.r.server.logger.Panic("attempt to stop a stopped replState")
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
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*pb.Log{},
	}
}

func (r *replScheduler) prepareRequest(firstIndex, lastIndex uint64) (string, *pb.AppendEntriesRequest, error) {
	r.server.logger.Infof("prepareRequest(%d, %d)", firstIndex, lastIndex)
	requestId := NewObjectID().Hex()

	request := &pb.AppendEntriesRequest{
		Term:         r.server.currentTerm(),
		LeaderId:     r.server.id,
		LeaderCommit: r.server.commitIndex(),
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*pb.Log{},
	}

	if prevLogIndex := firstIndex - 1; prevLogIndex > 0 {
		logMeta, err := r.server.logProvider.Meta(prevLogIndex)
		if err != nil {
			return "", nil, err
		}
		if logMeta != nil {
			request.PrevLogIndex = logMeta.Index
			request.PrevLogTerm = logMeta.Term
		}
	}

	lastLogIndex := r.server.lastLogIndex()
	if firstIndex > lastLogIndex || (firstIndex == lastLogIndex && firstIndex == 0) {
		return requestId, request, nil
	}

	request.Entries = make([]*pb.Log, 0, lastLogIndex-firstIndex+1)
	for i := firstIndex; i <= lastLogIndex; i++ {
		e, err := r.server.logProvider.Entry(i)
		if err != nil {
			return "", nil, err
		}
		request.Entries = append(request.Entries, e.Copy())
	}

	return requestId, request, nil
}

func (r *replScheduler) matchIndex(serverId string) uint64 {
	if v, _ := r.matchIndexes.Load(serverId); v != nil {
		return v.(uint64)
	}
	return 0
}

func (r *replScheduler) setMatchIndex(serverID string, matchIndex uint64) {
	c := r.server.confStore.Latest()
	r.matchIndexes.Store(serverID, matchIndex)
	r.server.logger.Infow("setMatchIndex", zap.Object("conf", c))
	r.server.updateCommitIndex(r.nextCommitIndexLocked(c))
}

func (r *replScheduler) nextCommitIndexLocked(c *Configuration) uint64 {
	matchIndexes := map[string]uint64{}
	r.matchIndexes.Range(func(key, value any) bool {
		matchIndexes[key.(string)] = value.(uint64)
		return true
	})

	r.server.logger.Infow("matchIndexes", "matchIndexes", matchIndexes)

	if !c.Joint() {
		currentIndexes := make([]uint64, 0, len(c.Current.Peers))
		for _, p := range c.Current.Peers {
			if index, ok := matchIndexes[p.Id]; ok {
				currentIndexes = append(currentIndexes, index)
			} else {
				r.server.logger.Panicw(
					"confusing condition: found a server ID that does not belong to current configuration",
					logFields(r.server, zap.String("orphan_server_id", p.Id))...,
				)
			}
		}
		sort.SliceStable(currentIndexes, func(i, j int) bool { return currentIndexes[i] > currentIndexes[j] })
		commitIndex := currentIndexes[c.CurrentConfig().Quorum()-1]
		r.server.logger.Infow("next commit index",
			logFields(r.server, zap.Uint64("next_commit_index", commitIndex))...)
		return commitIndex
	} else {
		currentIndexes := make([]uint64, 0, len(c.Current.Peers))
		nextIndexes := make([]uint64, 0, len(c.Next.Peers))
		for _, p := range c.Peers() {
			inCurrent, inNext := c.CurrentConfig().Contains(p.Id), c.NextConfig().Contains(p.Id)
			if !inCurrent && !inNext {
				r.server.logger.Panicw(
					"confusing condition: found a server ID that does not belong to both any configuration",
					logFields(r.server, zap.String("orphan_server_id", p.Id))...,
				)
			}
			if inCurrent {
				if index, ok := matchIndexes[p.Id]; ok {
					currentIndexes = append(currentIndexes, index)
				} else {
					r.server.logger.Panicw(
						"confusing condition: found a server ID that does not belong to current configuration",
						logFields(r.server, zap.String("orphan_server_id", p.Id))...,
					)
				}
			}
			if inNext {
				if index, ok := matchIndexes[p.Id]; ok {
					nextIndexes = append(nextIndexes, index)
				} else {
					r.server.logger.Panicw(
						"confusing condition: found a server ID that does not belong to next configuration",
						logFields(r.server, zap.String("orphan_server_id", p.Id))...,
					)
				}
			}
		}
		r.server.logger.Info("currentIndexes", currentIndexes)
		r.server.logger.Info("nextIndexes", nextIndexes)
		sort.SliceStable(currentIndexes, func(i, j int) bool { return currentIndexes[i] > currentIndexes[j] })
		sort.SliceStable(nextIndexes, func(i, j int) bool { return nextIndexes[i] > nextIndexes[j] })
		commitIndex := currentIndexes[c.CurrentConfig().Quorum()-1]
		if index := nextIndexes[c.NextConfig().Quorum()-1]; index < commitIndex {
			commitIndex = index
		}
		r.server.logger.Infow("next commit index",
			logFields(r.server, zap.Uint64("next_commit_index", commitIndex))...)
		return commitIndex
	}
}

func (r *replScheduler) Start(stepdownCh serverStepdownChan) {
	c := r.server.confStore.Latest()

	replId := NewObjectID().Hex()
	r.server.logger.Infow("replication/heartbeat scheduled",
		logFields(r.server, "replication_id", replId)...)

	r.statesMu.Lock()
	r.states = map[string]*replState{}
	for _, p := range c.Peers() {
		if p.Id == r.server.id {
			r.states[p.Id] = &replState{
				r:             r,
				peer:          p,
				configuration: c,
				nextIndex:     r.server.lastLogIndex() + 1,
			}
			r.matchIndexes.Store(p.Id, r.server.lastLogIndex())
		} else {
			r.states[p.Id] = &replState{
				r:             r,
				peer:          p,
				configuration: c,
				nextIndex:     r.server.lastLogIndex(), // To start replication to non-self peers immediately
			}
			r.matchIndexes.Store(p.Id, uint64(0))
		}
	}
	for _, s := range r.states {
		s.Replicate(replId, stepdownCh)
	}
	r.statesMu.Unlock()
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
