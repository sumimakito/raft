package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sumimakito/raft/pb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type ServerInfo struct {
	ID       string `json:"id"`
	Endpoint string `json:"endpoint"`
}

type ServerStates struct {
	ID                string   `json:"id"`
	Endpoint          string   `json:"endpoint"`
	Leader            *pb.Peer `json:"leader"`
	Role              string   `json:"role"`
	CurrentTerm       uint64   `json:"current_term"`
	LastLogIndex      uint64   `json:"last_log_index"`
	LastVoteTerm      uint64   `json:"last_vote_term"`
	LastVoteCandidate string   `json:"last_vote_candidate"`
	CommitIndex       uint64   `json:"commit_index"`
}

type ServerCoreOptions struct {
	Id             string
	InitialCluster []*pb.Peer
	StableStore    StableStore
	StateMachine   StateMachine
	SnapshotStore  SnapshatStore
	Transport      Transport
}

type serverStepdownChan chan uint64

type serverChannels struct {
	noCopy

	// commitCh receives updates on the commit index.
	commitCh chan uint64

	logOpsCh     chan logStoreOp
	logRestoreCh chan FutureTask[any, SnapshotMeta]

	rpcCh chan *RPC

	serveErrCh chan error
	shutdownCh chan error

	snapshotRestoreCh chan FutureTask[bool, string]

	// stateMachineSnapshotCh is used to trigger a snapshot on the state machine.
	stateMachineSnapshotCh chan FutureTask[*stateMachineSnapshot, any]
}

type Server struct {
	id             string
	initialCluster []*pb.Peer
	opts           *serverOptions
	serveFlag      uint32
	logger         *zap.SugaredLogger

	clusterLeader atomic.Value // *Peer

	serverState
	commitState

	serverChannels

	stableStore     StableStore
	confStore       *configurationStore
	stateMachine    *stateMachineProxy
	rpcHandler      *rpcHandler
	replScheduler   *replScheduler
	snapshotService *snapshotService

	apiServer *apiServer

	logStore      *logStoreProxy
	snapshotStore SnapshatStore
	trans         Transport

	// flagReselectLoop is a flag used by current loop to exit and re-select a loop to enter.
	flagReselectLoop uint32

	shutdownOnce sync.Once
}

func NewServer(coreOpts ServerCoreOptions, opts ...ServerOption) (*Server, error) {
	var initialCluster []*pb.Peer
	if coreOpts.InitialCluster != nil {
		initialCluster = make([]*pb.Peer, 0, len(coreOpts.InitialCluster))
		for _, p := range coreOpts.InitialCluster {
			initialCluster = append(initialCluster, p.Copy())
		}
	}

	server := &Server{
		id:             coreOpts.Id,
		initialCluster: initialCluster,
		serverState:    serverState{stateRole: Follower},
		commitState:    commitState{},
		serverChannels: serverChannels{
			commitCh:               make(chan uint64, 16),
			logOpsCh:               make(chan logStoreOp, 64),
			logRestoreCh:           make(chan FutureTask[any, SnapshotMeta], 64),
			rpcCh:                  make(chan *RPC, 16),
			serveErrCh:             make(chan error, 8),
			shutdownCh:             make(chan error, 8),
			snapshotRestoreCh:      make(chan FutureTask[bool, string], 8),
			stateMachineSnapshotCh: make(chan FutureTask[*stateMachineSnapshot, any], 16),
		},
		stableStore:   coreOpts.StableStore,
		trans:         coreOpts.Transport,
		snapshotStore: coreOpts.SnapshotStore,
		opts:          applyServerOpts(opts...),
	}

	// Set up the logger
	server.logger = serverLogger(server.opts.logLevel)

	// Set up the LogStore
	server.logStore = newLogStoreProxy(server, server.stableStore)
	if err := server.restoreStates(); err != nil {
		return nil, err
	}

	server.apiServer = newAPIServer(server, server.opts.apiExtensions...)
	// Recover the configurationStore using the LogStore.
	if confStore, err := newConfigurationStore(server); err != nil {
		return nil, err
	} else {
		server.confStore = confStore
	}
	server.replScheduler = newReplScheduler(server)
	server.snapshotService = newSnapshotService(server)
	server.rpcHandler = newRPCHandler(server)
	server.stateMachine = newStateMachineProxy(server, coreOpts.StateMachine)

	// Restore using the latest snapshot (if any).
	snapshotMetaList, err := server.snapshotStore.List()
	if err != nil {
		return nil, err
	}
	if len(snapshotMetaList) > 0 {
		snapshotMeta := snapshotMetaList[0]
		ok, err := server.snapshotService.Restore(snapshotMeta.Id())
		if err != nil {
			return nil, err
		}
		if ok {
			server.logger.Infow("restored from snapshot",
				logFields(server, zap.Any("snapshot_meta", snapshotMeta))...)
		} else {
			server.logger.Infow("snapshot exists but not used for restoration",
				logFields(server, zap.Any("snapshot_meta", snapshotMeta))...)
		}
	}

	conf := server.confStore.Latest()

	if len(conf.Peers()) > 0 {
		// Restore cluster from saved configuration.
		selfRegistered := false
		for _, peer := range conf.Peers() {
			if server.id == peer.Id {
				// Check for an edge condition
				if server.Endpoint() != peer.Endpoint {
					server.logger.Panicw("confusing condition: two servers have the same ID but different endpoints",
						logFields(server)...)
				}
				break
			}
		}
		if !selfRegistered {
			server.logger.Warnw("the server is not in the latest configuration's peer list", logFields(server)...)
		}
	} else {
		// The latest configuration does not contain any peers.
		// The server should be the first node in the cluster.
		pbConfiguration := &pb.Configuration{
			Current: &pb.Config{Peers: server.initialCluster},
		}
		configurationBytes, err := proto.Marshal(pbConfiguration)
		if err != nil {
			return nil, err
		}
		pbLogBody := &pb.LogBody{Type: pb.LogType_CONFIGURATION, Data: configurationBytes}
		if _, err := server.appendLogs([]*pb.LogBody{pbLogBody}); err != nil {
			server.logger.Panicw("error occurred bootstrapping configuration for ourself",
				logFields(server, zap.Error(err))...)
		}
	}

	return server, nil
}

func (s *Server) alterCommitIndex(commitIndex uint64) {
	s.commitCh <- commitIndex
}

// alterConfiguration changes the latest configuration the server uses.
// Loop re-selection will be marked as needed after calling alterConfiguration().
func (s *Server) alterConfiguration(c *configuration) {
	s.confStore.SetLatest(c)
	s.reselectLoop()
	s.logger.Infow("configuration has been updated", logFields(s, zap.Reflect("configuration", c))...)
}

func (s *Server) alterLeader(leader *pb.Peer) {
	s.logger.Infow("alter leader", logFields(s, zap.Reflect("new_leader", leader))...)
	s.setLeader(leader)
}

func (s *Server) alterRole(role ServerRole) {
	s.logger.Infow("alter role", logFields(s, "new_role", role.String())...)
	s.setRole(role)
}

func (s *Server) alterTerm(term uint64) {
	s.logger.Infow("alter term", logFields(s, "new_term", term)...)
	s.setCurrentTerm(term)
}

// stepdownFollower converts the server into a follower
func (s *Server) stepdownFollower(leader *pb.Peer) {
	if s.role() < Follower {
		s.logger.Panicw("stepdownFollower() requires the server to have a role which is higher than follower",
			logFields(s)...)
	}
	s.setLeader(leader)
	s.setRole(Follower)
}

// appendLogs submits the logs to the LogStore and updates the index states.
// NOT safe for concurrent use.
// Should be used by non-leader servers.
func (s *Server) appendLogs(bodies []*pb.LogBody) ([]*pb.LogMeta, error) {
	lastLogIndex := s.lastLogIndex()
	term := s.currentTerm()
	logs := make([]*pb.Log, len(bodies))
	logMeta := make([]*pb.LogMeta, len(bodies))
	lastConfArrayIndex := len(logs)

	for i, body := range bodies {
		log := &pb.Log{
			Meta: &pb.LogMeta{
				Index: lastLogIndex + 1 + uint64(i),
				Term:  term,
			},
			Body: body.Copy(),
		}
		logs[i] = log
		logMeta[i] = log.Meta
		if logs[i].Body.Type == pb.LogType_CONFIGURATION {
			lastConfArrayIndex = i
		}
	}

	var conf *configuration
	if lastConfArrayIndex < len(logs) {
		log := logs[lastConfArrayIndex]
		var pbConfiguration pb.Configuration
		if err := proto.Unmarshal(log.Body.Data, &pbConfiguration); err != nil {
			// Errors here are not fatal
			return nil, err
		}
		conf = newConfiguration(&pbConfiguration, log.Meta.Index)
	}

	if err := s.logStore.AppendLogs(logs); err != nil {
		return nil, err
	}

	// Logs have been appended now.
	// Failure to update the index will cause a panic.
	s.setFirstLogIndex(Must2(s.logStore.FirstIndex()))
	s.setLastLogIndex(Must2(s.logStore.LastIndex()))

	// Special process is necessary if configuration logs are discovered.
	if conf != nil {
		// Stop the replication ...
		s.replScheduler.Stop()
		// And alter the configuration
		s.alterConfiguration(conf)
	}
	return logMeta, nil
}

func (s *Server) commitAndApply(commitIndex uint64) {
	s.logger.Infow("ready to update commit index", logFields(s, "new_commit_index", commitIndex)...)
	if commitIndex < s.commitIndex() {
		return
	}
	if commitIndex > s.lastLogIndex() {
		// Commit index should never overflow the log index.
		commitIndex = s.lastLogIndex()
	}
	lastApplied := s.lastApplied()
	if lastApplied.Index == commitIndex {
		s.logger.Debugw("lastAppliedIndex == commitIndex, there's nothing to apply", logFields(s)...)
		return
	}
	if lastApplied.Index > commitIndex {
		s.logger.Panicw("confusing condition: lastAppliedIndex > commitIndex", logFields(s)...)
	}
	s.setCommitIndex(commitIndex)
	firstIndex := lastApplied.Index + 1
	s.logger.Infow("ready to apply logs", logFields(s, "first_index", firstIndex, "last_index", commitIndex)...)
	var commitTerm uint64
	var lastConfigurationLog *pb.Log
	for i := firstIndex; i <= commitIndex; i++ {
		if s.logStore.withinSnapshot(i) {
			// Skip the log entry if its index is compacted by the snapshot.
			commitTerm = s.logStore.snapshotMeta.Term()
			continue
		}
		log := Must2(s.logStore.Entry(i))
		if log == nil {
			// We've found one or more gaps in the logs
			s.logger.Panicw("one or more log gaps are detected", logFields(s, "missing_index", i)...)
		}
		if i == commitIndex {
			commitTerm = log.Meta.Term
		}
		switch log.Body.Type {
		case pb.LogType_COMMAND:
			s.stateMachine.Apply(log.Body.Data)
		case pb.LogType_CONFIGURATION:
			lastConfigurationLog = log
		}
	}
	if log := lastConfigurationLog; log != nil {
		var pbConfiguration pb.Configuration
		proto.Unmarshal(log.Body.Data, &pbConfiguration)
		s.confStore.SetCommitted(newConfiguration(&pbConfiguration, log.Meta.Index))
		s.commitConfiguration(log.Meta.Index)
	}
	s.setLastApplied(commitIndex, commitTerm)
	s.logger.Infow("logs has been applied", logFields(s, "first_index", firstIndex, "last_index", commitIndex)...)
}

// commitConfiguration is used when a configuration log has been committed.
// Unsafe for concurrent use.
func (s *Server) commitConfiguration(index uint64) {
	if s.role() != Leader {
		// Configuration commitment has nothing to do with non-leader servers.
		return
	}
	latest := s.confStore.Latest()
	if !latest.Joint() {
		// The latest configuration is not a joint configuration.
		return
	}
	if latest.LogIndex() != index {
		// The latest configuration is yet to be committed.
		// We will skip this.
		// The uncommitted joint configuration should always be the last configuration.
		return
	}
	// A joint configuration (and the latest configuration) has been committed.
	Must1(s.confStore.commitTransition())
}

func (s *Server) handleRPC(rpc *RPC) {
	switch request := rpc.Request().(type) {
	case *pb.AppendEntriesRequest:
		rpc.Respond(s.rpcHandler.AppendEntries(rpc.Context(), rpc.requestID, request))
	case *pb.RequestVoteRequest:
		rpc.Respond(s.rpcHandler.RequestVote(rpc.Context(), rpc.requestID, request))
	case *InstallSnapshotRequest:
		rpc.Respond(s.rpcHandler.InstallSnapshot(rpc.Context(), rpc.requestID, request))
		if _, err := rpc.Response(); err != nil {
			panic(err)
		}
	case *pb.ApplyLogRequest:
		rpc.Respond(s.rpcHandler.ApplyLog(rpc.Context(), rpc.requestID, request))
	default:
		s.logger.Warnw("incoming RPC is unrecognized", logFields(s, "request", rpc.Request)...)
	}
}

func (s *Server) handleTerminal() {
	sig := <-terminalSignalCh()
	s.shutdownCh <- nil
	s.logger.Infow("terminal signal captured", logFields(s, "signal", sig)...)
}

func (s *Server) internalShutdown(err error) {
	if !s.setShutdownState() {
		return
	}
	s.logger.Infow("ready to shutdown", logFields(s, zap.Error(err))...)
	if err := s.apiServer.Stop(); err != nil {
		s.logger.Warnw("error occurred stopping the API server", logFields(s, zap.Error(err))...)
	}
	s.snapshotService.Stop()
	// Close the Transport
	if t, ok := s.trans.(TransportCloser); ok {
		if err := t.Close(); err != nil {
			s.logger.Infow(fmt.Sprintf("error occurred closing the Transport: %v", err), logFields(s)...)
		}
	}
	_ = s.logger.Sync()
	// Send err (if any) to the serve error channel
	s.serveErrCh <- err
}

func (s *Server) randomTimer(timeout time.Duration) *time.Timer {
	randomOffset := rand.Int63n(int64(s.opts.maxTimerRandomOffsetRatio*float64(timeout)) + 1)
	return time.NewTimer(timeout + time.Duration(randomOffset))
}

func (s *Server) reselectLoop() {
	atomic.StoreUint32(&s.flagReselectLoop, 1)
}

func (s *Server) resetReselectLoop() {
	atomic.StoreUint32(&s.flagReselectLoop, 0)
}

func (s *Server) shouldReselectLoop() bool {
	return atomic.LoadUint32(&s.flagReselectLoop) != 0
}

func (s *Server) runMainLoop() {
	for !s.shutdownState() {
		s.resetReselectLoop()
		switch s.role() {
		case Leader:
			s.runLoopLeader()
		case Candidate:
			s.runLoopCandidate()
		case Follower:
			s.runLoopFollower()
		}
	}
}

func (s *Server) runLoopLeader() {
	s.logger.Infow("run leader loop", logFields(s)...)

	// stepdownCh is used when the local term is found stale.
	stepdownCh := make(chan uint64, 1)

	s.snapshotService.StartScheduler()
	defer s.snapshotService.StopScheduler()

	s.replScheduler.Start(stepdownCh)
	defer s.replScheduler.Stop()

	for s.role() == Leader {
		select {
		case commitIndex := <-s.commitCh:
			s.commitAndApply(commitIndex)
		case t := <-s.logOpsCh:
			switch op := t.(type) {
			case *logStoreAppendOp:
				op.setResult(s.appendLogs(op.Task()))
			case *logStoreTrimOp:
				switch op.Type {
				case logStoreTrimPrefix:
					op.setResult(nil, s.logStore.TrimPrefix(op.Task()))
				case logStoreTrimSuffix:
					op.setResult(nil, s.logStore.TrimSuffix(op.Task()))
				default:
					s.logger.Warnw("unknown type in logStoreTrimOp", logFields(s)...)
				}
			default:
				s.logger.Warnw("unknown logStoreOp", logFields(s)...)
			}
		case t := <-s.logRestoreCh:
			t.setResult(nil, s.logStore.Restore(t.Task()))
		case rpc := <-s.trans.RPC():
			go s.handleRPC(rpc)
		case err := <-s.shutdownCh:
			s.internalShutdown(err)
			return
		case t := <-s.stateMachineSnapshotCh:
			t.setResult(s.stateMachine.Snapshot())
		case term := <-stepdownCh:
			// We'll update the leader in other loops
			s.stepdownFollower(pb.NilPeer)
			s.alterTerm(term)
			return
		case t := <-s.snapshotRestoreCh:
			s.replScheduler.Stop()
			t.setResult(s.snapshotService.Restore(t.Task()))
		}
		if s.shouldReselectLoop() {
			return
		}
	}
}

func (s *Server) runLoopCandidate() {
	s.logger.Infow("run candidate loop", logFields(s)...)

	c := s.confStore.Latest()

	if _, ok := c.Peer(s.id); !ok {
		// We're not in the latest configuration.
		// 1) A newly joined server is catching up with the leader.
		// 2) The server is removed from the cluster.
		s.logger.Infow("stay as a follower since current configuration does not include ourself",
			logFields(s)...)
		s.alterRole(Follower)
		s.reselectLoop()
		return
	}

	electionTimer := s.randomTimer(s.opts.electionTimeout)
	voteResCh, voteCancel, err := s.startElection()
	defer voteCancel()
	if err != nil {
		s.logger.Panicw("error occurred starting the election", logFields(s, zap.Error(err))...)
	}

	currentVotes := 0
	nextVotes := 0

	for s.role() == Candidate {
		select {
		case response := <-voteResCh:
			if response.Term > s.currentTerm() {
				voteCancel()
				s.logger.Infow("local term is stale", logFields(s)...)
				s.alterTerm(response.Term)
				return
			}
			if c.CurrentConfig().Contains(response.ServerId) {
				currentVotes++
			}
			if c.Joint() && c.NextConfig().Contains(response.ServerId) {
				nextVotes++
			}
			if !c.Joint() {
				if currentVotes >= c.CurrentConfig().Quorum() {
					voteCancel()
					s.logger.Infow("won the election", logFields(s)...)
					s.alterRole(Leader)
					leaderPeer, _ := s.confStore.Latest().Peer(s.id)
					s.alterLeader(leaderPeer)
					return
				}
			} else {
				if currentVotes >= c.CurrentConfig().Quorum() && nextVotes >= c.NextConfig().Quorum() {
					voteCancel()
					s.logger.Infow("won the election", logFields(s)...)
					s.alterRole(Leader)
					leaderPeer, _ := s.confStore.Latest().Peer(s.id)
					s.alterLeader(leaderPeer)
					return
				}
			}
		case <-electionTimer.C:
			s.logger.Infow("timed out in Candidate loop", logFields(s)...)
			voteCancel()
			return
		case commitIndex := <-s.commitCh:
			s.commitAndApply(commitIndex)
		case t := <-s.logRestoreCh:
			t.setResult(nil, s.logStore.Restore(t.Task()))
		case rpc := <-s.trans.RPC():
			go s.handleRPC(rpc)
		case err := <-s.shutdownCh:
			voteCancel()
			s.internalShutdown(err)
			return
		case t := <-s.snapshotRestoreCh:
			t.setResult(s.snapshotService.Restore(t.Task()))
		}
		if s.shouldReselectLoop() {
			return
		}
	}
}

func (s *Server) runLoopFollower() {
	s.logger.Infow("run follower loop", logFields(s)...)
	followerTimer := s.randomTimer(s.opts.followerTimeout)

	s.snapshotService.StartScheduler()
	defer s.snapshotService.StopScheduler()

	for s.role() == Follower {
		select {
		case <-followerTimer.C:
			s.logger.Infow("follower timed out", logFields(s)...)
			s.alterRole(Candidate)
			s.reselectLoop()
		case commitIndex := <-s.commitCh:
			s.commitAndApply(commitIndex)
		case t := <-s.logOpsCh:
			switch op := t.(type) {
			case *logStoreAppendOp:
				op.setResult(s.appendLogs(op.Task()))
			case *logStoreTrimOp:
				switch op.Type {
				case logStoreTrimPrefix:
					op.setResult(nil, s.logStore.TrimPrefix(op.Task()))
				case logStoreTrimSuffix:
					op.setResult(nil, s.logStore.TrimSuffix(op.Task()))
				default:
					s.logger.Warnw("unknown type in logStoreTrimOp", logFields(s)...)
				}
			default:
				s.logger.Warnw("unknown logStoreOp", logFields(s)...)
			}
		case t := <-s.logRestoreCh:
			t.setResult(nil, s.logStore.Restore(t.Task()))
		case rpc := <-s.trans.RPC():
			followerTimer.Reset(s.opts.followerTimeout)
			go s.handleRPC(rpc)
		case err := <-s.shutdownCh:
			s.internalShutdown(err)
			return
		case t := <-s.stateMachineSnapshotCh:
			t.setResult(s.stateMachine.Snapshot())
		case t := <-s.snapshotRestoreCh:
			t.setResult(s.snapshotService.Restore(t.Task()))
		}
		if s.shouldReselectLoop() {
			return
		}
	}
}

func (s *Server) serveAPIServer() {
	rand.Seed(time.Now().UnixNano())
	bindAddress := s.opts.apiServerListenAddress
	if bindAddress == "" {
		bindAddress = fmt.Sprintf("0.0.0.0:%d", 20000+rand.Intn(25001))
	}
	listener, err := net.Listen("tcp", bindAddress)
	if err != nil {
		s.logger.Warn(err)
	}
	if err := s.apiServer.Serve(listener); err != nil && err != http.ErrServerClosed {
		s.logger.Warn(err)
	}
}

func (s *Server) startElection() (<-chan *pb.RequestVoteResponse, context.CancelFunc, error) {
	s.logger.Infow("ready to start the election", logFields(s)...)
	s.alterTerm(s.currentTerm() + 1)
	s.setLastVoteSummary(s.currentTerm(), s.id)
	s.logger.Infow("election started", logFields(s)...)

	voteCtx, voteCancel := context.WithCancel(context.Background())

	c := s.confStore.Latest()
	resCh := make(chan *pb.RequestVoteResponse, len(c.Peers()))

	var lastIndex uint64
	var lastTerm uint64

	log, err := s.logStore.LastEntry(0)
	if err != nil {
		voteCancel()
		return nil, nil, err
	}
	if log != nil {
		lastIndex = log.Meta.Index
		lastTerm = log.Meta.Term
	}

	request := &pb.RequestVoteRequest{
		Term:         s.currentTerm(),
		CandidateId:  s.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	requestVote := func(peer *pb.Peer) {
		if response, err := s.trans.RequestVote(voteCtx, peer, request); err != nil {
			s.logger.Debugw("error requesting vote", logFields(s, "error", err)...)
		} else {
			resCh <- response
		}
	}

	for _, peer := range c.Peers() {
		// Do not ask ourself to vote
		if peer.Id == s.id {
			continue
		}
		go requestVote(peer)
	}

	resCh <- &pb.RequestVoteResponse{ServerId: s.id, Term: s.currentTerm(), Granted: true}

	return resCh, voteCancel, nil
}

func (s *Server) startMetrics(exporter MetricsExporter) {

}

// Apply.
// Future(LogMeta, error)
func (s *Server) Apply(ctx context.Context, body *pb.LogBody) FutureTask[*pb.LogMeta, *pb.LogBody] {
	t := newFutureTask[*pb.LogMeta](body.Copy())
	if s.role() == Leader {
		// Leader path
		internalTask := newFutureTask[[]*pb.LogMeta]([]*pb.LogBody{body.Copy()})
		appendOp := &logStoreAppendOp{FutureTask: internalTask}
		select {
		case s.logOpsCh <- appendOp:
		case <-ctx.Done():
			internalTask.setResult(nil, ErrDeadlineExceeded)
		}
		if logMeta, err := internalTask.Result(); err != nil {
			t.setResult(nil, err)
		} else {
			t.setResult(logMeta[0], nil)
		}
		return t
	}

	// Proxy path
	go func() {
		// Redirect requests to the leader on non-leader servers.
		response, err := s.trans.ApplyLog(ctx, s.Leader(), &pb.ApplyLogRequest{Body: body.Copy()})
		if err != nil {
			t.setResult(nil, err)
		}
		// TODO: Crashes happen here sometimes.
		switch r := response.Response.(type) {
		case *pb.ApplyLogResponse_Meta:
			t.setResult(r.Meta, nil)
		case *pb.ApplyLogResponse_Error:
			t.setResult(nil, errors.New(r.Error))
		}
	}()

	return t
}

// ApplyCommand.
// Future(LogMeta, error)
func (s *Server) ApplyCommand(ctx context.Context, command Command) FutureTask[*pb.LogMeta, *pb.LogBody] {
	return s.Apply(ctx, &pb.LogBody{
		Type: pb.LogType_COMMAND,
		Data: command,
	})
}

func (s *Server) StateMachine() StateMachine {
	return s.stateMachine.StateMachine
}

func (s *Server) Id() string {
	return s.id
}

func (s *Server) Endpoint() string {
	return s.trans.Endpoint()
}

func (s *Server) Info() ServerInfo {
	return ServerInfo{
		ID:       s.id,
		Endpoint: s.Endpoint(),
	}
}

func (s *Server) Leader() *pb.Peer {
	if v := s.clusterLeader.Load(); v != nil && v != pb.NilPeer {
		return v.(*pb.Peer)
	}
	return pb.NilPeer
}

func (s *Server) setLeader(leader *pb.Peer) {
	if leader == nil {
		leader = pb.NilPeer
	}
	s.clusterLeader.Store(leader)
}

// Register is used to register a server to current cluster.
// ErrInJointConsensus is returned when the server is already in a joint consensus.
func (s *Server) Register(peer *pb.Peer) error {
	latest := s.confStore.Latest()
	next := latest.Current.Copy()
	next.Peers = append(next.Peers, peer)
	return s.confStore.initiateTransition(newConfig(next))
}

func (s *Server) Serve() error {
	if !atomic.CompareAndSwapUint32(&s.serveFlag, 0, 1) {
		return errors.New("Serve() can only be called once")
	}

	go s.handleTerminal()

	if s.opts.metricsExporter != nil {
		go s.startMetrics(s.opts.metricsExporter)
	}

	if t, ok := s.trans.(TransportServer); ok {
		go func() {
			if err := t.Serve(); err != nil {
				s.internalShutdown(err)
			}
		}()
	}

	go s.serveAPIServer()

	s.snapshotService.Start()
	go s.runMainLoop()

	return <-s.serveErrCh
}

func (s *Server) Shutdown(err error) {
	s.shutdownCh <- err
}

func (s *Server) States() ServerStates {
	lastVoteSummary := s.lastVoteSummary()
	return ServerStates{
		ID:                s.id,
		Endpoint:          s.Endpoint(),
		Leader:            s.Leader(),
		Role:              s.role().String(),
		CurrentTerm:       s.currentTerm(),
		LastLogIndex:      s.lastLogIndex(),
		LastVoteTerm:      lastVoteSummary.term,
		LastVoteCandidate: lastVoteSummary.candidate,
		CommitIndex:       s.commitIndex(),
	}
}
