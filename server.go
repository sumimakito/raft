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
	ID           string
	Log          LogStore
	StateMachine StateMachine
	Snapshot     SnapshotStore
	Transport    Transport
}

type serverChannels struct {
	noCopy
	bootstrapCh chan FutureTask[any, any]

	// confCh receives updates on the configuration.
	// Should be used only by the leader.
	confCh chan *Configuration

	rpcCh chan *RPC

	// applyLogCh receives log applying requests.
	// Non-leader servers should redirect this request to the leader.
	applyLogCh chan FutureTask[*pb.LogMeta, *pb.LogBody]

	// commitCh receives updates on the commit index.
	commitCh chan uint64

	snapshotCh chan FutureTask[any, any]

	shutdownCh chan error
	serveErrCh chan error
}

type Server struct {
	id        string
	opts      *serverOptions
	serveFlag uint32
	logger    *zap.SugaredLogger

	clusterLeader atomic.Value // *Peer

	stable *stableStore
	serverState
	commitState

	serverChannels

	confStore     configurationStore
	stateMachine  *stateMachineAdapter
	rpcHandler    *rpcHandler
	repl          *replScheduler
	snapshotSched *snapshotScheduler

	apiServer *apiServer

	logStore LogStore
	snapshot SnapshotStore
	trans    Transport

	shutdownOnce sync.Once
}

func NewServer(coreOpts ServerCoreOptions, opts ...ServerOption) *Server {
	server := &Server{
		id:          coreOpts.ID,
		serverState: serverState{stateRole: Follower},
		commitState: commitState{},
		serverChannels: serverChannels{
			bootstrapCh: make(chan FutureTask[any, any], 1),
			confCh:      make(chan *Configuration, 16),
			rpcCh:       make(chan *RPC, 16),
			applyLogCh:  make(chan FutureTask[*pb.LogMeta, *pb.LogBody], 16),
			commitCh:    make(chan uint64, 16),
			snapshotCh:  make(chan FutureTask[any, any], 16),
			shutdownCh:  make(chan error, 8),
			serveErrCh:  make(chan error, 8),
		},
		logStore: coreOpts.Log,
		trans:    coreOpts.Transport,
		snapshot: coreOpts.Snapshot,
		opts:     applyServerOpts(opts...),
	}
	// Set up the logger
	server.logger = wrappedServerLogger(server.opts.logLevel)
	go func() { <-terminalSignalCh(); _ = server.logger.Sync() }()

	server.stable = newStableStore(server)
	server.restoreStates()

	server.apiServer = newAPIServer(server, server.opts.apiExtensions...)
	server.confStore = newConfigurationStore(server)
	server.repl = newReplScheduler(server)
	server.snapshotSched = newSnapshotScheduler(server)
	server.rpcHandler = newRPCHandler(server)
	server.stateMachine = newStateMachineAdapter(server, coreOpts.StateMachine)

	return server
}

func (s *Server) alterCommitIndex(commitIndex uint64) {
	s.commitCh <- commitIndex
}

func (s *Server) alterConfiguration(c *Configuration) {
	s.confStore.SetLatest(c)
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

// appendLogs submits the logs to the log store and updates the index states.
// NOT safe for concurrent use.
// Should be used by non-leader servers.
func (s *Server) appendLogs(bodies []*pb.LogBody) {
	lastLogIndex := s.logStore.LastIndex()
	term := s.currentTerm()
	lastCfgArrayIndex := len(bodies)
	logs := make([]*pb.Log, len(bodies))
	for i, body := range bodies {
		logs[i] = &pb.Log{
			Meta: &pb.LogMeta{
				Index: lastLogIndex + 1 + uint64(i),
				Term:  term,
			},
			Body: body.Copy(),
		}
		if logs[i].Body.Type == pb.LogType_CONFIGURATION {
			lastCfgArrayIndex = i
		}
	}
	s.logStore.AppendLogs(logs)
	s.setLastLogIndex(s.logStore.LastIndex())
	if lastCfgArrayIndex < len(logs) {
		log := logs[lastCfgArrayIndex]
		var pbConfiguration pb.Configuration
		Must1(proto.Unmarshal(log.Body.Data, &pbConfiguration))
		c := newConfiguration(&pbConfiguration)
		c.logIndex = log.Meta.Index
		s.confCh <- c
	}
}

func (s *Server) handleRPC(rpc *RPC) {
	switch request := rpc.Request.(type) {
	case *pb.AppendEntriesRequest:
		rpc.respond(s.rpcHandler.AppendEntries(context.Background(), rpc.requestID, request))
	case *pb.RequestVoteRequest:
		rpc.respond(s.rpcHandler.RequestVote(context.Background(), rpc.requestID, request))
	case *pb.InstallSnapshotRequest:
		rpc.respond(s.rpcHandler.InstallSnapshot(context.TODO(), rpc.requestID, request))
	case *pb.ApplyLogRequest:
		rpc.respond(s.rpcHandler.ApplyLog(context.Background(), rpc.requestID, request))
	default:
		s.logger.Warnw("incoming RPC is unrecognized", logFields(s, "request", rpc.Request)...)
	}
}

func (s *Server) handleTerminal() {
	sig := <-terminalSignalCh()
	s.shutdownCh <- nil
	s.logger.Infow("terminal signal captured", logFields(s, "signal", sig)...)
}

func (s *Server) randomTimer(timeout time.Duration) *time.Timer {
	randomOffset := rand.Int63n(int64(s.opts.maxTimerRandomOffsetRatio*float64(timeout)) + 1)
	return time.NewTimer(timeout + time.Duration(randomOffset))
}

func (s *Server) runBootstrap(futureTask FutureTask[any, any]) {
	// c, ok := futureTask.Task().(*Configuration)
	// if !ok {
	// 	s.logger.Panicw("received an unknown FutureTask in bootstrap", logFields(s)...)
	// }
	// s.logger.Infow("bootstrapped with congifuration", logFields(s, "configuration", c)...)
	// s.leaderConfCh <- c
	// futureTask.setResult(c, nil)
}

func (s *Server) runMainLoop() {
	for !s.shutdownState() {
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
	replResCh := s.repl.Start()
	defer s.repl.Stop()

	for s.role() == Leader {
		select {
		case response := <-replResCh:
			if response.Term > s.currentTerm() {
				// We'll update the leader in other loops
				s.stepdownFollower(nilPeer)
				s.alterTerm(response.Term)
				return
			}
		case t := <-s.bootstrapCh:
			t.setResult(nil, ErrNonFollower)
		case t := <-s.applyLogCh:
			body := t.Task()
			log := &pb.Log{
				Meta: &pb.LogMeta{
					Index: s.logStore.LastIndex() + 1,
					Term:  s.currentTerm(),
				},
				Body: body.Copy(),
			}
			if log.Body.Type == pb.LogType_CONFIGURATION {
				var pbConfiguration pb.Configuration
				Must1(proto.Unmarshal(log.Body.Data, &pbConfiguration))
				c := newConfiguration(&pbConfiguration)
				c.logIndex = log.Meta.Index
				s.repl.Stop()
				s.logStore.AppendLogs([]*pb.Log{log})
				s.setLastLogIndex(s.logStore.LastIndex())
				s.alterConfiguration(c)
				t.setResult(log.Meta, nil)
				return
			}
			s.logStore.AppendLogs([]*pb.Log{log})
			s.setLastLogIndex(s.logStore.LastIndex())
			t.setResult(log.Meta, nil)
		case commitIndex := <-s.commitCh:
			s.updateCommitIndex(commitIndex)
		case rpc := <-s.trans.RPC():
			go s.handleRPC(rpc)
		case <-s.snapshotCh:
			s.stateMachine.Snapshot()
		case err := <-s.shutdownCh:
			s.internalShutdown(err)
			return
		}
	}
}

func (s *Server) runLoopCandidate() {
	s.logger.Infow("run candidate loop", logFields(s)...)

	electionTimer := s.randomTimer(s.opts.electionTimeout)
	voteResCh, voteCancel := s.startElection()
	defer voteCancel()

	currentVotes := 0
	nextVotes := 0

	c := s.confStore.Latest()

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
					leaderPeer := s.confStore.Latest().Peer(s.id)
					s.alterLeader(leaderPeer)
					return
				}
			} else {
				if currentVotes >= c.CurrentConfig().Quorum() && nextVotes >= c.NextConfig().Quorum() {
					voteCancel()
					s.logger.Infow("won the election", logFields(s)...)
					s.alterRole(Leader)
					leaderPeer := s.confStore.Latest().Peer(s.id)
					s.alterLeader(leaderPeer)
					return
				}
			}
		case <-electionTimer.C:
			s.logger.Infow("timed out in Candidate loop", logFields(s)...)
			voteCancel()
			return
		case t := <-s.bootstrapCh:
			t.setResult(nil, ErrNonFollower)
		case commitIndex := <-s.commitCh:
			s.updateCommitIndex(commitIndex)
		case c := <-s.confCh:
			s.alterConfiguration(c)
			return
		case rpc := <-s.trans.RPC():
			go s.handleRPC(rpc)
		case err := <-s.shutdownCh:
			voteCancel()
			s.internalShutdown(err)
			return
		}
	}
}

func (s *Server) runLoopFollower() {
	s.logger.Infow("run follower loop", logFields(s)...)
	followerTimer := s.randomTimer(s.opts.followerTimeout)
	for s.role() == Follower {
		select {
		case <-followerTimer.C:
			s.logger.Infow("follower timed out", logFields(s)...)
			s.alterRole(Candidate)
			return
		case t := <-s.bootstrapCh:
			s.runBootstrap(t)
		case <-s.applyLogCh:
			//
		case commitIndex := <-s.commitCh:
			s.updateCommitIndex(commitIndex)
		case c := <-s.confCh:
			s.alterConfiguration(c)
			return
		case rpc := <-s.trans.RPC():
			followerTimer.Reset(s.opts.followerTimeout)
			go s.handleRPC(rpc)
		case err := <-s.shutdownCh:
			s.internalShutdown(err)
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

func (s *Server) internalShutdown(err error) {
	if !s.setShutdownState() {
		return
	}
	s.logger.Infow("ready to shutdown", logFields(s, zap.Error(err))...)
	if err := s.apiServer.Stop(); err != nil {
		s.logger.Warnw("error occurred stopping the API server", logFields(s, zap.Error(err))...)
	}
	s.snapshotSched.Stop()
	// Close the Transport
	if closer, ok := s.trans.(TransportCloser); ok {
		closer.Close()
		s.logger.Infow(fmt.Sprintf("transport %T closed", s.trans), logFields(s)...)
	} else {
		s.logger.Infow(fmt.Sprintf("transport %T does not implement interface TransportCloser", s.trans), logFields(s)...)
	}
	// Send err (if any) to the serve error channel
	s.serveErrCh <- err
}

func (s *Server) startElection() (<-chan *pb.RequestVoteResponse, context.CancelFunc) {
	s.logger.Infow("ready to start the election", logFields(s)...)
	s.alterTerm(s.currentTerm() + 1)
	s.setLastVoteSummary(s.currentTerm(), s.id)
	s.logger.Infow("election started", logFields(s)...)

	voteCtx, voteCancel := context.WithCancel(context.Background())

	c := s.confStore.Latest()
	resCh := make(chan *pb.RequestVoteResponse, len(c.Peers()))

	lastTerm, lastIndex := s.logStore.LastTermIndex()

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

	return resCh, voteCancel
}

func (s *Server) startMetrics(exporter MetricsExporter) {

}

func (s *Server) updateCommitIndex(commitIndex uint64) {
	s.logger.Infow("ready to update commit index", logFields(s, "new_commit_index", commitIndex)...)
	if commitIndex > s.lastLogIndex() {
		// Commit index should never overflow the log index.
		commitIndex = s.lastLogIndex()
	}
	lastAppliedIndex := s.lastAppliedIndex()
	if lastAppliedIndex == commitIndex {
		s.logger.Infow("lastAppliedIndex == commitIndex, there's nothing to apply", logFields(s)...)
		return
	}
	s.setCommitIndex(commitIndex)
	if lastAppliedIndex > commitIndex {
		s.logger.Panicw("confusing condition: lastAppliedIndex > commitIndex", logFields(s)...)
	}
	firstIndex := lastAppliedIndex + 1
	s.logger.Infow("ready to apply logs", logFields(s, "first_index", firstIndex, "last_index", commitIndex)...)
	for i := firstIndex; i <= commitIndex; i++ {
		log := s.logStore.Entry(i)
		if log == nil {
			// We've found one or more gaps in the logs
			s.logger.Panicw("one or more log gaps are detected", logFields(s, "missing_index", i)...)
		}
		switch log.Body.Type {
		case pb.LogType_COMMAND:
			s.stateMachine.Apply(log.Meta.Index, log.Meta.Term, log.Body.Data)
		case pb.LogType_CONFIGURATION:
			var pbConfiguration pb.Configuration
			Must1(proto.Unmarshal(log.Body.Data, &pbConfiguration))
			c := newConfiguration(&pbConfiguration)
			c.logIndex = log.Meta.Index
			// If the latest configuration is in a joint consensus, commit the joint consensus
			// and append the post-transition configuration.
			if latest := s.confStore.Latest(); latest.Joint() && latest.logIndex == log.Meta.Index {
				Must1(s.confStore.CommitTransition())
			}
		}
	}
	s.setLastAppliedIndex(commitIndex)
	s.logger.Infow("logs has been applied", logFields(s, "first_index", firstIndex, "last_index", commitIndex)...)
}

// Apply.
// Future(LogMeta, error)
func (s *Server) Apply(ctx context.Context, body *pb.LogBody) FutureTask[*pb.LogMeta, *pb.LogBody] {
	fu := newFutureTask[*pb.LogMeta](body.Copy())
	if s.role() == Leader {
		// Leader path
		select {
		case s.applyLogCh <- fu:
		case <-ctx.Done():
			fu.setResult(nil, ErrDeadlineExceeded)
		}
	} else {
		go func() {
			// Redirect requests to the leader on non-leader servers.
			response, err := s.trans.ApplyLog(ctx, s.Leader(), &pb.ApplyLogRequest{Body: body.Copy()})
			if err != nil {
				fu.setResult(nil, err)
			}
			switch r := response.Response.(type) {
			case *pb.ApplyLogResponse_Meta:
				fu.setResult(r.Meta, nil)
			case *pb.ApplyLogResponse_Error:
				fu.setResult(nil, errors.New(r.Error))
			}
		}()
	}
	return fu
}

// ApplyCommand.
// Future(LogMeta, error)
func (s *Server) ApplyCommand(ctx context.Context, command Command) FutureTask[*pb.LogMeta, *pb.LogBody] {
	return s.Apply(ctx, &pb.LogBody{
		Type: pb.LogType_COMMAND,
		Data: command,
	})
}

func (s *Server) Bootstrap(c *Configuration) Future[any] {
	if s.shutdownState() {
		return newErrorFuture(ErrServerShutdown)
	}
	task := newFutureTask[any, any](c)
	select {
	case s.bootstrapCh <- task:
		return task
	case err := <-s.shutdownCh:
		s.internalShutdown(err)
		return newErrorFuture(ErrServerShutdown)
	}
}

func (s *Server) StateMachine() StateMachine {
	return s.stateMachine.stateMachine
}

func (s *Server) ID() string {
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
	if v := s.clusterLeader.Load(); v != nil && v != nilPeer {
		return v.(*pb.Peer)
	}
	return nilPeer
}

func (s *Server) setLeader(leader *pb.Peer) {
	if leader == nil {
		leader = nilPeer
	}
	s.clusterLeader.Store(leader)
}

func (s *Server) Register(peer *pb.Peer) error {
	latest := s.confStore.Latest()
	next := latest.Current.Copy()
	next.Peers = append(next.Peers, peer)
	return s.confStore.InitiateTransition(newConfig(next))
}

func (s *Server) Serve() error {
	if !atomic.CompareAndSwapUint32(&s.serveFlag, 0, 1) {
		return errors.New("Serve() can only be called once")
	}

	go s.handleTerminal()

	c := s.confStore.Latest()

	// The server must be the first node in a cluster or a node in a restored cluster.
	if len(c.Peers()) > 0 {
		// The latest configuration holds a non-empty peer list.
		// The server should be a node in a restored cluster.
		selfRegistered := false
		for _, peer := range c.Peers() {
			if s.id == peer.Id {
				// Check for an edge condition
				if s.Endpoint() != peer.Endpoint {
					s.logger.Panicw("confusing condition: two servers have the same ID but different endpoints",
						logFields(s)...)
				}
				break
			}
		}
		if !selfRegistered {
			s.logger.Panicw("the server is not in the latest configuration's peer list", logFields(s)...)
		}
	} else {
		// The latest configuration does not contain any peers.
		// The server should be the first node in the cluster.
		c := newConfiguration(&pb.Configuration{
			Current: &pb.Config{
				Peers: []*pb.Peer{{Id: s.id, Endpoint: s.Endpoint()}},
			},
		})
		s.confStore.ArbitraryAppend(c)
	}

	if s.opts.metricsExporter != nil {
		go s.startMetrics(s.opts.metricsExporter)
	}

	go func() {
		if err := s.trans.Serve(); err != nil {
			s.internalShutdown(err)
		}
	}()

	go s.serveAPIServer()

	s.snapshotSched.Start()

	go s.runMainLoop()

	return <-s.serveErrCh
}

func (s *Server) Shutdown(err error) {
	s.shutdownCh <- err
}

func (s *Server) Snapshot() {

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
