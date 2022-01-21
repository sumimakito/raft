package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type ServerID string

type ServerEndpoint string

type ServerInfo struct {
	ID       ServerID       `json:"id"`
	Endpoint ServerEndpoint `json:"endpoint"`
}

type ServerStates struct {
	ID                ServerID       `json:"id"`
	Endpoint          ServerEndpoint `json:"endpoint"`
	Leader            Peer           `json:"leader"`
	Role              ServerRole     `json:"role"`
	CurrentTerm       uint64         `json:"current_term"`
	LastLogIndex      uint64         `json:"last_log_index"`
	LastVoteTerm      uint64         `json:"last_vote_term"`
	LastVoteCandidate ServerID       `json:"last_vote_candidate"`
	CommitIndex       uint64         `json:"commit_index"`
}

type serverChannels struct {
	noCopy
	bootstrapCh chan FutureTask

	// confCh receives updates on the configuration.
	// Should be used only by the leader.
	confCh chan *Configuration

	rpcCh chan *RPC

	// applyLogCh receives log applying requests.
	// Non-leader servers should redirect this request to the leader.
	applyLogCh chan FutureTask

	// commitCh receives updates on the commit index.
	commitCh chan uint64

	shutdownCh    chan error
	terminalSigCh chan error
}

type Server struct {
	id   ServerID
	opts *serverOptions

	clusterLeader atomic.Value // Peer

	stable *stableStore
	serverState
	commitState

	serverChannels

	rpcHandler *rpcHandler
	repl       *replScheduler

	apiServer *apiServer

	confStore    configurationStore
	logStore     LogStore
	stateMachine StateMachine
	trans        Transport

	logger    *zap.SugaredLogger
	serveFlag uint32
}

func NewServer(id ServerID, logStore LogStore, stateMachine StateMachine, transport Transport, opts ...ServerOption) *Server {
	server := &Server{
		id:          id,
		serverState: serverState{stateRole: Follower},
		commitState: commitState{},
		serverChannels: serverChannels{
			bootstrapCh:   make(chan FutureTask, 1),
			confCh:        make(chan *Configuration, 16),
			rpcCh:         make(chan *RPC, 16),
			applyLogCh:    make(chan FutureTask, 16),
			commitCh:      make(chan uint64, 16),
			shutdownCh:    make(chan error, 1),
			terminalSigCh: make(chan error, 1),
		},
		logStore:     logStore,
		stateMachine: stateMachine,
		trans:        transport,
		opts:         applyServerOpts(opts...),
	}
	server.logger = wrappedServerLogger()
	go func() { <-terminalSignalCh(); _ = server.logger.Sync() }()
	server.stable = newStableStore(server)
	server.rpcHandler = newRPCHandler(server)
	server.repl = newReplScheduler(server)
	server.apiServer = newAPIServer(server, server.opts.apiExtensions...)
	server.restoreStates()
	server.confStore = newConfigurationStore(server)

	return server
}

func (s *Server) alterCommitIndex(commitIndex uint64) {
	s.commitCh <- commitIndex
}

func (s *Server) alterConfiguration(c *Configuration) {
	s.confStore.SetLatest(c)
	s.logger.Infow("configuration has been updated", logFields(s, "configuration", c)...)
}

func (s *Server) alterLeader(leader Peer) {
	s.logger.Infow("alter leader", logFields(s, "new_leader", leader)...)
	s.SetLeader(leader)
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
func (s *Server) stepdownFollower(leader Peer) {
	if s.role() < Follower {
		s.logger.Panicw("stepdownFollower() requires the server to have a role which is higher than follower",
			logFields(s)...)
	}
	s.SetLeader(leader)
	s.setRole(Follower)
}

// appendLogs submits the logs to the log store and updates the index states.
// NOT safe for concurrent use.
// Should be used by non-leader servers.
func (s *Server) appendLogs(bodies []LogBody) {
	lastLogIndex := s.logStore.LastIndex()
	term := s.currentTerm()
	lastCfgArrayIndex := len(bodies)
	logs := make([]*Log, len(bodies))
	for i := range bodies {
		logs[i] = &Log{
			LogMeta: LogMeta{
				Index: lastLogIndex + 1 + uint64(i),
				Term:  term,
			},
			LogBody: LogBody{Type: bodies[i].Type, Data: append(([]byte)(nil), bodies[i].Data...)},
		}
		if logs[i].Type == LogConfiguration {
			lastCfgArrayIndex = i
		}
	}
	s.logStore.AppendLogs(logs)
	s.setLastLogIndex(s.logStore.LastIndex())
	if lastCfgArrayIndex < len(logs) {
		log := logs[lastCfgArrayIndex]
		c := &Configuration{logIndex: log.Index}
		c.Decode(log.Data)
		s.confCh <- c
	}
}

func (s *Server) handleRPC(rpc *RPC) {
	switch request := rpc.Request.(type) {
	case *AppendEntriesRequest:
		rpc.respond(s.rpcHandler.AppendEntries(context.Background(), rpc.requestID, request))
	case *RequestVoteRequest:
		rpc.respond(s.rpcHandler.RequestVote(context.Background(), rpc.requestID, request))
	case *ApplyLogRequest:
		rpc.respond(s.rpcHandler.ApplyLog(context.Background(), rpc.requestID, request))
	default:
		s.logger.Warnw("incoming RPC is unrecognized", logFields(s, "request", rpc.Request)...)
	}
}

func (s *Server) handleFinale() {
	sig := <-terminalSignalCh()
	s.shutdownCh <- nil
	s.logger.Infow("terminal signal captured", logFields(s, "signal", sig)...)
}

func (s *Server) randomTimer(timeout time.Duration) *time.Timer {
	randomOffset := rand.Int63n(int64(s.opts.maxTimerRandomOffsetRatio*float64(timeout)) + 1)
	return time.NewTimer(timeout + time.Duration(randomOffset))
}

func (s *Server) runBootstrap(futureTask FutureTask) {
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
			body := t.Task().(LogBody)
			log := &Log{
				LogMeta: LogMeta{
					Index: s.logStore.LastIndex() + 1,
					Term:  s.currentTerm(),
				},
				LogBody: LogBody{Type: body.Type, Data: append(([]byte)(nil), body.Data...)},
			}
			if log.Type == LogConfiguration {
				c := &Configuration{logIndex: log.Index}
				c.Decode(log.Data)
				s.repl.Stop()
				s.logStore.AppendLogs([]*Log{log})
				s.setLastLogIndex(s.logStore.LastIndex())
				s.alterConfiguration(c)
				t.setResult(log.LogMeta, nil)
				return
			}
			s.logStore.AppendLogs([]*Log{log})
			s.setLastLogIndex(s.logStore.LastIndex())
			t.setResult(log.LogMeta, nil)
		case commitIndex := <-s.commitCh:
			s.updateCommitIndex(commitIndex)
		case rpc := <-s.trans.RPC():
			go s.handleRPC(rpc)
		case err := <-s.shutdownCh:
			s.runShutdown(err)
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
			if c.Current.Contains(response.ServerID) {
				currentVotes++
			}
			if c.Joint() && c.Next.Contains(response.ServerID) {
				nextVotes++
			}
			if !c.Joint() {
				if currentVotes >= c.Current.Quorum() {
					voteCancel()
					s.logger.Infow("won the election", logFields(s)...)
					s.alterRole(Leader)
					leaderPeer := s.confStore.Latest().Peer(s.id)
					s.alterLeader(leaderPeer)
					return
				}
			} else {
				if currentVotes >= c.Current.Quorum() && nextVotes >= c.Next.Quorum() {
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
			s.runShutdown(err)
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
			s.runShutdown(err)
			return
		}
	}
}

func (s *Server) runShutdown(err error) {
	// atomically set the shutdown flag once
	if !s.setShutdownState() {
		return
	}
	s.logger.Infow("ready to shutdown", logFields(s, "error", err)...)
	// Send err (if any) to the terminal channel
	s.terminalSigCh <- err
	defer close(s.terminalSigCh)
	// Close the Transport
	if closer, ok := s.trans.(TransportCloser); ok {
		closer.Close()
		s.logger.Infow(fmt.Sprintf("transport %T closed", s.trans), logFields(s)...)
	} else {
		s.logger.Infow(fmt.Sprintf("transport %T does not implement interface TransportCloser", s.trans), logFields(s)...)
	}
}

func (s *Server) serveAPIServer() {
	for !s.shutdownState() {
		rand.Seed(time.Now().UnixNano())
		bindAddress := s.opts.apiServerListenAddress
		if bindAddress == "" {
			bindAddress = fmt.Sprintf("0.0.0.0:%d", 20000+rand.Intn(25001))
		}
		listener, err := net.Listen("tcp", bindAddress)
		if err != nil {
			s.logger.Warn(err)
		}
		if err := s.apiServer.Serve(listener); err != nil {
			s.logger.Warn(err)
		}
	}
}

func (s *Server) startElection() (<-chan *RequestVoteResponse, context.CancelFunc) {
	s.logger.Infow("ready to start the election", logFields(s)...)
	s.alterTerm(s.currentTerm() + 1)
	s.setLastVoteSummary(s.currentTerm(), s.id)
	s.logger.Infow("election started", logFields(s)...)

	voteCtx, voteCancel := context.WithCancel(context.Background())

	c := s.confStore.Latest()
	resCh := make(chan *RequestVoteResponse, len(c.Peers()))

	lastTerm, lastIndex := s.logStore.LastTermIndex()

	request := &RequestVoteRequest{
		Term:         s.currentTerm(),
		CandidateID:  s.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	requestVote := func(peer Peer) {
		if response, err := s.trans.RequestVote(voteCtx, peer, request); err != nil {
			s.logger.Debugw("error requesting vote", logFields(s, "error", err)...)
		} else {
			resCh <- response
		}
	}

	for _, peer := range c.Peers() {
		// Do not ask ourself to vote
		if peer.ID == s.id {
			continue
		}
		go requestVote(peer)
	}

	resCh <- &RequestVoteResponse{ServerID: s.id, Term: s.currentTerm(), VoteGranted: true}

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
		switch log.Type {
		case LogCommand:
			s.stateMachine.Apply(log.Data)
		case LogConfiguration:
			c := &Configuration{logIndex: log.Index}
			c.Decode(log.Data)
			// If the latest configuration is in a joint consensus, commit the joint consensus
			// and append the post-transition configuration.
			if latest := s.confStore.Latest(); latest.Joint() && latest.logIndex == log.Index {
				Must1(s.confStore.CommitTransition())
			}
		}
	}
	s.setLastAppliedIndex(commitIndex)
	s.logger.Infow("logs has been applied", logFields(s, "first_index", firstIndex, "last_index", commitIndex)...)
}

// Apply.
// Future(LogMeta, error)
func (s *Server) Apply(ctx context.Context, log LogBody) FutureTask {
	t := newFutureTask(log)
	if s.role() == Leader {
		// Leader path
		select {
		case s.applyLogCh <- t:
		case <-ctx.Done():
			t.setResult(nil, ErrDeadlineExceeded)
		}
	} else {
		go func() {
			// Redirect requests to the leader on non-leader servers.
			response, err := s.trans.ApplyLog(ctx, s.Leader(), &ApplyLogRequest{Body: log})
			if err != nil {
				t.setResult(nil, err)
			}
			if response.Error != nil {
				t.setResult(nil, response.Error)
			}
			t.setResult(response.Meta, nil)
		}()
	}
	return t
}

// ApplyCommand.
// Future(LogMeta, error)
func (s *Server) ApplyCommand(ctx context.Context, command Command) FutureTask {
	return s.Apply(ctx, LogBody{Type: LogCommand, Data: command})
}

func (s *Server) Bootstrap(c *Configuration) Future {
	if s.shutdownState() {
		return newErrorFuture(ErrServerShutdown)
	}
	task := newFutureTask(c)
	select {
	case s.bootstrapCh <- task:
		return task
	case err := <-s.shutdownCh:
		s.runShutdown(err)
		return newErrorFuture(ErrServerShutdown)
	}
}

func (s *Server) StateMachine() StateMachine {
	return s.stateMachine
}

func (s *Server) ID() ServerID {
	return s.id
}

func (s *Server) Endpoint() ServerEndpoint {
	return ServerEndpoint(s.trans.Endpoint())
}

func (s *Server) Info() ServerInfo {
	return ServerInfo{
		ID:       s.id,
		Endpoint: s.Endpoint(),
	}
}

func (s *Server) Leader() Peer {
	if v := s.clusterLeader.Load(); v != nil {
		return v.(Peer)
	}
	return nilPeer
}

func (s *Server) SetLeader(leader Peer) {
	s.clusterLeader.Store(leader)
}

func (s *Server) Register(peer Peer) error {
	latest := s.confStore.Latest()
	next := latest.Current.Copy()
	next.Peers = append(next.Peers, peer)
	return s.confStore.InitiateTransition(next)
}

func (s *Server) Serve() error {
	if !atomic.CompareAndSwapUint32(&s.serveFlag, 0, 1) {
		return errors.New("Serve() can only be called once")
	}

	c := s.confStore.Latest()

	// The server must be the first node in a cluster or a node in a restored cluster.
	if len(c.Peers()) > 0 {
		// The latest configuration holds a non-empty peer list.
		// The server should be a node in a restored cluster.
		selfRegistered := false
		for _, peer := range c.Peers() {
			if s.id == peer.ID {
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
		c := &Configuration{
			Current: newRaftConfiguration([]Peer{
				{ID: s.id, Endpoint: s.Endpoint()},
			}),
		}
		s.confStore.ArbitraryAppend(c)
	}

	go s.handleFinale()
	if s.opts.metricsExporter != nil {
		go s.startMetrics(s.opts.metricsExporter)
	}
	go s.trans.Serve()
	go s.serveAPIServer()
	go s.runMainLoop()

	return <-s.terminalSigCh
}

func (s *Server) States() ServerStates {
	lastVoteSummary := s.lastVoteSummary()
	return ServerStates{
		ID:                s.id,
		Endpoint:          s.Endpoint(),
		Leader:            s.Leader(),
		Role:              s.role(),
		CurrentTerm:       s.currentTerm(),
		LastLogIndex:      s.lastLogIndex(),
		LastVoteTerm:      lastVoteSummary.term,
		LastVoteCandidate: lastVoteSummary.candidate,
		CommitIndex:       s.commitIndex(),
	}
}
