package raft

import "sync/atomic"

type ServerRole uint32

const (
	Leader ServerRole = 1 + iota
	Candidate
	Follower
)

func (r ServerRole) String() string {
	switch r {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	}
	return "Unknown"
}

type voteSummary struct {
	term      uint64
	candidate ServerID
}

// nilVoteSummary is equivalent to the "unvoted" state due to its zero term
var nilVoteSummary = voteSummary{term: 0, candidate: ""}

type serverState struct {
	noCopy

	stateRole            ServerRole   // volatile
	stateCurrentTerm     uint64       // persistent
	stateLastLogIndex    uint64       // volatile
	stateLastVoteSummary atomic.Value // voteSummary persistent
	stateShutdownState   uint32       // volatile
}

func (s *Server) restoreStates() {
	atomic.StoreUint64(&s.serverState.stateCurrentTerm, Must2(s.stable.CurrentTerm()).(uint64))
	atomic.StoreUint64(&s.serverState.stateLastLogIndex, s.logStore.LastIndex())
	s.serverState.stateLastVoteSummary.Store(Must2(s.stable.LastVote()).(voteSummary))
}

func (s *Server) role() ServerRole {
	return ServerRole(atomic.LoadUint32((*uint32)(&s.serverState.stateRole)))
}

func (s *Server) setRole(role ServerRole) {
	atomic.StoreUint32((*uint32)(&s.serverState.stateRole), uint32(role))
}

func (s *Server) currentTerm() uint64 {
	return atomic.LoadUint64(&s.serverState.stateCurrentTerm)
}

func (s *Server) setCurrentTerm(currentTerm uint64) {
	Must1(s.stable.SetCurrentTerm(currentTerm))
	atomic.StoreUint64(&s.serverState.stateCurrentTerm, currentTerm)
}

func (s *Server) lastLogIndex() uint64 {
	return atomic.LoadUint64(&s.serverState.stateLastLogIndex)
}

func (s *Server) setLastLogIndex(lastLogIndex uint64) {
	atomic.StoreUint64(&s.serverState.stateLastLogIndex, lastLogIndex)
}

func (s *Server) lastVoteSummary() voteSummary {
	if v := s.serverState.stateLastVoteSummary.Load(); v != nil {
		return v.(voteSummary)
	}
	return nilVoteSummary
}

func (s *Server) setLastVoteSummary(term uint64, candidate ServerID) {
	summary := voteSummary{term: term, candidate: candidate}
	Must1(s.stable.SetLastVote(summary))
	s.serverState.stateLastVoteSummary.Store(summary)
}

func (server *Server) shutdownState() bool {
	return atomic.LoadUint32(&server.serverState.stateShutdownState) != 0
}

func (server *Server) setShutdownState() bool {
	return atomic.CompareAndSwapUint32(&server.serverState.stateShutdownState, 0, 1)
}

type commitState struct {
	noCopy

	aCommitIndex      uint64
	aLastAppliedIndex uint64
}

func (state *commitState) commitIndex() uint64 {
	return atomic.LoadUint64(&state.aCommitIndex)
}

func (state *commitState) setCommitIndex(commitIndex uint64) {
	atomic.StoreUint64(&state.aCommitIndex, commitIndex)
}

func (state *commitState) lastAppliedIndex() uint64 {
	return atomic.LoadUint64(&state.aCommitIndex)
}

func (state *commitState) setLastAppliedIndex(lastAppliedIndex uint64) {
	atomic.StoreUint64(&state.aLastAppliedIndex, lastAppliedIndex)
}
