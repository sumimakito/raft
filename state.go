package raft

import (
	"sync/atomic"
)

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
	candidate string
}

// nilVoteSummary is equivalent to the "unvoted" state due to its zero term
var nilVoteSummary = voteSummary{term: 0, candidate: ""}

type serverState struct {
	noCopy

	stateRole            ServerRole   // volatile
	stateCurrentTerm     uint64       // persistent
	stateFirstLogIndex   uint64       // volatile
	stateLastLogIndex    uint64       // volatile
	stateLastVoteSummary atomic.Value // voteSummary persistent
	stateShutdownState   uint32       // volatile
}

func (s *Server) restoreStates() error {
	atomic.StoreUint64(&s.serverState.stateCurrentTerm, Must2(s.stableStore.CurrentTerm()))
	atomic.StoreUint64(&s.serverState.stateFirstLogIndex, Must2(s.logStore.FirstIndex()))
	atomic.StoreUint64(&s.serverState.stateLastLogIndex, Must2(s.logStore.LastIndex()))
	s.serverState.stateLastVoteSummary.Store(Must2(s.stableStore.LastVote()))
	return nil
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
	Must1(s.stableStore.SetCurrentTerm(currentTerm))
	atomic.StoreUint64(&s.serverState.stateCurrentTerm, currentTerm)
}

func (s *Server) firstLogIndex() uint64 {
	return atomic.LoadUint64(&s.serverState.stateFirstLogIndex)
}

func (s *Server) setFirstLogIndex(firstLogIndex uint64) {
	atomic.StoreUint64(&s.serverState.stateFirstLogIndex, firstLogIndex)
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

func (s *Server) setLastVoteSummary(term uint64, candidate string) {
	summary := voteSummary{term: term, candidate: candidate}
	Must1(s.stableStore.SetLastVote(summary))
	s.serverState.stateLastVoteSummary.Store(summary)
}

func (server *Server) shutdownState() bool {
	return atomic.LoadUint32(&server.serverState.stateShutdownState) != 0
}

func (server *Server) setShutdownState() bool {
	return atomic.CompareAndSwapUint32(&server.serverState.stateShutdownState, 0, 1)
}

type lastAppliedTuple struct {
	Index uint64
	Term  uint64
}

var nilLastAppliedTuple = lastAppliedTuple{Index: 0, Term: 0}

type commitState struct {
	noCopy

	aCommitIndex uint64
	aLastApplied atomic.Value // lastAppliedTuple
}

func (state *commitState) commitIndex() uint64 {
	return atomic.LoadUint64(&state.aCommitIndex)
}

func (state *commitState) setCommitIndex(commitIndex uint64) {
	atomic.StoreUint64(&state.aCommitIndex, commitIndex)
}

func (state *commitState) lastApplied() lastAppliedTuple {
	if v := state.aLastApplied.Load(); v != nil {
		return v.(lastAppliedTuple)
	}
	return nilLastAppliedTuple
}

func (state *commitState) setLastApplied(index, term uint64) {
	state.aLastApplied.Store(lastAppliedTuple{Index: index, Term: term})
}

// StateStore defines the interface to save and restore the persistent
// server states from a stable store.
type StateStore interface {
	CurrentTerm() (uint64, error)
	SetCurrentTerm(term uint64) error
	LastVote() (voteSummary, error)
	SetLastVote(summary voteSummary) error
}
