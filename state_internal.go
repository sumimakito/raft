package raft

type internalStateStore struct {
	currentTerm uint64
	lastVote    voteSummary
}

func newInternalStateStore() *internalStateStore {
	return &internalStateStore{lastVote: nilVoteSummary}
}

func (s *internalStateStore) CurrentTerm() (uint64, error) {
	return s.currentTerm, nil
}

func (s *internalStateStore) SetCurrentTerm(currentTerm uint64) error {
	s.currentTerm = currentTerm
	return nil
}

func (s *internalStateStore) LastVote() (voteSummary, error) {
	return s.lastVote, nil
}

func (s *internalStateStore) SetLastVote(summary voteSummary) error {
	s.lastVote = summary
	return nil
}
