package pb

import (
	"go.uber.org/zap/zapcore"
)

func (r *AppendEntriesRequest) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddUint64("term", r.Term)
	e.AddString("leader_id", r.LeaderId)
	e.AddUint64("leader_commit", r.LeaderCommit)
	e.AddUint64("prev_log_index", r.PrevLogIndex)
	e.AddUint64("prev_log_term", r.PrevLogTerm)
	if err := e.AddArray("entries", LogArray(r.Entries)); err != nil {
		return err
	}
	return nil
}
