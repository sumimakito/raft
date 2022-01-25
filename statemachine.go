package raft

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type StateMachine interface {
	Apply(command Command)
	Snapshot() StateMachineSnapshot
	Restore()
}

type StateMachineSnapshot interface {
	Write(sink SnapshotSink) error
}

// stateMachineAdapter acts as a proxy between the underlying StateMachine and
// the server instance and hides details for snapshotting.
type stateMachineAdapter struct {
	server       *Server
	stateMachine StateMachine

	lastIndex uint64
	lastTerm  uint64

	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64
	lastSnapshotID    string
}

func newStateMachineAdapter(server *Server, stateMachine StateMachine) *stateMachineAdapter {
	return &stateMachineAdapter{server: server, stateMachine: stateMachine}
}

// Apply receives a command and its containing log's index and term, apply the
// command to the underlying StateMachine and records the index and term.
// Unsafe for concurrent use.
func (a *stateMachineAdapter) Apply(index, term uint64, command Command) {
	a.stateMachine.Apply(command)
	a.lastIndex, a.lastTerm = index, term
}

// Snapshot takes a snapshot of the underlying StateMachine and returns the id of
// the newly taken snapshot.
// Unsafe for concurrent use.
func (a *stateMachineAdapter) Snapshot() (spanshotID string, err error) {
	c := a.server.confStore.Latest()
	if c.Joint() {
		a.server.logger.Debugw("snapshot skipped due to joint consensus", logFields(a.server)...)
		return a.lastSnapshotID, nil
	}
	snapshot := a.stateMachine.Snapshot()
	index, term := a.lastIndex, a.lastTerm
	if index == a.lastSnapshotIndex && term == a.lastSnapshotTerm {
		a.server.logger.Debugw("snapshot skipped", logFields(a.server)...)
		return a.lastSnapshotID, nil
	}
	sink, err := a.server.snapshot.Create(index, term, c.Configuration)
	if err != nil {
		return "", err
	}
	snapshotId := sink.Id()
	a.server.logger.Infow("ready to take a snapshot",
		logFields(a.server,
			zap.String("snapshot_id", snapshotId),
			zap.Uint64("snapshot_index", index),
			zap.Uint64("snapshot_term", term))...)
	if err := snapshot.Write(sink); err != nil {
		if cancelError := sink.Cancel(); cancelError != nil {
			return "", errors.Wrap(cancelError, err.Error())
		}
		return "", err
	}
	if err := sink.Close(); err != nil {
		return "", err
	}
	a.lastSnapshotIndex, a.lastSnapshotTerm, a.lastSnapshotID = index, term, snapshotId
	a.server.logger.Infow("snapshot has been taken",
		logFields(a.server,
			zap.String("snapshot_id", snapshotId),
			zap.Uint64("snapshot_index", index),
			zap.Uint64("snapshot_term", term))...)
	return snapshotId, nil
}
