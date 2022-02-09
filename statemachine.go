package raft

type StateMachine interface {
	Apply(command Command)
	Snapshot() (StateMachineSnapshot, error)
	Restore(snapshot Snapshot) error
}

type StateMachineSnapshot interface {
	Write(sink SnapshotSink) error
}

type stateMachineSnapshot struct {
	StateMachineSnapshot
	Index uint64
	Term  uint64
}

// stateMachineProxy acts as a proxy between the underlying StateMachine and
// the server instance and hides details for snapshotting.
type stateMachineProxy struct {
	server *Server
	StateMachine
}

func newStateMachineProxy(server *Server, stateMachine StateMachine) *stateMachineProxy {
	return &stateMachineProxy{server: server, StateMachine: stateMachine}
}

// Apply receives a command and its containing log's index and term, apply the
// command to the underlying StateMachine and records the index and term.
// Unsafe for concurrent use.
func (a *stateMachineProxy) Apply(command Command) {
	a.StateMachine.Apply(command)
	a.server.snapshotService.Scheduler().CountApply()
}

func (a *stateMachineProxy) Snapshot() (*stateMachineSnapshot, error) {
	s, err := a.StateMachine.Snapshot()
	if err != nil {
		return nil, err
	}
	lastApplied := a.server.lastApplied()
	return &stateMachineSnapshot{StateMachineSnapshot: s, Index: lastApplied.Index, Term: lastApplied.Term}, nil
}
