package raft

type StateMachine interface {
	Apply(index, term uint64, command Command)
	Snapshot() (StateMachineSnapshot, error)
	Restore(snapshot Snapshot) error
}

type StateMachineSnapshot interface {
	Index() uint64
	Term() uint64
	Write(sink SnapshotSink) error
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
func (a *stateMachineProxy) Apply(index, term uint64, command Command) {
	a.StateMachine.Apply(index, term, command)
	a.server.snapshotService.Scheduler().CountApply()
}
