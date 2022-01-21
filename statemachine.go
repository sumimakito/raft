package raft

type StateMachine interface {
	Apply(command Command)
	Snapshot()
}
