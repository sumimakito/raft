package raft

type StableStore interface {
	LogStore
	StateStore
}
