package raft

type Peer struct {
	ID       ServerID       `json:"id"`
	Endpoint ServerEndpoint `json:"endpoint"`
}

var nilPeer = Peer{}
