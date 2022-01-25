package raft

import (
	"github.com/sumimakito/raft/pb"
)

var nilPeer = &pb.Peer{Id: "", Endpoint: ""}
