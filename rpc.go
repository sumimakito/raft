package raft

import (
	"context"
)

type RPC struct {
	requestID  string
	Request    interface{}
	responseCh chan *RPCResponse
}

func NewRPC(request interface{}) *RPC {
	return &RPC{requestID: NewObjectID().Hex(), Request: request, responseCh: make(chan *RPCResponse, 1)}
}

func (rpc *RPC) respond(response interface{}, err error) {
	rpc.responseCh <- &RPCResponse{Response: response, Error: err}
}

func (rpc *RPC) Response() <-chan *RPCResponse {
	return rpc.responseCh
}

type RPCResponse struct {
	Response interface{}
	Error    error
}

type AppendEntriesRequest struct {
	Term         uint64   `json:"term" codec:"term"`
	LeaderID     ServerID `json:"leader_id" codec:"leader_id"`
	LeaderCommit uint64   `json:"leader_commit" codec:"leader_commit"`
	PrevLogTerm  uint64   `json:"prev_log_term" codec:"prev_log_term"`
	PrevLogIndex uint64   `json:"prev_log_index" codec:"prev_log_index"`
	Entries      []*Log   `json:"entries" codec:"entries"`
}

type AppendEntriesResponse struct {
	ServerID ServerID `json:"server_id" codec:"server_id"`
	Term     uint64   `json:"term" codec:"term"`
	Success  bool     `json:"success" codec:"success"`
}

type RequestVoteRequest struct {
	Term         uint64   `json:"term" codec:"term"`
	CandidateID  ServerID `json:"candidate_id" codec:"candidate_id"`
	LastLogTerm  uint64   `json:"last_log_term" codec:"last_log_term"`
	LastLogIndex uint64   `json:"last_log_index" codec:"last_log_index"`
}

type RequestVoteResponse struct {
	ServerID    ServerID `json:"server_id" codec:"server_id"`
	Term        uint64   `json:"term" codec:"term"`
	VoteGranted bool     `json:"vote_granted" codec:"vote_granted"`
}

type InstallSnapshotRequest struct {
	Term              uint64   `json:"term" codec:"term"`
	LeaderID          ServerID `json:"leader_id" codec:"leader_id"`
	LastIncludedIndex uint64   `json:"last_included_index" codec:"last_included_index"`
	LastIncludedTerm  uint64   `json:"last_included_term" codec:"last_included_term"`
	Offset            uint64   `json:"offset" codec:"offset"`
	Data              []byte   `json:"data" codec:"data"`
	Done              bool     `json:"done" codec:"done"`
}

type InstallSnapshotResponse struct {
	Term uint64 `json:"term" codec:"term"`
}

type ApplyLogRequest struct {
	Body LogBody `json:"body" codec:"body"`
}

type ApplyLogResponse struct {
	Meta  *LogMeta `json:"meta" codec:"meta"`
	Error error    `json:"error" codec:"error"`
}

type rpcHandler struct {
	server *Server
}

func newRPCHandler(server *Server) *rpcHandler {
	return &rpcHandler{server: server}
}

func (h *rpcHandler) AppendEntries(ctx context.Context, requestID string, request *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	h.server.logger.Debugw("incoming RPC: AppendEntries",
		logFields(h.server, "request_id", requestID, "request", request)...)

	response := &AppendEntriesResponse{
		ServerID: h.server.id,
		Term:     h.server.currentTerm(),
		Success:  false,
	}

	if request.Term < h.server.currentTerm() {
		h.server.logger.Debugw("incoming term is stale", logFields(h.server, "request_id", requestID)...)
		return response, nil
	}

	if h.server.Leader().ID != request.LeaderID {
		leaderPeer := h.server.confStore.Latest().Peer(request.LeaderID)
		h.server.alterLeader(leaderPeer)
	}

	if request.Term > h.server.currentTerm() {
		h.server.logger.Debugw("local term is stale", logFields(h.server, "request_id", requestID)...)
		if h.server.role() != Follower {
			leaderPeer := h.server.confStore.Latest().Peer(request.LeaderID)
			h.server.stepdownFollower(leaderPeer)
		}
		h.server.alterTerm(request.Term)
		response.Term = h.server.currentTerm()
	}

	if request.PrevLogIndex > 0 {
		requestPrevLog := h.server.logStore.Entry(request.PrevLogIndex)
		if requestPrevLog == nil || request.PrevLogTerm != requestPrevLog.Term {
			h.server.logger.Infow("incoming previous log does not exist or has a different term",
				logFields(h.server, "request_id", requestID, "request", request)...)
			return response, nil
		}
	}

	if len(request.Entries) > 0 {
		lastLogIndex := h.server.lastLogIndex()
		firstAppendArrayIndex := 0
		if request.Entries[0].Index <= lastLogIndex {
			firstCleanUpIndex := uint64(0)
			for i, e := range request.Entries {
				if e.Index > lastLogIndex {
					break
				}
				log := h.server.logStore.Entry(e.Index)
				if log.Term != e.Term {
					firstCleanUpIndex = log.Index
					break
				}
				firstAppendArrayIndex = i + 1
			}
			if firstCleanUpIndex > 0 {
				h.server.logStore.DeleteAfter(firstCleanUpIndex)
			}
		}
		bodies := make([]LogBody, 0, len(request.Entries)-firstAppendArrayIndex)
		for i := firstAppendArrayIndex; i < len(request.Entries); i++ {
			bodies = append(bodies, request.Entries[i].LogBody)
		}
		h.server.appendLogs(bodies)
	}

	if request.LeaderCommit > h.server.commitIndex() {
		h.server.logger.Infow("local commit index is stale",
			logFields(h.server, "request_id", requestID, "new_commit_index", request.LeaderCommit)...)
		h.server.alterCommitIndex(request.LeaderCommit)
	}

	response.Success = true
	return response, nil
}

func (h *rpcHandler) RequestVote(ctx context.Context, requestID string, request *RequestVoteRequest) (*RequestVoteResponse, error) {
	h.server.logger.Infow("incoming RPC: RequestVote",
		logFields(h.server, "request_id", requestID, "request", request)...)

	response := &RequestVoteResponse{
		ServerID:    h.server.id,
		Term:        h.server.currentTerm(),
		VoteGranted: false,
	}

	if request.Term < h.server.currentTerm() {
		h.server.logger.Debugw("incoming term is stale", logFields(h.server, "request_id", requestID)...)
		return response, nil
	}

	// Check if our server has voted in current term.
	lastVoteSummary := h.server.lastVoteSummary()
	if h.server.currentTerm() <= lastVoteSummary.term {
		h.server.logger.Debugw("server has voted in this term",
			logFields(h.server, "request_id", requestID, "candidate", lastVoteSummary.candidate)...)
		// Check if the granted vote is for current candidate.
		if lastVoteSummary.candidate == request.CandidateID {
			response.VoteGranted = true
		}
		return response, nil
	}

	// (5.1) Update current term and convert to follower.
	if request.Term > h.server.currentTerm() {
		if h.server.role() != Follower {
			h.server.stepdownFollower(nilPeer)
		}
		h.server.alterTerm(request.Term)
		response.Term = h.server.currentTerm()
	}

	lastTerm, lastIndex := h.server.logStore.LastTermIndex()

	// Check if candidate's term of the last log is stale.
	if request.LastLogTerm < lastTerm {
		return response, nil
	}

	// Check if candidate's index of the last log is stale if the candidate
	// and our server have the same last term.
	if request.LastLogTerm == lastTerm && request.LastLogIndex < lastIndex {
		return response, nil
	}

	h.server.setLastVoteSummary(h.server.currentTerm(), request.CandidateID)

	response.VoteGranted = true
	return response, nil
}

func (h *rpcHandler) InstallSnapshot(
	ctx context.Context, requestID string, request *InstallSnapshotRequest,
) (*InstallSnapshotRequest, error) {
	h.server.logger.Infow("incoming RPC: InstallSnapshot",
		logFields(h.server, "request_id", requestID, "request", request)...)
}

func (h *rpcHandler) ApplyLog(ctx context.Context, requestID string, request *ApplyLogRequest) (*ApplyLogResponse, error) {
	h.server.logger.Infow("incoming RPC: ApplyLog",
		logFields(h.server, "request_id", requestID, "request", request)...)

	if h.server.role() != Leader {
		return &ApplyLogResponse{Error: ErrNonLeader}, nil
	}

	result, err := h.server.Apply(ctx, request.Body).Result()
	if err != nil {
		return &ApplyLogResponse{Error: err}, nil
	}
	logMeta := result.(LogMeta)
	return &ApplyLogResponse{Meta: &logMeta}, nil
}
