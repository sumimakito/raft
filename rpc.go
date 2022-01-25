package raft

import (
	"context"
	"io"

	"github.com/sumimakito/raft/pb"
)

type RPC struct {
	ctx        context.Context
	requestID  string
	futureTask FutureTask[any, any]
}

func NewRPC(ctx context.Context, request interface{}) *RPC {
	return &RPC{
		ctx:        ctx,
		requestID:  NewObjectID().Hex(),
		futureTask: newFutureTask[any](request),
	}
}

func (r *RPC) Context() context.Context {
	return r.ctx
}

func (r *RPC) Request() interface{} {
	return r.futureTask.Task()
}

func (r *RPC) Respond(response interface{}, err error) {
	r.futureTask.setResult(response, err)
}

func (r *RPC) Response() (interface{}, error) {
	return r.futureTask.Result()
}

type InstallSnapshotRequest struct {
	Metadata *pb.InstallSnapshotRequestMeta
	Reader   io.ReadCloser
}

type rpcHandler struct {
	server *Server
}

func newRPCHandler(server *Server) *rpcHandler {
	return &rpcHandler{server: server}
}

func (h *rpcHandler) AppendEntries(
	ctx context.Context, requestID string, request *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {
	h.server.logger.Debugw("incoming RPC: AppendEntries",
		logFields(h.server, "request_id", requestID, "request", request)...)

	response := &pb.AppendEntriesResponse{
		ServerId: h.server.id,
		Term:     h.server.currentTerm(),
		Success:  false,
	}

	if request.Term < h.server.currentTerm() {
		h.server.logger.Debugw("incoming term is stale", logFields(h.server, "request_id", requestID)...)
		return response, nil
	}

	if h.server.Leader().Id != request.LeaderId {
		leaderPeer := h.server.confStore.Latest().Peer(request.LeaderId)
		h.server.alterLeader(leaderPeer)
	}

	if request.Term > h.server.currentTerm() {
		h.server.logger.Debugw("local term is stale", logFields(h.server, "request_id", requestID)...)
		if h.server.role() != Follower {
			leaderPeer := h.server.confStore.Latest().Peer(request.LeaderId)
			h.server.stepdownFollower(leaderPeer)
		}
		h.server.alterTerm(request.Term)
		response.Term = h.server.currentTerm()
	}

	if request.PrevLogIndex > 0 {
		requestPrevLog := h.server.logStore.Entry(request.PrevLogIndex)
		if requestPrevLog == nil || request.PrevLogTerm != requestPrevLog.Meta.Term {
			h.server.logger.Infow("incoming previous log does not exist or has a different term",
				logFields(h.server, "request_id", requestID, "request", request)...)
			return response, nil
		}
	}

	if len(request.Entries) > 0 {
		lastLogIndex := h.server.lastLogIndex()
		firstAppendArrayIndex := 0
		if request.Entries[0].Meta.Index <= lastLogIndex {
			firstCleanUpIndex := uint64(0)
			for i, e := range request.Entries {
				if e.Meta.Index > lastLogIndex {
					break
				}
				log := h.server.logStore.Entry(e.Meta.Index)
				if log.Meta.Term != e.Meta.Term {
					firstCleanUpIndex = log.Meta.Index
					break
				}
				firstAppendArrayIndex = i + 1
			}
			if firstCleanUpIndex > 0 {
				h.server.logStore.DeleteAfter(firstCleanUpIndex)
			}
		}
		bodies := make([]*pb.LogBody, 0, len(request.Entries)-firstAppendArrayIndex)
		for i := firstAppendArrayIndex; i < len(request.Entries); i++ {
			bodies = append(bodies, request.Entries[i].Body.Copy())
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

func (h *rpcHandler) RequestVote(
	ctx context.Context, requestID string, request *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {
	h.server.logger.Infow("incoming RPC: RequestVote",
		logFields(h.server, "request_id", requestID, "request", request)...)

	response := &pb.RequestVoteResponse{
		ServerId: h.server.id,
		Term:     h.server.currentTerm(),
		Granted:  false,
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
		if lastVoteSummary.candidate == request.CandidateId {
			response.Granted = true
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

	h.server.setLastVoteSummary(h.server.currentTerm(), request.CandidateId)

	response.Granted = true
	return response, nil
}

func (h *rpcHandler) InstallSnapshot(
	ctx context.Context, requestID string, request *InstallSnapshotRequest,
) (*pb.InstallSnapshotResponse, error) {
	h.server.logger.Infow("incoming RPC: InstallSnapshot",
		logFields(h.server, "request_id", requestID, "request", request)...)

	snapshotMetadata, err := h.server.snapshot.DecodeMeta(request.Metadata.SnapshotMetadata)
	if err != nil {
		return nil, err
	}

	sink, err := h.server.snapshot.Create(snapshotMetadata.Index(), snapshotMetadata.Term(), snapshotMetadata.Configuration())
	if err != nil {
		return nil, err
	}

	chunk := make([]byte, 0, 1024)
	for {
		n, err := request.Reader.Read(chunk)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if _, err := sink.Write(chunk[:n]); err != nil {
			return nil, err
		}
	}
	request.Reader.Close()

	return &pb.InstallSnapshotResponse{Term: h.server.currentTerm()}, nil
}

func (h *rpcHandler) ApplyLog(ctx context.Context, requestID string, request *pb.ApplyLogRequest) (*pb.ApplyLogResponse, error) {
	h.server.logger.Infow("incoming RPC: ApplyLog",
		logFields(h.server, "request_id", requestID, "request", request)...)

	if h.server.role() != Leader {
		return &pb.ApplyLogResponse{
			Response: &pb.ApplyLogResponse_Error{
				Error: ErrNonLeader.Error(),
			},
		}, nil
	}

	result, err := h.server.Apply(ctx, request.Body).Result()
	if err != nil {
		return &pb.ApplyLogResponse{
			Response: &pb.ApplyLogResponse_Error{
				Error: err.Error(),
			},
		}, nil
	}
	return &pb.ApplyLogResponse{
		Response: &pb.ApplyLogResponse_Meta{
			Meta: result.Copy(),
		},
	}, nil
}
