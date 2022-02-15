package raft

import (
	"context"
	"io"

	"github.com/pkg/errors"
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
		Status:   pb.ReplStatus_REPL_UNKNOWN,
	}

	if request.Term < h.server.currentTerm() {
		h.server.logger.Debugw("incoming term is stale", logFields(h.server, "request_id", requestID)...)
		response.Status = pb.ReplStatus_REPL_ERR_STALE_TERM
		return response, nil
	}

	if h.server.Leader().Id != request.LeaderId {
		leaderPeer, _ := h.server.confStore.Latest().Peer(request.LeaderId)
		h.server.alterLeader(leaderPeer)
	}

	if request.Term > h.server.currentTerm() {
		h.server.logger.Debugw("local term is stale", logFields(h.server, "request_id", requestID)...)
		if h.server.role() != Follower {
			leaderPeer, _ := h.server.confStore.Latest().Peer(request.LeaderId)
			h.server.stepdownFollower(leaderPeer)
		}
		h.server.alterTerm(request.Term)
		response.Term = h.server.currentTerm()
	}

	if request.PrevLogIndex > 0 {
		if h.server.logStore.withinCompacted(request.PrevLogIndex) {
			h.server.logger.Panicw("previous log index is compacted by the snapshot",
				logFields(h.server, "request_id", requestID, "request", request)...)
		}
		prevLogMeta, err := h.server.logStore.Meta(request.PrevLogIndex)
		if err != nil {
			return nil, err
		}
		if prevLogMeta == nil || request.PrevLogTerm != prevLogMeta.Term {
			h.server.logger.Infow("incoming previous log does not exist or has a different term",
				logFields(h.server, "request_id", requestID, "request", request)...)
			response.Status = pb.ReplStatus_REPL_ERR_NO_LOG
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
				log, err := h.server.logStore.Entry(e.Meta.Index)
				if err != nil {
					return nil, err
				}
				var logTerm uint64
				if log != nil {
					logTerm = log.Meta.Term
				}
				if logTerm != e.Meta.Term {
					firstCleanUpIndex = log.Meta.Index
					break
				}
				firstAppendArrayIndex = i + 1
			}
			if firstCleanUpIndex > 0 {
				if err := h.server.logStore.TrimSuffix(firstCleanUpIndex - 1); err != nil {
					// Return errors here should produce no side effects
					return nil, err
				}
			}
		}
		bodies := make([]*pb.LogBody, 0, len(request.Entries)-firstAppendArrayIndex)
		for i := firstAppendArrayIndex; i < len(request.Entries); i++ {
			bodies = append(bodies, request.Entries[i].Body.Copy())
		}
		appendOp := &logStoreAppendOp{FutureTask: newFutureTask[[]*pb.LogMeta](bodies)}
		h.server.logOpsCh <- appendOp
		if _, err := appendOp.Result(); err != nil {
			return nil, err
		}
	}

	if request.LeaderCommit > h.server.commitIndex() {
		h.server.logger.Infow("local commit index is stale",
			logFields(h.server, "request_id", requestID, "new_commit_index", request.LeaderCommit)...)
		h.server.alterCommitIndex(request.LeaderCommit)
	}

	response.Status = pb.ReplStatus_REPL_OK
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

	lastLog, err := h.server.logStore.LastEntry(0)
	if err != nil {
		return nil, err
	}

	var lastIndex uint64
	var lastTerm uint64

	if lastLog != nil {
		lastIndex = lastLog.Meta.Index
		lastTerm = lastLog.Meta.Term
	}

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

// TODO: Should respond to shutdown signal since it may take longer than expected
// to complete the installation.
func (h *rpcHandler) InstallSnapshot(
	ctx context.Context, requestID string, request *InstallSnapshotRequest,
) (*pb.InstallSnapshotResponse, error) {
	h.server.logger.Infow("incoming RPC: InstallSnapshot",
		logFields(h.server, "request_id", requestID, "request", request)...)

	response := &pb.InstallSnapshotResponse{Term: h.server.currentTerm()}

	if request.Metadata.Term < h.server.currentTerm() {
		h.server.logger.Debugw("incoming term is stale", logFields(h.server, "request_id", requestID)...)
		return response, nil
	}

	snapshotMeta, err := h.server.snapshotStore.DecodeMeta(request.Metadata.SnapshotMetadata)
	if err != nil {
		return nil, err
	}

	sink, err := h.server.snapshotStore.Create(
		snapshotMeta.Index(), snapshotMeta.Term(),
		snapshotMeta.Configuration(), snapshotMeta.ConfigurationIndex())
	if err != nil {
		return nil, err
	}

	snapshotMeta = sink.Meta()

	if _, err := io.Copy(sink, request.Reader); err != nil {
		if cancelError := sink.Cancel(); cancelError != nil {
			return nil, errors.Wrap(cancelError, err.Error())
		}
		return nil, err
	}

	if err := request.Reader.Close(); err != nil {
		return nil, err
	}

	if err := sink.Close(); err != nil {
		return nil, err
	}

	if _, err := h.server.snapshotService.Restore(sink.Meta().Id()); err != nil {
		return nil, err
	}

	return response, nil
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
