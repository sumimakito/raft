package raft

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/sumimakito/raft/pb"
)

type internalTransClientLookup struct {
	mu      sync.RWMutex
	clients map[string]*internalTransClient
}

func newInternalTransClientLookup() *internalTransClientLookup {
	return &internalTransClientLookup{clients: map[string]*internalTransClient{}}
}

func (l *internalTransClientLookup) Get(endpoint string) (*internalTransClient, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	client, ok := l.clients[endpoint]
	return client, ok
}

func (l *internalTransClientLookup) Register(client *internalTransClient) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.clients[client.endpoint] = client
}

func (l *internalTransClientLookup) Unregister(client *internalTransClient) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.clients, client.endpoint)
}

type internalTransClient struct {
	endpoint string
	rpcCh    chan *RPC
}

func newInternalTransClient(endpoint string) *internalTransClient {
	return &internalTransClient{endpoint: endpoint, rpcCh: make(chan *RPC, 16)}
}

func (s *internalTransClient) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	r := NewRPC(ctx, request)
	s.rpcCh <- r
	response, err := r.Response()
	if err != nil {
		return nil, err
	}
	return response.(*pb.AppendEntriesResponse), nil
}

func (s *internalTransClient) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	r := NewRPC(ctx, request)
	s.rpcCh <- r
	response, err := r.Response()
	if err != nil {
		return nil, err
	}
	return response.(*pb.RequestVoteResponse), nil
}

func (s *internalTransClient) InstallSnapshot(
	ctx context.Context,
	requestMeta *pb.InstallSnapshotRequestMeta,
	reader io.Reader,
) (*pb.InstallSnapshotResponse, error) {
	request := &InstallSnapshotRequest{
		Metadata: requestMeta,
		Reader:   io.NopCloser(reader),
	}

	r := NewRPC(ctx, request)
	s.rpcCh <- r

	response, err := r.Response()
	if err != nil {
		return nil, err
	}
	return response.(*pb.InstallSnapshotResponse), nil
}

func (s *internalTransClient) ApplyLog(ctx context.Context, request *pb.ApplyLogRequest) (*pb.ApplyLogResponse, error) {
	r := NewRPC(ctx, request)
	s.rpcCh <- r
	response, err := r.Response()
	if err != nil {
		return nil, err
	}
	return response.(*pb.ApplyLogResponse), nil
}

type internalTransport struct {
	lookup   *internalTransClientLookup
	endpoint string
	client   *internalTransClient
}

func newInternalTransport(lookup *internalTransClientLookup, endpoint string) (*internalTransport, error) {
	return &internalTransport{lookup: lookup, endpoint: endpoint, client: newInternalTransClient(endpoint)}, nil
}

func (t *internalTransport) Endpoint() string {
	return t.client.endpoint
}

func (t *internalTransport) AppendEntries(
	ctx context.Context, peer *pb.Peer, request *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {
	client, ok := t.lookup.Get(peer.Endpoint)
	if !ok {
		return nil, fmt.Errorf("client %s not registered", peer.Endpoint)
	}
	response, err := client.AppendEntries(ctx, request)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (t *internalTransport) RequestVote(
	ctx context.Context, peer *pb.Peer, request *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {
	client, ok := t.lookup.Get(peer.Endpoint)
	if !ok {
		return nil, fmt.Errorf("client %s not registered", peer.Endpoint)
	}
	response, err := client.RequestVote(ctx, request)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (t *internalTransport) InstallSnapshot(
	ctx context.Context, peer *pb.Peer, requestMeta *pb.InstallSnapshotRequestMeta, reader io.Reader,
) (*pb.InstallSnapshotResponse, error) {
	client, ok := t.lookup.Get(peer.Endpoint)
	if !ok {
		return nil, fmt.Errorf("client %s not registered", peer.Endpoint)
	}
	response, err := client.InstallSnapshot(ctx, requestMeta, reader)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (t *internalTransport) ApplyLog(
	ctx context.Context, peer *pb.Peer, request *pb.ApplyLogRequest,
) (*pb.ApplyLogResponse, error) {
	client, ok := t.lookup.Get(peer.Endpoint)
	if !ok {
		return nil, fmt.Errorf("client %s not registered", peer.Endpoint)
	}
	response, err := client.ApplyLog(ctx, request)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (t *internalTransport) RPC() <-chan *RPC {
	return t.client.rpcCh
}

func (t *internalTransport) Serve() error {
	t.lookup.Register(t.client)
	return nil
}

func (t *internalTransport) Close() error {
	t.lookup.Unregister(t.client)
	return nil
}
