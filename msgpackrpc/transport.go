package msgpackrpc

import (
	"context"
	"errors"
	"log"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"

	"github.com/sumimakito/raft"
	"github.com/ugorji/go/codec"
)

type RPCService struct {
	rpcCh chan *raft.RPC
}

func (s *RPCService) AppendEntries(request *raft.AppendEntriesRequest, response *raft.AppendEntriesResponse) error {
	rpc := raft.NewRPC(request)
	s.rpcCh <- rpc
	res := <-rpc.Response()
	if res.Error != nil {
		return res.Error
	}
	*response = *(res.Response).(*raft.AppendEntriesResponse)
	return nil
}

func (s *RPCService) RequestVote(request *raft.RequestVoteRequest, response *raft.RequestVoteResponse) error {
	rpc := raft.NewRPC(request)
	s.rpcCh <- rpc
	res := <-rpc.Response()
	if res.Error != nil {
		return res.Error
	}
	*response = *(res.Response).(*raft.RequestVoteResponse)
	return nil
}

func (s *RPCService) ApplyLog(request *raft.ApplyLogRequest, response *raft.ApplyLogResponse) error {
	rpc := raft.NewRPC(request)
	s.rpcCh <- rpc
	res := <-rpc.Response()
	if res.Error != nil {
		return res.Error
	}
	*response = *(res.Response).(*raft.ApplyLogResponse)
	return nil
}

type rpcClient struct {
	conn   net.Conn
	client *rpc.Client
}

type Transport struct {
	service *RPCService

	listener net.Listener

	serveFlag uint32

	clients   map[raft.ServerID]*rpcClient
	clientsMu sync.RWMutex // protects clients

	shutdownCh chan struct{}
}

func NewTransport(listener net.Listener) *Transport {
	t := &Transport{
		service:    &RPCService{rpcCh: make(chan *raft.RPC, 16)},
		listener:   listener,
		clients:    map[raft.ServerID]*rpcClient{},
		shutdownCh: make(chan struct{}, 1),
	}
	return t
}

func (t *Transport) Endpoint() raft.ServerEndpoint {
	return raft.ServerEndpoint(t.listener.Addr().String())
}

func (t *Transport) Serve() {
	if !atomic.CompareAndSwapUint32(&t.serveFlag, 0, 1) {
		panic("Serve() should be only called once")
	}
	log.Println("transport started", "addr", t.listener.Addr())
	rpcServer := rpc.NewServer()
	raft.Must1(rpcServer.Register(t.service))
	for {
		select {
		case <-t.shutdownCh:
			return
		default:
		}
		conn := raft.Must2(t.listener.Accept()).(net.Conn)
		rpcCodec := codec.GoRpc.ServerCodec(conn, &codec.MsgpackHandle{})
		rpcServer.ServeCodec(rpcCodec)
	}
}

func (t *Transport) Connect(peer raft.Peer) error {
	t.clientsMu.RLock()
	if _, ok := t.clients[peer.ID]; ok {
		return nil
	}
	t.clientsMu.RUnlock()
	t.clientsMu.Lock()
	defer t.clientsMu.Unlock()
	return t.connectLocked(peer)
}

func (t *Transport) Disconnect(peer raft.Peer) {
	t.clientsMu.Lock()
	defer t.clientsMu.Unlock()
	if client, ok := t.clients[peer.ID]; ok {
		delete(t.clients, peer.ID)
		client.conn.Close()
	}
}

func (t *Transport) DisconnectAll() {
	t.clientsMu.Lock()
	defer t.clientsMu.Unlock()
	for _, client := range t.clients {
		client.conn.Close()
	}
	t.clients = map[raft.ServerID]*rpcClient{}
}

func (t *Transport) connectLocked(peer raft.Peer) error {
	if _, ok := t.clients[peer.ID]; ok {
		return nil
	}
	conn, err := net.Dial("tcp", string(peer.Endpoint))
	if err != nil {
		return err
	}
	log.Println("peer connected", "addr", conn.RemoteAddr())
	rpcCodec := codec.GoRpc.ClientCodec(conn, &codec.MsgpackHandle{})
	t.clients[peer.ID] = &rpcClient{conn: conn, client: rpc.NewClientWithCodec(rpcCodec)}
	return nil
}

func (t *Transport) disconnectLocked(peer raft.Peer) {
	if client, ok := t.clients[peer.ID]; ok {
		delete(t.clients, peer.ID)
		client.conn.Close()
	}
}

func (t *Transport) tryClient(peer raft.Peer, fn func(c *rpcClient) error) error {
	retryState := -1
	var lastErr error
	var client *rpcClient
	var ok bool
retryClient:
	if retryState > 0 {
		return lastErr
	}
	retryState++
	t.clientsMu.RLock()
	client, ok = t.clients[peer.ID]
	t.clientsMu.RUnlock()
	// Check if the client is unset
	if !ok {
		t.clientsMu.Lock()
		// Check again to ensure the client is unset
		client, ok = t.clients[peer.ID]
		if ok {
			// Client is set
			t.clientsMu.Unlock()
			goto tryCall
		}
		// Client is unset
		// Try to connect it
		if err := t.connectLocked(peer); err != nil {
			t.clientsMu.Unlock()
			return err
		}
		t.clientsMu.Unlock()
		lastErr = errors.New("client not connected")
		goto retryClient
	}
tryCall:
	if err := fn(client); err != nil {
		if err == rpc.ErrShutdown {
			// Disconnect current client
			t.clientsMu.Lock()
			t.disconnectLocked(peer)
			// And try to connect it again
			if err := t.connectLocked(peer); err != nil {
				t.clientsMu.Unlock()
				return err
			}
			t.clientsMu.Unlock()
			lastErr = err
			goto retryClient
		}
		return err
	}
	return nil
}

func (t *Transport) AppendEntries(
	ctx context.Context, peer raft.Peer, request *raft.AppendEntriesRequest,
) (*raft.AppendEntriesResponse, error) {
	response := &raft.AppendEntriesResponse{}
	if err := t.tryClient(peer, func(c *rpcClient) error {
		return c.client.Call("RPCService.AppendEntries", request, response)
	}); err != nil {
		return nil, err
	}
	return response, nil
}

func (t *Transport) RequestVote(
	ctx context.Context, peer raft.Peer, request *raft.RequestVoteRequest,
) (*raft.RequestVoteResponse, error) {
	response := &raft.RequestVoteResponse{}
	if err := t.tryClient(peer, func(c *rpcClient) error {
		return c.client.Call("RPCService.RequestVote", request, response)
	}); err != nil {
		return nil, err
	}
	return response, nil
}

func (t *Transport) ApplyLog(
	ctx context.Context, peer raft.Peer, request *raft.ApplyLogRequest,
) (*raft.ApplyLogResponse, error) {
	response := &raft.ApplyLogResponse{}
	if err := t.tryClient(peer, func(c *rpcClient) error {
		return c.client.Call("RPCService.ApplyLog", request, response)
	}); err != nil {
		return nil, err
	}
	return response, nil
}

func (t *Transport) Close() error {
	t.DisconnectAll()
	t.shutdownCh <- struct{}{}
	return nil
}

func (t *Transport) RPC() <-chan *raft.RPC {
	return t.service.rpcCh
}
