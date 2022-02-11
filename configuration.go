package raft

import (
	"sync/atomic"

	"github.com/sumimakito/raft/pb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type config struct {
	peerMap SingleFlight[map[string]*pb.Peer]

	*pb.Config
}

func newConfig(pbConfig *pb.Config) *config {
	return &config{Config: pbConfig.Copy()}
}

func (c *config) Copy() *config {
	return newConfig(c.Config.Copy())
}

func (c *config) Contains(serverId string) bool {
	peerMap := c.peerMap.Do(func() map[string]*pb.Peer {
		m := map[string]*pb.Peer{}
		for _, p := range c.Peers {
			m[p.Id] = p
		}
		return m
	})
	_, ok := peerMap[serverId]
	return ok
}

func (c *config) Quorum() int {
	return len(c.Peers)/2 + 1
}

type configuration struct {
	*pb.Configuration

	peerMapSingle      SingleFlight[map[string]*pb.Peer]
	peersSingle        SingleFlight[[]*pb.Peer]
	currentPeersSingle SingleFlight[[]*pb.Peer]

	currentSingle SingleFlight[*config]
	nextSingle    SingleFlight[*config]

	logIndex uint64
}

var nilConfiguration = newConfiguration(&pb.Configuration{Current: &pb.Config{Peers: []*pb.Peer{}}}, 0)

func newConfiguration(pbConfiguration *pb.Configuration, logIndex uint64) *configuration {
	return &configuration{Configuration: pbConfiguration, logIndex: logIndex}
}

func (c *configuration) Copy(logIndex uint64) *configuration {
	return newConfiguration(c.Configuration.Copy(), logIndex)
}

func (c *configuration) LogIndex() uint64 {
	return c.logIndex
}

func (c *configuration) peerMap() map[string]*pb.Peer {
	return c.peerMapSingle.Do(func() map[string]*pb.Peer {
		m := map[string]*pb.Peer{}
		for i := range c.Current.Peers {
			if _, ok := m[c.Current.Peers[i].Id]; ok {
				continue
			}
			m[c.Current.Peers[i].Id] = c.Current.Peers[i]
		}
		if c.Next != nil {
			for i := range c.Next.Peers {
				if _, ok := m[c.Next.Peers[i].Id]; ok {
					continue
				}
				m[c.Next.Peers[i].Id] = c.Next.Peers[i]
			}
		}
		return m
	})
}

func (c *configuration) peers() []*pb.Peer {
	if c.Next == nil || len(c.Next.Peers) == 0 {
		return c.currentPeersSingle.Do(func() []*pb.Peer {
			peers := make([]*pb.Peer, 0, len(c.Current.Peers))
			for _, p := range c.Current.Peers {
				peers = append(peers, p)
			}
			return peers
		})
	}

	peerMap := c.peerMap()
	return c.peersSingle.Do(func() []*pb.Peer {
		peers := make([]*pb.Peer, 0, len(peerMap))
		for _, p := range peerMap {
			peers = append(peers, p)
		}
		return peers
	})
}

func (c *configuration) CurrentConfig() *config {
	return c.currentSingle.Do(func() *config { return newConfig(c.Current) })
}

func (c *configuration) NextConfig() *config {
	if c.Next == nil {
		return nil
	}
	return c.nextSingle.Do(func() *config { return newConfig(c.Next) })
}

func (c *configuration) Joint() bool {
	return c.Next != nil
}

func (c *configuration) Peer(serverId string) (*pb.Peer, bool) {
	p, ok := c.peerMap()[serverId]
	return p, ok
}

func (c *configuration) Peers() []*pb.Peer {
	return c.peers()
}

type configurationStore struct {
	server    *Server
	committed atomic.Value // *Configuration
	latest    atomic.Value // *Configuration
}

func newConfigurationStore(server *Server) (*configurationStore, error) {
	c := &configurationStore{server: server}
	c.committed.Store(nilConfiguration)
	c.latest.Store(nilConfiguration)

	// Find the latest configuration
	log, err := server.logProvider.LastEntry(pb.LogType_CONFIGURATION)
	if err != nil {
		return nil, err
	}
	if log != nil {
		var conf pb.Configuration
		if err := proto.Unmarshal(log.Body.Data, &conf); err != nil {
			return nil, err
		}
		c.latest.Store(newConfiguration(&conf, log.Meta.Index))
	}

	server.logger.Infow("latest conf", zap.Reflect("conf", c.Latest()))

	return c, nil
}

// initiateTransition creates a configuration for joint consensus that combines
// current and next configuration, and appends the configuration log.
// Should not be called when the server is already in a joint consensus.
// When the leader prepares to change the configuration, this should be the only
// function to call.
func (s *configurationStore) initiateTransition(next *config) error {
	latest := s.latest.Load().(*configuration)
	if latest.Joint() {
		return ErrInJointConsensus
	}
	c := latest.CopyInitiateTransition(next.Config)
	appendOp := &logProviderAppendOp{
		FutureTask: newFutureTask[[]*pb.LogMeta]([]*pb.LogBody{
			{Type: pb.LogType_CONFIGURATION, Data: Must2(proto.Marshal(c))},
		}),
	}
	s.server.logOpsCh <- appendOp
	if _, err := appendOp.Result(); err != nil {
		return err
	}
	s.server.logger.Infow("a configuration transition has been initiated",
		logFields(s.server, "configuration", c)...)
	return nil
}

// commitTransition creates a new configuration from the next configuration in the
// configuration for joint consensus and appends the configuration log.
// Should not be called when the server is not in a joint consensus.
// Should only be called by Server.commitAndApply().
func (s *configurationStore) commitTransition() error {
	latest := s.latest.Load().(*configuration)
	if !latest.Joint() {
		return ErrNotInJointConsensus
	}
	c := latest.CopyCommitTransition()
	s.server.appendLogs([]*pb.LogBody{
		{Type: pb.LogType_CONFIGURATION, Data: Must2(proto.Marshal(c))},
	})
	s.server.logger.Infow("a configuration transition has been committed",
		logFields(s.server, "configuration", c)...)
	return nil
}

func (s *configurationStore) Joint() bool {
	return s.latest.Load().(*configuration).Joint()
}

func (s *configurationStore) Committed() *configuration {
	return s.committed.Load().(*configuration)
}

func (s *configurationStore) SetCommitted(c *configuration) {
	if c == nil {
		c = nilConfiguration
	}
	s.committed.Store(c)
}

func (s *configurationStore) Latest() *configuration {
	return s.latest.Load().(*configuration)
}

func (s *configurationStore) SetLatest(c *configuration) {
	if c == nil {
		c = nilConfiguration
	}
	s.latest.Store(c)
}
