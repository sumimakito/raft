package raft

import (
	"sync/atomic"

	"github.com/sumimakito/raft/pb"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	peerMap SingleFlight[map[string]*pb.Peer]

	*pb.Config
}

func newConfig(pbConfig *pb.Config) *Config {
	return &Config{Config: pbConfig.Copy()}
}

func (c *Config) Copy() *Config {
	return newConfig(c.Config.Copy())
}

func (c *Config) Contains(serverId string) bool {
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

func (c *Config) Quorum() int {
	return len(c.Peers)/2 + 1
}

type Configuration struct {
	logIndex uint64

	peerMapSingle      SingleFlight[map[string]*pb.Peer]
	peersSingle        SingleFlight[[]*pb.Peer]
	currentPeersSingle SingleFlight[[]*pb.Peer]

	currentSingle SingleFlight[*Config]
	nextSingle    SingleFlight[*Config]

	*pb.Configuration
}

func newConfiguration(pbConfiguration *pb.Configuration) *Configuration {
	return &Configuration{Configuration: pbConfiguration}
}

func (c *Configuration) Copy() *Configuration {
	return newConfiguration(c.Configuration.Copy())
}

func (c *Configuration) peerMap() map[string]*pb.Peer {
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

func (c *Configuration) peers() []*pb.Peer {
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

func (c *Configuration) CurrentConfig() *Config {
	return c.currentSingle.Do(func() *Config { return newConfig(c.Current) })
}

func (c *Configuration) NextConfig() *Config {
	if c.Next == nil {
		return nil
	}
	return c.nextSingle.Do(func() *Config { return newConfig(c.Next) })
}

func (c *Configuration) Joint() bool {
	return c.Next != nil
}

func (c *Configuration) Peer(serverId string) *pb.Peer {
	if p, ok := c.peerMap()[serverId]; ok {
		return p
	}
	return nil
}

func (c *Configuration) Peers() []*pb.Peer {
	return c.peers()
}

var nilConfiguration = newConfiguration(
	&pb.Configuration{Current: &pb.Config{Peers: []*pb.Peer{}}},
)

type configurationStore struct {
	server *Server
	latest atomic.Value // *Configuration
}

func newConfigurationStore(server *Server) (*configurationStore, error) {
	c := &configurationStore{server: server}
	c.latest.Store(nilConfiguration)

	// Find the latest configuration
	log, err := server.logProvider.LastTypedEntry(pb.LogType_CONFIGURATION)
	if err != nil {
		return nil, err
	}
	if log != nil {
		var conf pb.Configuration
		if err := proto.Unmarshal(log.Body.Data, &conf); err != nil {
			return nil, err
		}
		c.latest.Store(&Configuration{logIndex: log.Meta.Index, Configuration: &conf})
	}

	return c, nil
}

// InitiateTransition creates a configuration for joint consensus that combines
// current and next configuration, and appends the configuration log.
// Should not be called when the server is already in a joint consensus.
// When the leader prepares to change the configuration, this should be the only
// function to call.
func (s *configurationStore) InitiateTransition(next *Config) error {
	latest := s.latest.Load().(*Configuration)
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

// CommitTransition creates a new configuration from the next configuration in the
// configuration for joint consensus and appends the configuration log.
// Should not be called when the server is not in a joint consensus.
func (s *configurationStore) CommitTransition() error {
	latest := s.latest.Load().(*Configuration)
	if !latest.Joint() {
		return ErrNotInJointConsensus
	}
	c := latest.CopyCommitTransition()
	appendOp := &logProviderAppendOp{
		FutureTask: newFutureTask[[]*pb.LogMeta]([]*pb.LogBody{
			{Type: pb.LogType_CONFIGURATION, Data: Must2(proto.Marshal(c))},
		}),
	}
	s.server.logOpsCh <- appendOp
	if _, err := appendOp.Result(); err != nil {
		return err
	}
	s.server.logger.Infow("a configuration transition has been committed",
		logFields(s.server, "configuration", c)...)
	return nil
}

func (s *configurationStore) Latest() *Configuration {
	return s.latest.Load().(*Configuration)
}

func (s *configurationStore) SetLatest(c *Configuration) {
	if c == nil {
		c = nilConfiguration
	}
	s.latest.Store(c)
}
