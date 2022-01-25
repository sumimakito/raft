package raft

import (
	"sync/atomic"

	"github.com/sumimakito/raft/pb"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	peerMap SingleFlight // map[string]Peer

	*pb.Config
}

func newConfig(pbConfig *pb.Config) *Config {
	return &Config{Config: pbConfig.Copy()}
}

func (c *Config) Copy() *Config {
	return newConfig(c.Config.Copy())
}

func (c *Config) Contains(serverId string) bool {
	peerMap := c.peerMap.Do(func() interface{} {
		m := map[string]*pb.Peer{}
		for _, p := range c.Peers {
			m[p.Id] = p
		}
		return m
	}).(map[string]*pb.Peer)
	_, ok := peerMap[serverId]
	return ok
}

func (c *Config) Quorum() int {
	return len(c.Peers)/2 + 1
}

type Configuration struct {
	logIndex uint64

	peerMapSingle      SingleFlight
	peersSingle        SingleFlight
	currentPeersSingle SingleFlight

	currentSingle SingleFlight
	nextSingle    SingleFlight

	*pb.Configuration
}

func newConfiguration(pbConfiguration *pb.Configuration) *Configuration {
	return &Configuration{Configuration: pbConfiguration}
}

func (c *Configuration) Copy() *Configuration {
	return newConfiguration(c.Configuration.Copy())
}

func (c *Configuration) peerMap() map[string]*pb.Peer {
	return c.peerMapSingle.Do(func() interface{} {
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
	}).(map[string]*pb.Peer)
}

func (c *Configuration) peers() []*pb.Peer {
	if c.Next == nil || len(c.Next.Peers) == 0 {
		return c.currentPeersSingle.Do(func() interface{} {
			peers := make([]*pb.Peer, 0, len(c.Current.Peers))
			for _, p := range c.Current.Peers {
				peers = append(peers, p)
			}
			return peers
		}).([]*pb.Peer)
	}

	peerMap := c.peerMap()
	return c.peersSingle.Do(func() interface{} {
		peers := make([]*pb.Peer, 0, len(peerMap))
		for _, p := range peerMap {
			peers = append(peers, p)
		}
		return peers
	}).([]*pb.Peer)
}

func (c *Configuration) CurrentConfig() *Config {
	return c.currentSingle.Do(func() interface{} { return newConfig(c.Current) }).(*Config)
}

func (c *Configuration) NextConfig() *Config {
	if c.Next == nil {
		return nil
	}
	return c.nextSingle.Do(func() interface{} { return newConfig(c.Next) }).(*Config)
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

func newConfigurationStore(server *Server) (c configurationStore) {
	c.server = server
	c.latest.Store(nilConfiguration)

	// Find the latest configuration
	if finder, ok := server.logStore.(LogStoreTypedFinder); ok {
		// Fast path
		if log := finder.LastTypedEntry(pb.LogType_CONFIGURATION); log != nil {
			var conf pb.Configuration
			Must1(proto.Unmarshal(log.Body.Data, &conf))
			c.latest.Store(&Configuration{logIndex: log.Meta.Index, Configuration: &conf})
		}
	} else {
		// Slow path
		for i := server.logStore.LastIndex(); i > 0; i-- {
			log := server.logStore.Entry(i)
			if log == nil {
				server.logger.Panicw("one or more log gaps are detected", logFields(server, "missing_index", i)...)
			}
			if log.Body.Type == pb.LogType_CONFIGURATION {
				var conf pb.Configuration
				Must1(proto.Unmarshal(log.Body.Data, &conf))
				c.latest.Store(&Configuration{logIndex: log.Meta.Index, Configuration: &conf})
				break
			}
		}
	}

	return c
}

func (s *configurationStore) ArbitraryAppend(c *Configuration) {
	s.server.appendLogs([]*pb.LogBody{
		{Type: pb.LogType_CONFIGURATION, Data: Must2(proto.Marshal(c.Configuration)).([]byte)},
	})
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
	s.server.applyLogCh <- newFutureTask[*pb.LogMeta](&pb.LogBody{
		Type: pb.LogType_CONFIGURATION,
		Data: Must2(proto.Marshal(c)).([]byte),
	})
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
	s.server.applyLogCh <- newFutureTask[*pb.LogMeta](&pb.LogBody{
		Type: pb.LogType_CONFIGURATION,
		Data: Must2(proto.Marshal(c)).([]byte),
	})
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
