package raft

import (
	"sync/atomic"

	"github.com/ugorji/go/codec"
)

type Config struct {
	peerMap SingleFlight
	Peers   []Peer `json:"peers" codec:"peers"`
}

func newConfig(peers []Peer) *Config {
	c := &Config{}
	c.Peers = append(c.Peers, peers...)
	return c
}

func (c *Config) Copy() *Config {
	return newConfig(c.Peers)
}

func (c *Config) Contains(serverID ServerID) bool {
	peerMap := c.peerMap.Do(func() interface{} {
		m := map[ServerID]Peer{}
		for _, p := range c.Peers {
			m[p.ID] = p
		}
		return m
	}).(map[ServerID]Peer)
	_, ok := peerMap[serverID]
	return ok
}

func (c *Config) Quorum() int {
	return len(c.Peers)/2 + 1
}

type Configuration struct {
	logIndex uint64

	peerMapSF SingleFlight
	peersSF   SingleFlight

	Current *Config `json:"current" codec:"current"` // cOld
	Next    *Config `json:"next" codec:"next"`       // cNew
}

func (c *Configuration) peerMap() map[ServerID]Peer {
	return c.peerMapSF.Do(func() interface{} {
		m := map[ServerID]Peer{}
		for i := range c.Current.Peers {
			if _, ok := m[c.Current.Peers[i].ID]; ok {
				continue
			}
			m[c.Current.Peers[i].ID] = c.Current.Peers[i]
		}
		if c.Next != nil {
			for i := range c.Next.Peers {
				if _, ok := m[c.Next.Peers[i].ID]; ok {
					continue
				}
				m[c.Next.Peers[i].ID] = c.Next.Peers[i]
			}
		}
		return m
	}).(map[ServerID]Peer)
}

func (c *Configuration) peers() []Peer {
	if c.Next == nil || len(c.Next.Peers) == 0 {
		// Fast path
		return c.Current.Peers
	}

	peerMap := c.peerMap()
	return c.peersSF.Do(func() interface{} {
		peers := make([]Peer, 0, len(peerMap))
		for _, p := range peerMap {
			peers = append(peers, p)
		}
		return peers
	}).([]Peer)
}

func (c *Configuration) Copy() *Configuration {
	out := &Configuration{Current: c.Current.Copy()}
	if out.Next != nil {
		out.Next = c.Next.Copy()
	}
	return out
}

func (c *Configuration) CopyInitiateTransition(next *Config) *Configuration {
	return &Configuration{Current: c.Current.Copy(), Next: next.Copy()}
}

func (c *Configuration) CopyCommitTransition() *Configuration {
	return &Configuration{Current: c.Next.Copy()}
}

func (c *Configuration) Encode() (out []byte) {
	codec.NewEncoderBytes(&out, &codec.MsgpackHandle{}).MustEncode(*c)
	return
}

func (c *Configuration) Decode(in []byte) {
	codec.NewDecoderBytes(in, &codec.MsgpackHandle{}).MustDecode(c)
}

func (c *Configuration) Joint() bool {
	return c.Next != nil
}

func (c *Configuration) CurrentPeers() []Peer {
	return c.Current.Peers
}

func (c *Configuration) NextPeers() []Peer {
	if c.Next != nil {
		return c.Next.Peers
	}
	return nil
}

func (c *Configuration) Peer(serverID ServerID) Peer {
	if p, ok := c.peerMap()[serverID]; ok {
		return p
	}
	return nilPeer
}

func (c *Configuration) Peers() []Peer {
	return c.peers()
}

var nilConfiguration = &Configuration{Current: newConfig(nil)}

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
		if log := finder.LastTypedEntry(LogConfiguration); log != nil {
			var latest Configuration
			latest.Decode(log.Data)
			latest.logIndex = log.Index
			c.latest.Store(latest)
		}
	} else {
		// Slow path
		for i := server.logStore.LastIndex(); i > 0; i-- {
			log := server.logStore.Entry(i)
			if log == nil {
				server.logger.Panicw("one or more log gaps are detected", logFields(server, "missing_index", i)...)
			}
			if log.Type == LogConfiguration {
				var latest Configuration
				latest.Decode(log.Data)
				latest.logIndex = log.Index
				c.latest.Store(latest)
				break
			}
		}
	}

	return c
}

func (s *configurationStore) ArbitraryAppend(c *Configuration) {
	s.server.appendLogs([]LogBody{{Type: LogConfiguration, Data: c.Encode()}})
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
	c := latest.CopyInitiateTransition(next)
	s.server.applyLogCh <- newFutureTask(LogBody{Type: LogConfiguration, Data: c.Encode()})
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
	s.server.applyLogCh <- newFutureTask(LogBody{Type: LogConfiguration, Data: c.Encode()})
	s.server.logger.Infow("a configuration transition has been committed",
		logFields(s.server, "configuration", c)...)
	return nil
}

func (s *configurationStore) Latest() *Configuration {
	return s.latest.Load().(*Configuration)
}

func (s *configurationStore) SetLatest(c *Configuration) {
	s.latest.Store(c)
}
