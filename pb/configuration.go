package pb

import "go.uber.org/zap/zapcore"

func (c *Config) Copy() *Config {
	out := &Config{}
	for _, peer := range c.Peers {
		out.Peers = append(out.Peers, peer.Copy())
	}
	return out
}

func (c *Config) MarshalLogObject(e zapcore.ObjectEncoder) error {
	if c == nil {
		return nil
	}
	if err := e.AddArray("peers", PeerArray(c.Peers)); err != nil {
		return err
	}
	return nil
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

func (c *Configuration) MarshalLogObject(e zapcore.ObjectEncoder) error {
	if err := e.AddObject("current", c.Current); err != nil {
		return err
	}
	if c.Next == nil {
		if err := e.AddReflected("next", nil); err != nil {
			return err
		}
	} else {
		if err := e.AddObject("next", c.Next); err != nil {
			return err
		}
	}
	return nil
}
