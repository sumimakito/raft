package pb

import "go.uber.org/zap/zapcore"

var NilPeer = &Peer{Id: "", Endpoint: ""}

func (p *Peer) Copy() *Peer {
	return &Peer{Id: p.Id, Endpoint: p.Endpoint}
}

func (p *Peer) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("id", p.Id)
	e.AddString("endpoint", p.Endpoint)
	return nil
}

type PeerArray []*Peer

func (a PeerArray) MarshalLogArray(e zapcore.ArrayEncoder) error {
	for _, p := range a {
		if err := e.AppendObject(p); err != nil {
			return err
		}
	}
	return nil
}
