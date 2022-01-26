package pb

import (
	"fmt"

	"go.uber.org/zap/zapcore"
)

var NilLog = &Log{Meta: &LogMeta{Index: 0, Term: 0}}

func (m *LogMeta) Copy() *LogMeta {
	return &LogMeta{
		Index: m.Index,
		Term:  m.Term,
	}
}

func (m *LogMeta) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddUint64("index", m.Index)
	e.AddUint64("term", m.Term)
	return nil
}

func (b *LogBody) Copy() *LogBody {
	return &LogBody{
		Type: b.Type,
		Data: append(([]byte)(nil), b.Data...),
	}
}

func (b *LogBody) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("type", b.Type.String())
	dataLen := len(b.Data)
	if dataLen == 1 {
		e.AddString("data", "<1 byte>")
	} else {
		e.AddString("data", fmt.Sprintf("<%d bytes>", dataLen))
	}
	return nil
}

func (l *Log) Copy() *Log {
	return &Log{
		Meta: l.Meta.Copy(),
		Body: l.Body.Copy(),
	}
}

func (l *Log) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddUint64("index", l.Meta.Index)
	e.AddUint64("term", l.Meta.Term)
	e.AddString("type", l.Body.Type.String())
	dataLen := len(l.Body.Data)
	if dataLen == 1 {
		e.AddString("data", "<... 1 byte>")
	} else {
		e.AddString("data", fmt.Sprintf("<... %d bytes>", dataLen))
	}
	return nil
}

type logArray []*Log

func (a logArray) MarshalLogArray(e zapcore.ArrayEncoder) error {
	for _, l := range a {
		if err := e.AppendObject(l); err != nil {
			return err
		}
	}
	return nil
}
