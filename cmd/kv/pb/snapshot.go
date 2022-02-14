package pb

import "go.uber.org/zap/zapcore"

func (m *SnapshotMeta) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("id", m.Id)
	e.AddUint64("index", m.Index)
	e.AddUint64("term", m.Term)
	if err := e.AddObject("configuration", m.Configuration); err != nil {
		return err
	}
	e.AddUint64("configuration_index", m.ConfigurationIndex)
	e.AddUint64("size", m.Size)
	e.AddUint64("crc64", m.Crc64)
	return nil
}
