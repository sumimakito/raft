package raft

type LogType uint8

const (
	LogCommand LogType = 1 + iota
	LogConfiguration
)

func (t LogType) String() string {
	switch t {
	case LogCommand:
		return "Command"
	case LogConfiguration:
		return "Configuration"
	}
	return "Unknown"
}

type LogMeta struct {
	Index uint64 `json:"index" codec:"index"`
	Term  uint64 `json:"term" codec:"term"`
}

type LogBody struct {
	Type LogType `json:"type" codec:"type"`
	Data []byte  `json:"-" codec:"data"`
}

type Log struct {
	LogMeta
	LogBody
}

type LogStore interface {
	AppendLogs(logs []*Log)
	DeleteAfter(firstIndex uint64)

	FirstIndex() uint64
	LastIndex() uint64

	Entry(index uint64) *Log
	LastEntry() *Log
	LastTermIndex() (term uint64, index uint64)
}

type LogStoreTypedFinder interface {
	FirstTypedEntry(t LogType) *Log
	LastTypedEntry(t LogType) *Log
}
