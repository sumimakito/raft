package main

import (
	"github.com/sumimakito/raft"
	"github.com/ugorji/go/codec"
)

type CommandType uint8

const (
	CommandSet CommandType = 1 + iota
	CommandUnset
)

type Command struct {
	Type  CommandType
	Key   string
	Value []byte
}

func (c *Command) Encode() []byte {
	var out []byte
	codec.NewEncoderBytes(&out, &codec.MsgpackHandle{}).MustEncode(c)
	return out
}

func DecodeCommand(command raft.Command) *Command {
	var cmd Command
	codec.NewDecoderBytes(command, &codec.MsgpackHandle{}).MustDecode(&cmd)
	return &cmd
}
