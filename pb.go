package raft

import (
	"errors"
	"reflect"
)

type ProtoCoder interface {
	EncodeProto(ptr interface{}) error
	DecodeProto(ptr interface{}) error
}

func EncodeProto(in interface{}, out interface{}) error {
	vi := reflect.ValueOf(in)
	if vi.Kind() != reflect.Pointer {
		return errors.New("in is not a pointer")
	}
	if reflect.ValueOf(out).Kind() != reflect.Pointer {
		return errors.New("out is not a pointer")
	}

	coder, ok := vi.Interface().(ProtoCoder)
	if !ok {
		return errors.New("in does not implement ProtoCoder")
	}
	return coder.EncodeProto(out)
}

func DecodeProto(in interface{}, out interface{}) error {
	if reflect.ValueOf(in).Kind() != reflect.Pointer {
		return errors.New("in is not a pointer")
	}
	vo := reflect.ValueOf(out)
	if vo.Kind() != reflect.Pointer {
		return errors.New("out is not a pointer")
	}

	coder, ok := vo.Interface().(ProtoCoder)
	if !ok {
		return errors.New("out does not implement ProtoCoder")
	}
	return coder.DecodeProto(in)
}
