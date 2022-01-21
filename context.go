package raft

import (
	"context"
	"time"
)

var defaultTimeout = 60 * time.Second

func Context(timeout ...time.Duration) (context.Context, context.CancelFunc) {
	t := defaultTimeout
	if len(timeout) > 0 {
		t = timeout[0]
	}
	return context.WithTimeout(context.Background(), t)
}
