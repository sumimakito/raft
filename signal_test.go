package raft

import (
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHandleTerminalSignals(t *testing.T) {
	signals := []syscall.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}
	timeout := 1000 * time.Millisecond

	for i := range signals {
		sig := signals[i]
		t.Run(sig.String(), func(t *testing.T) {
			c := terminalSignalCh()
			syscall.Kill(syscall.Getpid(), sig)
			select {
			case <-time.NewTimer(timeout).C:
				assert.FailNow(t, "timed out waiting for signal")
			case s := <-c:
				assert.Equal(t, sig, s)
			}
		})
	}
}
