package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAsyncCtl(t *testing.T) {
	ctl := newAsyncCtl()
	go func() {
		defer ctl.Release()
		time.Sleep(1000 * time.Millisecond)
	}()
	ctl.Cancel()
	select {
	case <-time.NewTimer(3000 * time.Millisecond).C:
		assert.FailNow(t, "timed out waiting for asyncCtl's release")
	case <-ctl.WaitRelease():
		return
	}
}

func TestAsyncCtlCancel(t *testing.T) {
	ctl := newAsyncCtl()
	go func() {
		defer ctl.Release()
		<-ctl.Cancelled()
	}()
	ctl.Cancel()
	select {
	case <-time.NewTimer(1000 * time.Millisecond).C:
		assert.FailNow(t, "timed out waiting for asyncCtl's release")
	case <-ctl.WaitRelease():
		return
	}
}
