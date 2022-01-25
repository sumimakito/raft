package raft

import "context"

// asyncCtl is used to control an operation's cancellation in an asynchronous manner.
type asyncCtl struct {
	ctx    context.Context
	cancel context.CancelFunc
	waitCh chan struct{}
}

func newAsyncCtl() *asyncCtl {
	ctx, cancel := context.WithCancel(context.Background())
	return &asyncCtl{ctx: ctx, cancel: cancel, waitCh: make(chan struct{}, 1)}
}

// Cancel is used to signal the operation to cancel/stop. This function does not
// guarantee that the operation is immediately cancelled/stopped after the call.
func (c *asyncCtl) Cancel() {
	c.cancel()
}

// Cancelled is used with select to check if Cancel() is called.
// Operation should prepare to cancel/stop when the channel returned is closed.
func (c *asyncCtl) Cancelled() <-chan struct{} {
	return c.ctx.Done()
}

func (c *asyncCtl) Context() context.Context {
	return c.ctx
}

// Release is used to inform that the operation is cancelled/stopped.
// Should only be called once in one infCtl's lifecycle.
func (c *asyncCtl) Release() {
	c.cancel()
	close(c.waitCh)
}

// WaitRelease is used with select to wait for the operation to cancel/stop.
func (c *asyncCtl) WaitRelease() <-chan struct{} {
	return c.waitCh
}
