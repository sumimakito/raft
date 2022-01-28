package raft

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"path"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"time"
)

func Ptr[T any](v T) *T {
	return &v
}

func Must1(err error) {
	if err != nil {
		log.Panic(err)
	}
}

func Must2[T any](result T, err error) T {
	if err != nil {
		log.Panic(err)
	}
	return result
}

func Zero(v interface{}) {
	reflectValue := reflect.ValueOf(v).Elem()
	reflectValue.Set(reflect.Zero(reflectValue.Type()))
}

type TickTrigger struct {
	t       int64
	ticks   int64
	timeout time.Duration
	fn      func()
}

func NewTickTrigger(ticks int64, timeout time.Duration, fn func()) *TickTrigger {
	if ticks <= 0 {
		panic("ticks should be a positive integer")
	}
	return &TickTrigger{ticks: ticks, timeout: timeout, fn: fn}
}

func (t *TickTrigger) Tick() {
	if atomic.AddInt64(&t.t, 1)%t.ticks == 0 {
		t.fn()
		atomic.AddInt64(&t.t, -t.ticks)
	}
}

type BufferedReadCloser struct {
	reader *bufio.Reader
	closer io.Closer
}

func NewBufferedReadCloser(r io.ReadCloser) *BufferedReadCloser {
	return &BufferedReadCloser{
		reader: bufio.NewReader(r),
		closer: r,
	}
}

func (r *BufferedReadCloser) Close() error {
	return r.closer.Close()
}

func (r *BufferedReadCloser) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

type BufferedWriteCloser struct {
	writer *bufio.Writer
	closer io.Closer
}

func NewBufferedWriteCloser(w io.WriteCloser) *BufferedWriteCloser {
	return &BufferedWriteCloser{
		writer: bufio.NewWriter(w),
		closer: w,
	}
}

func (w *BufferedWriteCloser) Close() error {
	return w.closer.Close()
}

func (w *BufferedWriteCloser) Flush() error {
	return w.writer.Flush()
}

func (w *BufferedWriteCloser) Write(p []byte) (n int, err error) {
	return w.writer.Write(p)
}

// CappedSlice is a ring buffer like slice which holds a maximum of `cap` items.
// When the slice overflows, older items will be evicted first.
type CappedSlice struct {
	cap   int
	tail  int
	slots []interface{}
}

func NewCappedSlice(cap int) *CappedSlice {
	if cap <= 0 {
		log.Panic("capacity must be a positive integer")
	}
	return &CappedSlice{
		cap:   cap,
		tail:  0,
		slots: make([]interface{}, 0, cap),
	}
}

func (c *CappedSlice) Push(v interface{}) {
	// Check if the slice is fully expanded
	if len(c.slots) < c.cap {
		c.slots = append(c.slots, v)
		c.tail = len(c.slots) - 1
		return
	}
	c.tail++
	if c.tail >= c.cap {
		c.tail = 0
	}
	c.slots[c.tail] = v
}

func (c *CappedSlice) Range(fn func(i int, v interface{}) (cont bool)) {
	n := c.cap
	if len(c.slots) < c.cap {
		n = len(c.slots)
	}
	for i := 0; i < n; i++ {
		ki := (c.tail + 1 + i) % n
		if !fn(ki, c.slots[ki]) {
			return
		}
	}
}

type StreamAverage struct {
	avg float64
	n   int
}

func (a *StreamAverage) Avg() float64 {
	return a.avg
}

func (a *StreamAverage) N() int {
	return a.n
}

func (a *StreamAverage) Push(v float64) float64 {
	a.avg = (a.avg*float64(a.n) + v) / float64(a.n+1)
	a.n++
	return a.avg
}

type StreamMinMaxInt64 struct {
	min int64
	max int64
	n   int
}

func (a *StreamMinMaxInt64) Cap(cap int) int64 {
	return a.min
}

func (a *StreamMinMaxInt64) Min() int64 {
	return a.min
}

func (a *StreamMinMaxInt64) Max() int64 {
	return a.max
}

func (a *StreamMinMaxInt64) N() int {
	return a.n
}

func (a *StreamMinMaxInt64) Push(v int64) (min, max int64) {
	if v > a.max {
		a.max = v
	}
	if v < a.min {
		a.min = v
	}
	a.n++
	return a.min, a.max
}

type StreamMinMaxFloat64 struct {
	min float64
	max float64
	n   int
}

func (a *StreamMinMaxFloat64) Min() float64 {
	return a.min
}

func (a *StreamMinMaxFloat64) Max() float64 {
	return a.max
}

func (a *StreamMinMaxFloat64) N() int {
	return a.n
}

func (a *StreamMinMaxFloat64) Push(v float64) (min, max float64) {
	if v > a.max {
		a.max = v
	}
	if v < a.min {
		a.min = v
	}
	a.n++
	return a.min, a.max
}

func EncodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func DecodeUint64(b []byte) uint64 {
	if len(b) >= 8 {
		return binary.BigEndian.Uint64(b)
	}
	alloc := make([]byte, 8)
	copy(alloc[len(alloc)-1-len(b):], b)
	return binary.BigEndian.Uint64(alloc)
}

func PathJoin(prefix, suffix string) string {
	if path.IsAbs(suffix) {
		return suffix
	}
	return filepath.Join(prefix, suffix)
}
