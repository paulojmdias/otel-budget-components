// Package clock provides an injectable clock so time driven logic is testable
// without sleeping.
package clock // import "github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/clock"

import (
	"sync"
	"time"
)

// Clock is the time source for all budget logic.
type Clock interface {
	Now() time.Time
	NewTicker(d time.Duration) Ticker
}

// Ticker delivers ticks on C.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Real returns the wall clock.
func Real() Clock { return realClock{} }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTicker(d time.Duration) Ticker { return realTicker{time.NewTicker(d)} }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

// Fake is a manually advanced clock. Tickers fire during Advance; each tick is
// delivered with a non blocking send on a channel with capacity 1, like
// time.Ticker drops ticks for slow receivers.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*fakeTicker
}

// NewFake returns a fake clock starting at t.
func NewFake(t time.Time) *Fake { return &Fake{now: t} }

// Now returns the fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Set jumps to t without firing tickers.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
}

// Advance moves time forward by d, firing tickers whose deadlines pass.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	for _, t := range f.tickers {
		if t.stopped {
			continue
		}
		for !t.next.After(f.now) {
			select {
			case t.c <- t.next:
			default:
			}
			t.next = t.next.Add(t.d)
		}
	}
}

// NewTicker returns a ticker driven by Advance.
func (f *Fake) NewTicker(d time.Duration) Ticker {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTicker{f: f, d: d, next: f.now.Add(d), c: make(chan time.Time, 1)}
	f.tickers = append(f.tickers, t)
	return t
}

type fakeTicker struct {
	f       *Fake
	d       time.Duration
	next    time.Time
	c       chan time.Time
	stopped bool
}

func (t *fakeTicker) C() <-chan time.Time { return t.c }

func (t *fakeTicker) Stop() {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	t.stopped = true
}
