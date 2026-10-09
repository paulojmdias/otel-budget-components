// Package ledger holds lock free spend counters, period math, and the burn
// rate ring buffers.
package ledger // import "github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/ledger"

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/costmodel"
)

// Spend is spend in micro units per signal and price class.
type Spend [budgetapi.NumSignals][budgetapi.NumPriceClasses]int64

// Total sums all signals and classes.
func (s *Spend) Total() int64 {
	var t int64
	for i := range s {
		for j := range s[i] {
			t += s[i][j]
		}
	}
	return t
}

// Add adds o into s.
func (s *Spend) Add(o *Spend) {
	for i := range s {
		for j := range s[i] {
			s[i][j] += o[i][j]
		}
	}
}

// Counter is an atomic micro unit counter that carries the sub micro
// remainder of each Cost, so small batches are never rounded away.
type Counter struct {
	micro atomic.Int64
	nano  atomic.Int64
}

// Add adds c and returns the whole micro units added, including any carry,
// so callers can forward exactly that amount to the sync layer.
func (k *Counter) Add(c costmodel.Cost) int64 {
	added := c.Micro
	if c.Nano != 0 {
		for {
			old := k.nano.Load()
			n := old + c.Nano
			if k.nano.CompareAndSwap(old, n%costmodel.NanoPerMicro) {
				added += n / costmodel.NanoPerMicro
				break
			}
		}
	}
	if added != 0 {
		k.micro.Add(added)
	}
	return added
}

// AddMicro adds whole micro units.
func (k *Counter) AddMicro(m int64) { k.micro.Add(m) }

// Load returns whole micro units.
func (k *Counter) Load() int64 { return k.micro.Load() }

// Swap sets the counter to v and returns the previous whole micro units.
func (k *Counter) Swap(v int64) int64 { return k.micro.Swap(v) }

// Cell is the counter set of one ledger ID.
type Cell struct {
	c [budgetapi.NumSignals][budgetapi.NumPriceClasses]Counter
}

// Counter returns the counter for a signal and class.
func (c *Cell) Counter(sig budgetapi.Signal, class budgetapi.PriceClass) *Counter {
	return &c.c[sig][class]
}

// Snapshot loads all counters.
func (c *Cell) Snapshot() Spend {
	var s Spend
	for i := range c.c {
		for j := range c.c[i] {
			s[i][j] = c.c[i][j].Load()
		}
	}
	return s
}

// Table holds the cells of one period.
type Table struct {
	ID    string
	cells sync.Map // ledger ID -> *Cell
	n     atomic.Int64
}

// NewTable returns an empty table for a period.
func NewTable(periodID string) *Table { return &Table{ID: periodID} }

// Cell returns the cell for a ledger ID, creating it if needed.
func (t *Table) Cell(id string) *Cell {
	if v, ok := t.cells.Load(id); ok {
		return v.(*Cell)
	}
	v, loaded := t.cells.LoadOrStore(id, &Cell{})
	if !loaded {
		t.n.Add(1)
	}
	return v.(*Cell)
}

// Existing returns the cell for a ledger ID without creating it.
func (t *Table) Existing(id string) (*Cell, bool) {
	v, ok := t.cells.Load(id)
	if !ok {
		return nil, false
	}
	return v.(*Cell), true
}

// Len returns the number of cells.
func (t *Table) Len() int64 { return t.n.Load() }

// Range iterates over cells.
func (t *Table) Range(f func(id string, c *Cell)) {
	t.cells.Range(func(k, v any) bool {
		f(k.(string), v.(*Cell))
		return true
	})
}

// Snapshot loads every cell.
func (t *Table) Snapshot() map[string]Spend {
	out := make(map[string]Spend)
	t.Range(func(id string, c *Cell) { out[id] = c.Snapshot() })
	return out
}

// Period is the budget window: a calendar month in UTC or a fixed duration.
type Period struct {
	Monthly bool
	Every   time.Duration
}

// Bounds returns the start and end of the period containing t.
func (p Period) Bounds(t time.Time) (time.Time, time.Time) {
	t = t.UTC()
	if p.Monthly || p.Every <= 0 {
		start := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
		return start, start.AddDate(0, 1, 0)
	}
	start := time.Unix(0, 0).UTC().Add(time.Duration(t.UnixNano()/int64(p.Every)) * p.Every)
	return start, start.Add(p.Every)
}

// ID returns the period ID: "2026-10" for monthly, else the window start as
// Unix seconds.
func (p Period) ID(t time.Time) string {
	start, _ := p.Bounds(t)
	if p.Monthly || p.Every <= 0 {
		return start.Format("2006-01")
	}
	return strconv.FormatInt(start.Unix(), 10)
}

// Length returns the duration of the period containing t.
func (p Period) Length(t time.Time) time.Duration {
	s, e := p.Bounds(t)
	return e.Sub(s)
}

// RingSlots is how many samples a window ring keeps. Deltas start at most
// window/RingSlots before the window start, so burn rates stay exact over the
// span they cover while memory per ledger stays constant whatever the window
// and decision interval.
const RingSlots = 32

// Sample is one fleet total observation, 16 bytes.
type Sample struct {
	At    int64 // Unix nanoseconds
	Total int64
}

// Ring keeps samples at least window/RingSlots apart, oldest first, enough
// to cover one window. Not concurrency safe; owned by the decision loop.
type Ring struct {
	buf     []Sample
	head    int // index of the oldest sample
	n       int
	spacing int64
}

// NewRing returns a ring for deltas over window.
func NewRing(window time.Duration) *Ring {
	return &Ring{buf: make([]Sample, RingSlots+2), spacing: int64(window) / RingSlots}
}

// Push records the total at a time, unless the newest sample is closer than
// the spacing. Evicts the oldest sample when full.
func (r *Ring) Push(at time.Time, total int64) {
	ns := at.UnixNano()
	if r.n > 0 && ns-r.at(r.n-1).At < r.spacing {
		return
	}
	if r.n < len(r.buf) {
		r.buf[(r.head+r.n)%len(r.buf)] = Sample{At: ns, Total: total}
		r.n++
		return
	}
	r.buf[r.head] = Sample{At: ns, Total: total}
	r.head = (r.head + 1) % len(r.buf)
}

// Len returns the number of samples.
func (r *Ring) Len() int { return r.n }

func (r *Ring) at(i int) Sample { return r.buf[(r.head+i)%len(r.buf)] }

// Delta returns how much total grew over the last w ending at (at, total),
// and the duration actually covered (at most w).
func (r *Ring) Delta(at time.Time, total int64, w time.Duration) (int64, time.Duration) {
	ns := at.UnixNano()
	from := ns - int64(w)
	for i := range r.n {
		if s := r.at(i); s.At >= from && s.At <= ns {
			return total - s.Total, time.Duration(ns - s.At)
		}
	}
	return 0, 0
}
