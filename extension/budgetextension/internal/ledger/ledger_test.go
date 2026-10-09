package ledger

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/costmodel"
)

func TestCounterCarry(t *testing.T) {
	var c Counter
	var forwarded int64
	for range 2500 {
		forwarded += c.Add(costmodel.Cost{Nano: 400_000_000})
	}
	assert.Equal(t, int64(1000), c.Load())
	assert.Equal(t, int64(1000), forwarded)
	assert.Equal(t, int64(5), c.Add(costmodel.Cost{Micro: 5}))
	c.AddMicro(2)
	assert.Equal(t, int64(1007), c.Swap(0))
	assert.Equal(t, int64(0), c.Load())
}

func TestCounterConcurrent(t *testing.T) {
	var c Counter
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 10000 {
				c.Add(costmodel.Cost{Micro: 1, Nano: 250_000_000})
			}
		})
	}
	wg.Wait()
	assert.Equal(t, int64(100000), c.Load())
}

func TestTable(t *testing.T) {
	tb := NewTable("2026-10")
	c := tb.Cell("a")
	assert.Same(t, c, tb.Cell("a"))
	c.Counter(budgetapi.SignalLogs, budgetapi.PriceHot).AddMicro(5)
	c.Counter(budgetapi.SignalMetrics, budgetapi.PriceCold).AddMicro(7)
	tb.Cell("b")
	assert.Equal(t, int64(2), tb.Len())
	snap := tb.Snapshot()
	a := snap["a"]
	assert.Equal(t, int64(12), a.Total())
	b := snap["b"]
	b.Add(&a)
	assert.Equal(t, int64(12), b.Total())
	_, ok := tb.Existing("zzz")
	assert.False(t, ok)
	_, ok = tb.Existing("a")
	assert.True(t, ok)
}

func TestPeriod(t *testing.T) {
	ts := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	m := Period{Monthly: true}
	assert.Equal(t, "2026-10", m.ID(ts))
	assert.Equal(t, 31*24*time.Hour, m.Length(ts))
	s, e := m.Bounds(ts)
	assert.Equal(t, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), s)
	assert.Equal(t, time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), e)
	assert.Equal(t, "2027-01", m.ID(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)))

	h := Period{Every: time.Hour}
	assert.Equal(t, "1792065600", h.ID(ts))
	assert.Equal(t, time.Hour, h.Length(ts))
	s, _ = h.Bounds(ts.Add(59 * time.Minute))
	assert.Equal(t, ts, s)
}

func TestRing(t *testing.T) {
	r := NewRing(320 * time.Second) // spacing 10s
	base := time.Unix(1000, 0)
	d, cov := r.Delta(base, 0, time.Minute)
	assert.Zero(t, d)
	assert.Zero(t, cov)
	// one push per second: only every 10th is kept
	for i := range 101 {
		r.Push(base.Add(time.Duration(i)*time.Second), int64(i*100))
	}
	assert.Equal(t, 11, r.Len())
	now := base.Add(100 * time.Second)
	d, cov = r.Delta(now, 10000, 30*time.Second)
	assert.Equal(t, int64(3000), d)
	assert.Equal(t, 30*time.Second, cov)
	d, cov = r.Delta(now, 10000, time.Hour)
	assert.Equal(t, int64(10000), d, "partial coverage starts at the oldest sample")
	assert.Equal(t, 100*time.Second, cov)

	// memory is bounded whatever the number of pushes
	for i := range 10000 {
		r.Push(base.Add(time.Duration(200+i*10)*time.Second), int64(i))
	}
	assert.Equal(t, RingSlots+2, r.Len())
	assert.Len(t, r.buf, RingSlots+2)
}
