// Package seriescount estimates active series per key per hour with a lock
// free HyperLogLog.
package seriescount // import "github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/seriescount"

import (
	"math"
	"math/bits"
	"sync/atomic"
)

const (
	precision = 12
	registers = 1 << precision
	words     = registers / 4
)

// Sketch is a dense HyperLogLog with precision 12 and 8 bit registers packed
// four per atomic word, so concurrent inserts need no lock.
type Sketch struct {
	w [words]atomic.Uint32
}

// Insert adds a 64 bit hash.
func (s *Sketch) Insert(h uint64) {
	idx := h >> (64 - precision)
	rest := h<<precision | 1<<(precision-1)
	rho := uint32(bits.LeadingZeros64(rest) + 1)
	word := &s.w[idx/4]
	shift := (idx % 4) * 8
	for {
		old := word.Load()
		if (old>>shift)&0xff >= rho {
			return
		}
		if word.CompareAndSwap(old, old&^(0xff<<shift)|rho<<shift) {
			return
		}
	}
}

// Estimate returns the estimated cardinality.
func (s *Sketch) Estimate() uint64 {
	var sum float64
	zeros := 0
	for i := range s.w {
		v := s.w[i].Load()
		for j := range 4 {
			r := (v >> (j * 8)) & 0xff
			if r == 0 {
				zeros++
			}
			sum += math.Ldexp(1, -int(r))
		}
	}
	m := float64(registers)
	alpha := 0.7213 / (1 + 1.079/m)
	est := alpha * m * m / sum
	if est <= 2.5*m && zeros > 0 {
		est = m * math.Log(m/float64(zeros))
	}
	return uint64(est + 0.5)
}

// Counter holds the current hour's sketch for one ledger ID and price class.
type Counter struct {
	cur atomic.Pointer[Sketch]
}

// Insert adds hashes, allocating the sketch on first use.
func (c *Counter) Insert(hs []uint64) {
	if len(hs) == 0 {
		return
	}
	s := c.cur.Load()
	if s == nil {
		s = &Sketch{}
		if !c.cur.CompareAndSwap(nil, s) {
			s = c.cur.Load()
		}
	}
	for _, h := range hs {
		s.Insert(h)
	}
}

// Rotate returns the estimate for the ending hour and starts a new sketch.
// Inserts racing with the swap may land in the old sketch and are lost,
// which is negligible for an hourly estimate.
func (c *Counter) Rotate() uint64 {
	s := c.cur.Swap(nil)
	if s == nil {
		return 0
	}
	return s.Estimate()
}
