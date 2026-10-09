// Package costmodel prices usage in integer micro units. Pure functions only.
package costmodel // import "github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/costmodel"

import (
	"math"
	"math/bits"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
)

const (
	// MicroPerUnit is the number of micro units in one currency unit.
	MicroPerUnit = 1_000_000
	// NanoPerMicro is the resolution of the sub micro remainder in Cost.
	NanoPerMicro = 1_000_000_000

	bytesPerGB      = 1_000_000_000
	itemsPerMillion = 1_000_000
	ppm             = 1_000_000
)

// ToMicro converts a config value in currency units to micro units, rounding
// to the nearest micro unit.
func ToMicro(v float64) int64 { return int64(math.Round(v * MicroPerUnit)) }

// Cost is an exact price: whole micro units plus a remainder in units of
// 1e-9 micro, in [0, NanoPerMicro). The remainder is carried by the ledger so
// many small batches do not round to zero.
type Cost struct {
	Micro int64
	Nano  int64
}

// IsZero reports whether the cost is zero.
func (c Cost) IsZero() bool { return c.Micro == 0 && c.Nano == 0 }

// Add returns c + o, normalizing the remainder.
func (c Cost) Add(o Cost) Cost {
	n := c.Nano + o.Nano
	return Cost{Micro: c.Micro + o.Micro + n/NanoPerMicro, Nano: n % NanoPerMicro}
}

// Prices are micro units per pricing unit.
type Prices struct {
	LogsGB               int64
	TracesGB             int64
	TracesMillionSpans   int64 // if > 0, used instead of TracesGB
	MetricsMillionPoints int64
	SeriesHour           int64
}

// Model prices usage per signal and price class.
type Model struct {
	Prices [budgetapi.NumPriceClasses]Prices
	// CompressionPPM multiplies bytes before pricing, in parts per million
	// (1_000_000 means 1.0).
	CompressionPPM [budgetapi.NumSignals]int64
}

// NewModel builds a model from float config values.
func NewModel(hot, cold Prices, compression [budgetapi.NumSignals]float64) Model {
	m := Model{Prices: [budgetapi.NumPriceClasses]Prices{hot, cold}}
	for i, r := range compression {
		if r <= 0 {
			r = 1
		}
		m.CompressionPPM[i] = int64(math.Round(r * ppm))
	}
	return m
}

// Cost prices one usage sample. Series hours are priced separately with
// SeriesCost at hour boundaries.
func (m *Model) Cost(sig budgetapi.Signal, class budgetapi.PriceClass, u budgetapi.Usage) Cost {
	p := &m.Prices[class]
	switch sig {
	case budgetapi.SignalLogs:
		return Scaled(m.effectiveBytes(sig, u.Bytes), p.LogsGB, bytesPerGB)
	case budgetapi.SignalTraces:
		if p.TracesMillionSpans > 0 {
			return Scaled(u.Items, p.TracesMillionSpans, itemsPerMillion)
		}
		return Scaled(m.effectiveBytes(sig, u.Bytes), p.TracesGB, bytesPerGB)
	case budgetapi.SignalMetrics:
		return Scaled(u.Items, p.MetricsMillionPoints, itemsPerMillion)
	}
	return Cost{}
}

// SeriesCost prices an hour of active series.
func (m *Model) SeriesCost(class budgetapi.PriceClass, series int64) Cost {
	return Scaled(series, m.Prices[class].SeriesHour, 1)
}

func (m *Model) effectiveBytes(sig budgetapi.Signal, b int64) int64 {
	c := m.CompressionPPM[sig]
	if c == 0 || c == ppm {
		return b
	}
	return Scaled(b, c, ppm).Micro
}

// Scaled computes qty * price / per exactly with a 128 bit intermediate. The
// quotient goes to Micro and the remainder, rescaled to 1e-9 micro, to Nano.
// Negative inputs and overflow saturate to zero and MaxInt64 respectively.
func Scaled(qty, price, per int64) Cost {
	if qty <= 0 || price <= 0 || per <= 0 {
		return Cost{}
	}
	hi, lo := bits.Mul64(uint64(qty), uint64(price))
	if hi >= uint64(per) {
		return Cost{Micro: math.MaxInt64}
	}
	q, r := bits.Div64(hi, lo, uint64(per))
	if q > math.MaxInt64 {
		return Cost{Micro: math.MaxInt64}
	}
	// r < per; rescale the remainder to 1e-9 micro with 128 bits as well.
	rh, rl := bits.Mul64(r, NanoPerMicro)
	nano, _ := bits.Div64(rh, rl, uint64(per))
	return Cost{Micro: int64(q), Nano: int64(nano)}
}
