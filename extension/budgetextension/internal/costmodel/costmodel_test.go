package costmodel

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
)

func TestToMicro(t *testing.T) {
	assert.Equal(t, int64(400_000), ToMicro(0.40))
	assert.Equal(t, int64(10), ToMicro(0.00001))
	assert.Equal(t, int64(1), ToMicro(0.0000005), "half rounds away from zero")
	assert.Equal(t, int64(0), ToMicro(0.0000004))
	assert.Equal(t, int64(120_500_000), ToMicro(120.5))
	assert.Equal(t, int64(300_000), ToMicro(0.3), "0.3 is not exact in binary, rounding fixes it")
}

func TestScaled(t *testing.T) {
	cases := []struct {
		qty, price, per int64
		want            Cost
	}{
		{1_000_000_000, 400_000, bytesPerGB, Cost{400_000, 0}},
		{1000, 400_000, bytesPerGB, Cost{0, 400_000_000}}, // 0.4 micro
		{2500, 400_000, bytesPerGB, Cost{1, 0}},
		{3, 1, 2, Cost{1, 500_000_000}},
		{1, 1, 3, Cost{0, 333_333_333}},
		{0, 5, 1, Cost{}},
		{-1, 5, 1, Cost{}},
		{5, 0, 1, Cost{}},
		{5, 5, 0, Cost{}},
		{math.MaxInt64, math.MaxInt64, 1, Cost{Micro: math.MaxInt64}},
		{math.MaxInt64, 4, 2, Cost{Micro: math.MaxInt64}},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, Scaled(c.qty, c.price, c.per), "%d*%d/%d", c.qty, c.price, c.per)
	}
}

func TestCostAdd(t *testing.T) {
	c := Cost{0, 600_000_000}.Add(Cost{1, 600_000_000})
	assert.Equal(t, Cost{2, 200_000_000}, c)
	assert.True(t, Cost{}.IsZero())
	assert.False(t, c.IsZero())
}

func TestModel(t *testing.T) {
	hot := Prices{LogsGB: ToMicro(0.40), TracesGB: ToMicro(0.30), MetricsMillionPoints: ToMicro(0.10), SeriesHour: ToMicro(0.00001)}
	cold := Prices{LogsGB: ToMicro(0.02), TracesMillionSpans: ToMicro(1)}
	m := NewModel(hot, cold, [3]float64{1, 0.5, 0})
	assert.Equal(t, int64(1_000_000), m.CompressionPPM[budgetapi.SignalMetrics], "0 defaults to 1.0")

	gb := budgetapi.Usage{Bytes: 1_000_000_000, Items: 1_000_000}
	assert.Equal(t, Cost{400_000, 0}, m.Cost(budgetapi.SignalLogs, budgetapi.PriceHot, gb))
	assert.Equal(t, Cost{20_000, 0}, m.Cost(budgetapi.SignalLogs, budgetapi.PriceCold, gb))
	assert.Equal(t, Cost{150_000, 0}, m.Cost(budgetapi.SignalTraces, budgetapi.PriceHot, gb), "compression 0.5")
	assert.Equal(t, Cost{1_000_000, 0}, m.Cost(budgetapi.SignalTraces, budgetapi.PriceCold, gb), "per million spans")
	assert.Equal(t, Cost{100_000, 0}, m.Cost(budgetapi.SignalMetrics, budgetapi.PriceHot, gb))
	assert.Equal(t, Cost{}, m.Cost(budgetapi.Signal(9), budgetapi.PriceHot, gb))
	assert.Equal(t, Cost{40_000, 0}, m.SeriesCost(budgetapi.PriceHot, 4000))

	// many small batches add up exactly with the remainder
	var total Cost
	for range 2500 {
		total = total.Add(m.Cost(budgetapi.SignalLogs, budgetapi.PriceHot, budgetapi.Usage{Bytes: 1}))
	}
	assert.Equal(t, Cost{1, 0}, total)
}
