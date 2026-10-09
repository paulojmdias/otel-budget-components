package decision

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/budgetapi"
	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/internal/clock"
)

const (
	unit     = int64(1_000_000)
	period   = time.Hour
	interval = 5 * time.Second
	// budget such that a rate of 1 unit per second is exactly burn 1.0
	budget = 3600 * unit
)

var t0 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

func testConfig() Config {
	return Config{
		Tiers: []Tier{
			{Threshold: 1.0, Actions: budgetapi.Action{DropBelowSeverity: plog.SeverityNumberInfo, SampleRatio: 1}},
			{Threshold: 2.0, Actions: budgetapi.Action{Divert: true, SampleRatio: 1}},
		},
		Short: time.Minute, Long: 6 * time.Minute, Interval: interval,
		WarmupCoverage: 0.8, HardCap: HardCapHighest,
		HysteresisRatio: 0.8, MinDwell: 3 * time.Minute,
		StaleAfter: time.Minute, FailOpenAfter: 5 * time.Minute,
	}
}

// sim drives an engine with a spend rate curve on a fake clock.
type sim struct {
	e     *Engine
	clk   *clock.Fake
	cum   int64
	spend int64
	asOf  time.Time
	// syncDown freezes asOf, as if the sync layer stopped answering.
	syncDown bool
	exempt   bool
	budget   int64
	tiers    []int // published tier per step
}

func newSim(cfg Config) *sim {
	return &sim{e: New(cfg), clk: clock.NewFake(t0), asOf: t0, budget: budget}
}

// run advances d with rate(elapsed) units per second.
func (s *sim) run(d time.Duration, rate func(time.Duration) float64) LedgerResult {
	var last LedgerResult
	for el := time.Duration(0); el < d; el += interval {
		s.clk.Advance(interval)
		inc := int64(rate(el) * float64(unit) * interval.Seconds())
		s.cum += inc
		s.spend += inc
		if !s.syncDown {
			s.asOf = s.clk.Now()
		}
		res := s.e.Step(Input{
			Now: s.clk.Now(), AsOf: s.asOf, PeriodLength: period,
			Ledgers: map[string]LedgerInput{"k": {Cumulative: s.cum, Spend: s.spend, Budget: s.budget, Exempt: s.exempt}},
		})
		last = res.Ledgers["k"]
		s.tiers = append(s.tiers, last.Tier)
	}
	return last
}

func constant(r float64) func(time.Duration) float64 { return func(time.Duration) float64 { return r } }

func maxTier(ts []int) int {
	m := 0
	for _, t := range ts {
		m = max(m, t)
	}
	return m
}

func changes(ts []int) int {
	n := 0
	for i := 1; i < len(ts); i++ {
		if ts[i] != ts[i-1] {
			n++
		}
	}
	return n
}

func TestScenarios(t *testing.T) {
	cases := []struct {
		name  string
		cfg   func(*Config)
		setup func(*sim)
		check func(t *testing.T, s *sim)
	}{
		{
			name: "steady under budget",
			setup: func(s *sim) {
				s.run(30*time.Minute, constant(0.5))
			},
			check: func(t *testing.T, s *sim) {
				assert.Equal(t, 0, maxTier(s.tiers))
			},
		},
		{
			name: "sudden spike",
			setup: func(s *sim) {
				s.run(10*time.Minute, constant(0.5))
				r := s.run(time.Minute, constant(3))
				assert.Equal(t, 0, r.Tier, "long window still below 1 after one minute")
				s.run(9*time.Minute, constant(3))
			},
			check: func(t *testing.T, s *sim) {
				assert.Equal(t, 2, s.tiers[len(s.tiers)-1])
				assert.Equal(t, 2, changes(s.tiers), "0 to 1 to 2, no flapping")
			},
		},
		{
			name: "slow creep",
			setup: func(s *sim) {
				s.run(40*time.Minute, func(el time.Duration) float64 { return 0.5 + el.Minutes()/40 })
			},
			check: func(t *testing.T, s *sim) {
				assert.Equal(t, 1, s.tiers[len(s.tiers)-1])
				assert.Equal(t, 1, maxTier(s.tiers))
			},
		},
		{
			name: "oscillation around a threshold",
			setup: func(s *sim) {
				s.run(10*time.Minute, constant(0.5))
				s.run(40*time.Minute, func(el time.Duration) float64 {
					if int(el.Minutes())%2 == 0 {
						return 1.4
					}
					return 0.8
				})
			},
			check: func(t *testing.T, s *sim) {
				assert.LessOrEqual(t, changes(s.tiers), 1, "hysteresis prevents flapping")
				assert.Equal(t, 1, s.tiers[len(s.tiers)-1])
			},
		},
		{
			name: "hard cap highest",
			setup: func(s *sim) {
				s.spend = budget
				s.run(2*time.Minute, constant(0.1))
			},
			check: func(t *testing.T, s *sim) {
				assert.Equal(t, 1, s.tiers[len(s.tiers)-1], "highest tier without divert")
				assert.Equal(t, 1, s.e.hardCapTier)
			},
		},
		{
			name: "hard cap divert",
			cfg:  func(c *Config) { c.HardCap = HardCapDivert },
			setup: func(s *sim) {
				s.spend = budget
				s.run(time.Minute, constant(0.1))
			},
			check: func(t *testing.T, s *sim) {
				assert.Equal(t, 2, s.tiers[len(s.tiers)-1])
			},
		},
		{
			name: "hard cap off",
			cfg:  func(c *Config) { c.HardCap = HardCapOff },
			setup: func(s *sim) {
				s.spend = budget
				s.run(time.Minute, constant(0.1))
			},
			check: func(t *testing.T, s *sim) {
				assert.Equal(t, 0, maxTier(s.tiers))
				assert.Equal(t, 0, s.e.hardCapTier)
			},
		},
		{
			name: "warm up after restart",
			setup: func(s *sim) {
				s.run(45*time.Second, constant(5))
				assert.Equal(t, 0, maxTier(s.tiers), "no escalation before 80% of the short window")
				s.run(10*time.Second, constant(5))
			},
			check: func(t *testing.T, s *sim) {
				assert.Equal(t, 2, s.tiers[len(s.tiers)-1])
			},
		},
		{
			name: "exempt",
			setup: func(s *sim) {
				s.run(10*time.Minute, constant(3))
				s.exempt = true
				r := s.run(interval, constant(3))
				assert.True(t, r.Exempt)
				assert.Equal(t, 2, r.Internal, "engine keeps tracking while exempt")
			},
			check: func(t *testing.T, s *sim) {
				assert.Equal(t, 0, s.tiers[len(s.tiers)-1])
			},
		},
		{
			name: "stale freeze",
			setup: func(s *sim) {
				s.run(10*time.Minute, constant(0.5))
				s.syncDown = true
				s.run(4*time.Minute, constant(5))
			},
			check: func(t *testing.T, s *sim) {
				assert.Equal(t, 0, maxTier(s.tiers), "never escalate on stale data")
			},
		},
		{
			name: "stale allows de-escalation",
			cfg:  func(c *Config) { c.MinDwell = 0 },
			setup: func(s *sim) {
				s.run(10*time.Minute, constant(1.5))
				require.Equal(t, 1, s.tiers[len(s.tiers)-1])
				s.run(10*time.Minute, constant(0.1))
				s.syncDown = true
				s.run(2*time.Minute, constant(5))
			},
			check: func(t *testing.T, s *sim) {
				assert.Equal(t, 0, s.tiers[len(s.tiers)-1])
			},
		},
		{
			// storage came back empty and every replica restores its own
			// contribution a read apart: the fleet total drops, then climbs
			// back in steps that are not spend
			name: "staggered restore after storage data loss",
			cfg:  func(c *Config) { c.Settle = 15 * time.Second },
			setup: func(s *sim) {
				s.run(10*time.Minute, constant(0.5))
				s.syncDown = true
				s.run(2*time.Minute, constant(0.5))
				s.syncDown = false
				full := s.cum
				s.cum = full / 3
				s.run(interval, constant(0.5))
				s.cum += full / 3
				s.run(interval, constant(0.5))
				s.cum += full - 2*(full/3)
				s.run(5*time.Minute, constant(0.5))
			},
			check: func(t *testing.T, s *sim) {
				assert.Equal(t, 0, maxTier(s.tiers))
				assert.Equal(t, 1, s.e.Resets())
			},
		},
		{
			name: "tier holds while windows are cold after a reset",
			cfg:  func(c *Config) { c.Settle = 30 * time.Second; c.MinDwell = 0 },
			setup: func(s *sim) {
				s.run(10*time.Minute, constant(1.5))
				require.Equal(t, 1, s.tiers[len(s.tiers)-1])
				s.cum /= 2
				s.run(time.Minute, constant(1.5))
			},
			check: func(t *testing.T, s *sim) {
				assert.Equal(t, 1, s.tiers[len(s.tiers)-1])
				assert.Equal(t, 1, changes(s.tiers), "never dropped to 0 on cold windows")
			},
		},
		{
			// replicas flush the spend they held during a sync outage at
			// different times after it
			name: "staggered catch-up after a sync gap",
			cfg:  func(c *Config) { c.Settle = 15 * time.Second; c.Long = 90 * time.Second },
			setup: func(s *sim) {
				s.run(10*time.Minute, constant(0.5))
				s.syncDown = true
				s.run(2*time.Minute, constant(0))
				s.syncDown = false
				backlog := int64(0.5 * 120 * float64(unit))
				s.run(interval, constant(0.5))
				s.cum += backlog / 2
				s.run(interval, constant(0.5))
				s.cum += backlog / 2
				s.run(5*time.Minute, constant(0.5))
			},
			check: func(t *testing.T, s *sim) {
				assert.Equal(t, 0, maxTier(s.tiers))
			},
		},
		{
			name: "fail open",
			setup: func(s *sim) {
				s.run(10*time.Minute, constant(3))
				require.Equal(t, 2, s.tiers[len(s.tiers)-1])
				s.syncDown = true
				s.run(6*time.Minute, constant(3))
			},
			check: func(t *testing.T, s *sim) {
				assert.Equal(t, 0, s.tiers[len(s.tiers)-1])
			},
		},
		{
			name: "no budget never enforces",
			setup: func(s *sim) {
				s.budget = 0
				s.run(5*time.Minute, constant(100))
			},
			check: func(t *testing.T, s *sim) {
				assert.Equal(t, 0, maxTier(s.tiers))
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := testConfig()
			if c.cfg != nil {
				c.cfg(&cfg)
			}
			s := newSim(cfg)
			c.setup(s)
			c.check(t, s)
		})
	}
}

func TestHysteresisReducesTransitions(t *testing.T) {
	osc := func(el time.Duration) float64 {
		if int(el.Minutes())%2 == 0 {
			return 1.6
		}
		return 0.5
	}
	base := testConfig()
	base.Long = 75 * time.Second
	with := newSim(base)
	with.run(40*time.Minute, osc)
	cfg := base
	cfg.HysteresisRatio, cfg.MinDwell = 1, 0
	without := newSim(cfg)
	without.run(40*time.Minute, osc)
	t.Logf("transitions with hysteresis %d, without %d", with.e.Transitions(), without.e.Transitions())
	assert.Less(t, with.e.Transitions(), without.e.Transitions())
}

func TestStepDropsIdleLedgers(t *testing.T) {
	e := New(testConfig())
	e.Step(Input{Now: t0, AsOf: t0, PeriodLength: period, Ledgers: map[string]LedgerInput{"a": {Budget: 1}}})
	assert.Len(t, e.states, 1)
	later := t0.Add(7 * time.Minute)
	res := e.Step(Input{Now: later, AsOf: later.Add(time.Second), PeriodLength: period})
	assert.Empty(t, e.states)
	assert.Zero(t, res.SyncAge, "future as-of clamps to zero age")
	assert.True(t, res.Healthy)
}

func TestCombine(t *testing.T) {
	cfg := testConfig()
	cfg.Tiers[0].Actions.DropMetrics = []string{"go_*"}
	require.NoError(t, cfg.Tiers[0].Actions.Compile())
	e := New(cfg)
	assert.Equal(t, plog.SeverityNumber(0), e.Actions(9).DropBelowSeverity)
	d := e.Combine([]LedgerResult{{Tier: 1}, {Tier: 2}}, false, true, t0)
	assert.Equal(t, 2, d.Tier)
	assert.True(t, d.Actions.Divert)

	d = e.Combine([]LedgerResult{{Tier: 1}, {Tier: 1}}, true, true, t0)
	assert.Equal(t, 1, d.Tier)
	assert.True(t, d.Stale)
	assert.True(t, d.Actions.DropsMetric("go_gc"), "compiled globs survive combine")

	d = e.Combine([]LedgerResult{{Tier: 2}, {Tier: 0, Exempt: true}}, false, true, t0)
	assert.True(t, d.Exempt)
	assert.Equal(t, 0, d.Tier)

	d = e.Combine([]LedgerResult{{Tier: 2}}, false, false, t0)
	assert.Equal(t, 0, d.Tier, "fail open")
	assert.InDelta(t, 1.0, d.Actions.SampleRatio, 0)
}

// A noisy key whose data is mostly dropped at tier 1: accepted burn falls far
// below the threshold while offered burn stays high. Deciding on accepted
// spend alone makes it bounce between tiers; with offered spend it holds tier 1
// until the traffic itself drops.
func TestDroppingDataDoesNotFlap(t *testing.T) {
	run := func(withOffered bool) (int, int) {
		e := New(testConfig())
		clk := clock.NewFake(t0)
		var acc, off int64
		tier := 0
		var tiers []int
		step := func(offeredRate float64) {
			clk.Advance(interval)
			accepted := offeredRate
			if tier >= 1 {
				accepted = offeredRate / 10 // tier 1 drops 90% (DEBUG)
			}
			acc += int64(accepted * float64(unit) * interval.Seconds())
			off += int64(offeredRate * float64(unit) * interval.Seconds())
			li := LedgerInput{Cumulative: acc, Spend: acc, Budget: budget}
			if withOffered {
				li.OfferedCumulative = off
			}
			res := e.Step(Input{Now: clk.Now(), AsOf: clk.Now(), PeriodLength: period, Ledgers: map[string]LedgerInput{"k": li}})
			tier = res.Ledgers["k"].Tier
			tiers = append(tiers, tier)
		}
		for range int(2 * time.Hour / interval) {
			step(1.5) // noisy: 1.5x the sustainable rate, 90% of it DEBUG
		}
		noisy := changes(tiers)
		if withOffered {
			require.Equal(t, 1, tier, "escalated while noisy")
		}
		for range int(time.Hour / interval) {
			step(0.3) // the noise stops
		}
		return noisy, tier
	}

	flaps, _ := run(false)
	assert.Greater(t, flaps, 4, "without offered spend the key flaps")

	steady, final := run(true)
	assert.Equal(t, 1, steady, "with offered spend: one escalation, no flapping")
	assert.Equal(t, 0, final, "de-escalates once the traffic itself drops")
	t.Logf("tier changes over 2h noisy: %d without offered spend, %d with", flaps, steady)
}

// Storage that loses its data makes fleet totals go back. Burn rates must
// not turn negative, and a still noisy key escalates again once the short
// window has warmed up.
func TestTotalsGoingBackRestartWindows(t *testing.T) {
	e := New(testConfig())
	clk := clock.NewFake(t0)
	var cum int64
	step := func(rate float64) LedgerResult {
		clk.Advance(interval)
		cum += int64(rate * float64(unit) * interval.Seconds())
		return e.Step(Input{Now: clk.Now(), AsOf: clk.Now(), PeriodLength: period, Ledgers: map[string]LedgerInput{"k": {Cumulative: cum, Spend: cum, Budget: budget, OfferedCumulative: cum}}}).Ledgers["k"]
	}
	for range 120 {
		step(1.5)
	}
	cum = 0 // storage lost everything
	r := step(1.5)
	assert.GreaterOrEqual(t, r.BurnShort, 0.0)
	assert.GreaterOrEqual(t, r.BurnLong, 0.0)
	assert.Equal(t, 1, e.Resets())
	for range 12 { // one short window
		r = step(1.5)
	}
	assert.Equal(t, 1, r.Tier, "still noisy, so still escalated")
}
