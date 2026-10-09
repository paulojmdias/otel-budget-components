// Package decision computes burn rates and tiers per ledger ID. It is driven
// by the extension's background loop and is not concurrency safe.
package decision // import "github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/decision"

import (
	"time"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/ledger"
)

// HardCap selects what happens when period to date spend reaches the budget.
type HardCap uint8

const (
	HardCapOff HardCap = iota
	HardCapHighest
	HardCapDivert
)

// Tier is one enforcement level.
type Tier struct {
	Threshold float64
	Actions   budgetapi.Action
}

// Config drives the engine.
type Config struct {
	Tiers           []Tier
	Short, Long     time.Duration
	Interval        time.Duration
	WarmupCoverage  float64
	HardCap         HardCap
	HysteresisRatio float64
	MinDwell        time.Duration
	StaleAfter      time.Duration
	FailOpenAfter   time.Duration
	// Settle is how long fleet totals take to converge after a sync gap or
	// a reset: replicas flush their backlogs or restore lost counters one
	// by one as they reconnect, so the windows restart at every sample
	// until then instead of reading the catch-up as spend.
	Settle time.Duration
}

// LedgerInput is the state of one ledger ID at a step.
type LedgerInput struct {
	// Cumulative is a monotonic fleet total across periods, used for burn
	// rates so a period rollover does not reset the windows.
	Cumulative int64
	// Spend is the period to date fleet total, used for the hard cap.
	Spend int64
	// Budget is the period budget in micro units, already divided by N in
	// share mode. Zero or less disables enforcement for the ledger.
	Budget int64
	Exempt bool
	// OfferedCumulative is the monotonic fleet total of what processors were
	// offered, priced at hot prices, before any action. Zero when unknown.
	OfferedCumulative int64
}

// Input is everything one step needs.
type Input struct {
	Now          time.Time
	AsOf         time.Time // when the fleet totals were observed
	PeriodLength time.Duration
	Ledgers      map[string]LedgerInput
}

// LedgerResult is the outcome for one ledger ID.
type LedgerResult struct {
	Tier      int // published tier (0 when exempt or failed open)
	Internal  int // tier tracked by the engine
	BurnShort float64
	BurnLong  float64
	// OfferedBurnLong is the long window burn of offered usage: what the key
	// would spend without enforcement.
	OfferedBurnLong float64
	Spend           int64
	Budget          int64
	Warm            bool
	Exempt          bool
}

// Result is the outcome of one step.
type Result struct {
	Ledgers map[string]LedgerResult
	Stale   bool
	Healthy bool
	SyncAge time.Duration
}

type state struct {
	short      *ledger.Ring // accepted spend, short window
	long       *ledger.Ring // accepted spend, long window
	offered    *ledger.Ring // offered spend, long window
	lastAsOf   time.Time
	lastCum    int64 // totals observed at lastAsOf
	lastOff    int64
	tier       int
	belowSince time.Time
	lastSeen   time.Time
	// settleUntil ends the catch-up after a sync gap or a reset
	settleUntil time.Time
}

// Engine keeps per ledger state between steps.
type Engine struct {
	cfg         Config
	hardCapTier int
	states      map[string]*state
	transitions int
	resets      int
}

// New returns an engine.
func New(cfg Config) *Engine {
	e := &Engine{cfg: cfg, states: map[string]*state{}}
	switch cfg.HardCap {
	case HardCapHighest:
		for i, t := range cfg.Tiers {
			if !t.Actions.Divert {
				e.hardCapTier = i + 1
			}
		}
	case HardCapDivert:
		for i, t := range cfg.Tiers {
			if t.Actions.Divert {
				e.hardCapTier = i + 1
				break
			}
		}
	}
	return e
}

// Resets returns how many times a ledger's fleet total went backwards and its
// windows restarted.
func (e *Engine) Resets() int { return e.resets }

// Transitions returns the total number of internal tier changes so far.
func (e *Engine) Transitions() int { return e.transitions }

// Actions returns the actions of a tier, zero value for tier 0.
func (e *Engine) Actions(tier int) budgetapi.Action {
	if tier <= 0 || tier > len(e.cfg.Tiers) {
		return budgetapi.Action{SampleRatio: 1}
	}
	return e.cfg.Tiers[tier-1].Actions
}

func burn(delta int64, covered, period time.Duration, budget int64) float64 {
	if covered <= 0 || budget <= 0 || period <= 0 {
		return 0
	}
	return float64(delta) * float64(period) / (float64(budget) * float64(covered))
}

// Step advances the engine.
func (e *Engine) Step(in Input) Result {
	age := max(in.Now.Sub(in.AsOf), 0)
	res := Result{
		Ledgers: make(map[string]LedgerResult, len(in.Ledgers)),
		Stale:   age > e.cfg.StaleAfter,
		Healthy: age <= e.cfg.FailOpenAfter,
		SyncAge: age,
	}
	for id, li := range in.Ledgers {
		st, ok := e.states[id]
		if !ok {
			st = &state{short: ledger.NewRing(e.cfg.Short), long: ledger.NewRing(e.cfg.Long), offered: ledger.NewRing(e.cfg.Long)}
			e.states[id] = st
		}
		st.lastSeen = in.Now
		// fleet totals only grow; if they go back (storage lost its data)
		// the windows restart instead of reporting negative burn rates
		reset := li.Cumulative < st.lastCum || li.OfferedCumulative < st.lastOff
		if reset {
			e.restart(st)
			st.lastAsOf, st.lastCum, st.lastOff = time.Time{}, 0, 0
			e.resets++
		}
		// samples carry the sync as-of time; a frozen sync adds none
		if in.AsOf.After(st.lastAsOf) {
			if reset || (!st.lastAsOf.IsZero() && in.AsOf.Sub(st.lastAsOf) > e.cfg.StaleAfter) {
				st.settleUntil = in.AsOf.Add(e.cfg.Settle)
			}
			if !in.AsOf.After(st.settleUntil) {
				e.restart(st)
			}
			st.short.Push(in.AsOf, li.Cumulative)
			st.long.Push(in.AsOf, li.Cumulative)
			st.offered.Push(in.AsOf, li.OfferedCumulative)
			st.lastAsOf, st.lastCum, st.lastOff = in.AsOf, li.Cumulative, li.OfferedCumulative
		}
		res.Ledgers[id] = e.stepLedger(st, li, in, res.Stale, res.Healthy)
	}
	for id, st := range e.states {
		if in.Now.Sub(st.lastSeen) > e.cfg.Long {
			delete(e.states, id)
		}
	}
	return res
}

// restart empties the windows of a ledger.
func (e *Engine) restart(st *state) {
	st.short, st.long, st.offered = ledger.NewRing(e.cfg.Short), ledger.NewRing(e.cfg.Long), ledger.NewRing(e.cfg.Long)
}

func (e *Engine) stepLedger(st *state, li LedgerInput, in Input, stale, healthy bool) LedgerResult {
	ds, covS := st.short.Delta(st.lastAsOf, st.lastCum, e.cfg.Short)
	dl, covL := st.long.Delta(st.lastAsOf, st.lastCum, e.cfg.Long)
	ol, covO := st.offered.Delta(st.lastAsOf, st.lastOff, e.cfg.Long)
	r := LedgerResult{
		OfferedBurnLong: burn(ol, covO, in.PeriodLength, li.Budget),
		BurnShort:       burn(ds, covS, in.PeriodLength, li.Budget),
		BurnLong:        burn(dl, covL, in.PeriodLength, li.Budget),
		Spend:           li.Spend,
		Budget:          li.Budget,
		Warm:            e.cfg.Short > 0 && float64(covS) >= e.cfg.WarmupCoverage*float64(e.cfg.Short),
		Exempt:          li.Exempt,
	}
	prev := st.tier
	if li.Budget <= 0 {
		st.tier = 0
	} else {
		capped := e.hardCapTier > 0 && li.Spend >= li.Budget
		if !stale {
			target := 0
			for i, t := range e.cfg.Tiers {
				if r.BurnShort >= t.Threshold && r.BurnLong >= t.Threshold {
					target = i + 1
				}
			}
			if r.Warm && target > st.tier {
				st.tier = target
				st.belowSince = time.Time{}
			}
			if capped && e.hardCapTier > st.tier {
				st.tier = e.hardCapTier
				st.belowSince = time.Time{}
			}
		}
		floor := 0
		if capped {
			floor = e.hardCapTier
		}
		// cold windows (start, reset, catch-up) read as zero burn: hold
		if st.tier == prev && st.tier > floor && r.Warm {
			// De-escalate on what the key would send, not only on what it
			// sends now: dropping data lowers accepted burn, and deciding on
			// that alone makes a still noisy key bounce between tiers.
			th := e.cfg.Tiers[st.tier-1].Threshold
			if max(r.BurnLong, r.OfferedBurnLong) < th*e.cfg.HysteresisRatio {
				if st.belowSince.IsZero() {
					st.belowSince = in.Now
				}
				if in.Now.Sub(st.belowSince) >= e.cfg.MinDwell {
					st.tier--
					st.belowSince = time.Time{}
				}
			} else {
				st.belowSince = time.Time{}
			}
		}
	}
	if st.tier != prev {
		e.transitions++
	}
	r.Internal = st.tier
	r.Tier = st.tier
	if li.Exempt || !healthy {
		r.Tier = 0
	}
	return r
}

// Combine merges the results of a key and its groups into the processor
// decision: any exemption exempts, otherwise the most restrictive wins.
func (e *Engine) Combine(results []LedgerResult, stale, healthy bool, now time.Time) budgetapi.Decision {
	d := budgetapi.Decision{Stale: stale, UpdatedAt: now, Actions: e.Actions(0)}
	for _, r := range results {
		if r.Exempt {
			d.Exempt = true
		}
	}
	if d.Exempt || !healthy {
		return d
	}
	for _, r := range results {
		d = budgetapi.MostRestrictive(d, budgetapi.Decision{Tier: r.Tier, Actions: e.Actions(r.Tier)})
	}
	return d
}
