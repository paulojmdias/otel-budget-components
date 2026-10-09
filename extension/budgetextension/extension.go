package budgetextension // import "github.com/paulojmdias/otel-budget-processor/extension/budgetextension"

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensioncapabilities"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/clock"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/costmodel"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/decision"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/keying"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/ledger"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/metadata"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/seriescount"
	spendsync "github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/sync"
)

var (
	_ extension.Extension      = (*budgetExtension)(nil)
	_ budgetapi.Ledger         = (*budgetExtension)(nil)
	_ budgetapi.DropReporter   = (*budgetExtension)(nil)
	_ budgetapi.PolicyProvider = (*budgetExtension)(nil)
	_ budgetapi.SeriesPricer   = (*budgetExtension)(nil)

	_ extensioncapabilities.Dependent = (*budgetExtension)(nil)
)

// Self telemetry attributes, documented in metadata.yaml.
const (
	attrKey           = attribute.Key("otelcol.budget.key")
	attrSignal        = attribute.Key("otelcol.signal")
	attrPriceClass    = attribute.Key("otelcol.budget.price_class")
	attrDropReason    = attribute.Key("otelcol.budget.drop.reason")
	attrWindow        = attribute.Key("otelcol.budget.window")
	attrSyncMode      = attribute.Key("otelcol.budget.sync.mode")
	attrSyncOperation = attribute.Key("otelcol.budget.sync.operation")
)

// periodState is the hot path state of one period.
type periodState struct {
	id      string
	local   *ledger.Table // this replica's accounted spend, with sub micro carry
	offered *ledger.Table // offered bytes per key (stored in the hot class slot)
	dropped sync.Map      // key -> *dropCounters
}

type dropCounters [budgetapi.NumSignals][budgetapi.NumDropReasons]atomic.Int64

type seriesCounters [budgetapi.NumPriceClasses]seriescount.Counter

// snapshot is the immutable decision state published by the loop.
type snapshot struct {
	byToken  map[string]budgetapi.Decision
	byLedger map[string]decision.LedgerResult
	stale    bool
	healthy  bool
	at       time.Time
}

type budgetExtension struct {
	cfg    *Config
	logger *zap.Logger
	set    extension.Settings
	clock  clock.Clock
	tb     *metadata.TelemetryBuilder

	resolver   *keying.Resolver
	model      costmodel.Model
	engine     *decision.Engine
	period     ledger.Period
	sync       spendsync.SpendSync
	alwaysPass budgetapi.AlwaysPass

	cur     atomic.Pointer[periodState]
	snap    atomic.Pointer[snapshot]
	budgets map[string]int64    // rule name -> micro units
	exempt  map[string]struct{} // keys and group ledger IDs never enforced
	series  sync.Map            // ledger ID -> *seriesCounters
	started atomic.Bool

	// owned by the loop (tickMu serializes ticks from the loop and tests)
	tickMu     sync.Mutex
	carry      map[string]int64 // cumulative spend of previous periods
	lastTotals map[string]int64 // fleet totals of the current period at the last tick
	hour       time.Time
	lastTick   time.Time
	tele       teleState

	stop chan struct{}
	wg   sync.WaitGroup
}

type teleState struct {
	spend     map[string]ledger.Spend
	offered   map[string]ledger.Spend
	dropped   map[string][budgetapi.NumSignals][budgetapi.NumDropReasons]int64
	overflows int64
	top       map[string]bool
}

func newExtension(cfg *Config, set extension.Settings, clk clock.Clock) (*budgetExtension, error) {
	if clk == nil {
		clk = clock.Real()
	}
	tb, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	resolver, err := keying.New(cfg.KeyAttributes, cfg.MaxKeys, cfg.ruleSpecs())
	if err != nil {
		return nil, err
	}
	tiers, err := cfg.tiers()
	if err != nil {
		return nil, err
	}
	hc, err := cfg.hardCap()
	if err != nil {
		return nil, err
	}
	period, err := cfg.period()
	if err != nil {
		return nil, err
	}
	sev, err := budgetapi.ParseSeverity(cfg.AlwaysPass.LogSeverity)
	if err != nil {
		return nil, err
	}
	// storage mode: after a gap, replicas catch up as their clients
	// reconnect; one that takes longer than stale_after counts as gone
	var settle time.Duration
	if cfg.Sync.Mode == spendsync.ModeStorage {
		settle = cfg.StaleAfter + cfg.Sync.Storage.FlushInterval + cfg.Sync.Storage.ReadInterval
	}
	e := &budgetExtension{
		cfg:      cfg,
		logger:   set.Logger,
		set:      set,
		clock:    clk,
		tb:       tb,
		resolver: resolver,
		model:    cfg.costModel(),
		engine: decision.New(decision.Config{
			Tiers: tiers, Short: cfg.Windows.Short, Long: cfg.Windows.Long, Interval: cfg.DecisionInterval,
			WarmupCoverage: cfg.WarmupCoverage, HardCap: hc,
			HysteresisRatio: cfg.Hysteresis.Ratio, MinDwell: cfg.Hysteresis.MinDwell,
			StaleAfter: cfg.StaleAfter, FailOpenAfter: cfg.FailOpenAfter, Settle: settle,
		}),
		period:     period,
		alwaysPass: budgetapi.AlwaysPass{LogSeverity: sev, SpanStatusError: cfg.AlwaysPass.SpanStatusError},
		carry:      map[string]int64{},
		lastTotals: map[string]int64{},
		stop:       make(chan struct{}),
	}
	e.exempt = make(map[string]struct{}, len(cfg.Exempt))
	for _, k := range cfg.Exempt {
		e.exempt[k] = struct{}{}
	}
	e.budgets = make(map[string]int64, len(cfg.Budgets))
	for _, b := range cfg.Budgets {
		e.budgets[b.Name] = costmodel.ToMicro(b.Budget)
	}
	e.sync, err = e.newSync()
	if err != nil {
		return nil, err
	}
	now := clk.Now()
	id := period.ID(now)
	e.cur.Store(newPeriodState(id))
	e.sync.SetPeriod(id)
	e.hour = now.Truncate(time.Hour)
	e.snap.Store(&snapshot{byToken: map[string]budgetapi.Decision{}, byLedger: map[string]decision.LedgerResult{}, healthy: true, at: now})
	return e, nil
}

func newPeriodState(id string) *periodState {
	return &periodState{id: id, local: ledger.NewTable(id), offered: ledger.NewTable(id)}
}

func (e *budgetExtension) newSync() (spendsync.SpendSync, error) {
	set := spendsync.Settings{
		Clock:  e.clock,
		Logger: e.logger,
		Owner:  e.set.ID,
		OnError: func(op string) {
			e.tb.BudgetSyncErrors.Add(context.Background(), 1, metric.WithAttributes(
				attrSyncMode.String(e.cfg.Sync.Mode), attrSyncOperation.String(op),
			))
		},
		OnOverflow: func() {
			e.tb.BudgetSyncErrors.Add(context.Background(), 1, metric.WithAttributes(
				attrSyncMode.String(e.cfg.Sync.Mode), attrSyncOperation.String("pending_overflow"),
			))
		},
	}
	switch e.cfg.Sync.Mode {
	case spendsync.ModeShare:
		n, err := e.cfg.shareReplicas()
		if err != nil {
			return nil, err
		}
		return spendsync.NewLocal(spendsync.LocalConfig{Storage: e.cfg.Sync.Local.Storage, CheckpointInterval: e.cfg.Sync.Local.CheckpointInterval, Divisor: n}, set), nil
	case spendsync.ModeStorage:
		s := e.cfg.Sync.Storage
		return spendsync.NewStorage(spendsync.StorageConfig{
			Extension: *s.Extension, Mode: s.Mode, NodeID: s.NodeID, NodePrefix: s.NodePrefix, MaxNodes: s.MaxNodes,
			FlushInterval: s.FlushInterval, ReadInterval: s.ReadInterval, MaxPendingKeys: s.MaxPendingKeys,
		}, set), nil
	default:
		return spendsync.NewLocal(spendsync.LocalConfig{Storage: e.cfg.Sync.Local.Storage, CheckpointInterval: e.cfg.Sync.Local.CheckpointInterval}, set), nil
	}
}

// Dependencies makes the Collector start storage extensions before this one.
func (e *budgetExtension) Dependencies() []component.ID {
	if e.cfg.Sync.Mode == spendsync.ModeStorage && e.cfg.Sync.Storage.Extension != nil {
		return []component.ID{*e.cfg.Sync.Storage.Extension}
	}
	if e.cfg.Sync.Mode != spendsync.ModeStorage && e.cfg.Sync.Local.Storage != nil {
		return []component.ID{*e.cfg.Sync.Local.Storage}
	}
	return nil
}

// Start starts sync, publishes the first decisions, the API, and the loop.
func (e *budgetExtension) Start(ctx context.Context, host component.Host) error {
	if err := e.sync.Start(ctx, host); err != nil {
		return err
	}
	e.tick(e.clock.Now())
	tk := e.clock.NewTicker(e.cfg.DecisionInterval)
	e.wg.Go(func() {
		defer tk.Stop()
		for {
			select {
			case <-e.stop:
				return
			case <-tk.C():
				e.tick(e.clock.Now())
			}
		}
	})
	e.started.Store(true)
	return nil
}

// Shutdown stops everything.
func (e *budgetExtension) Shutdown(ctx context.Context) error {
	e.started.Store(false)
	select {
	case <-e.stop:
	default:
		close(e.stop)
	}
	var errs []error
	e.wg.Wait()
	errs = append(errs, e.sync.Shutdown(ctx))
	e.tb.Shutdown()
	return errors.Join(errs...)
}

// ResolveKey implements budgetapi.Ledger.
func (e *budgetExtension) ResolveKey(res pcommon.Resource) string { return e.resolver.ResolveKey(res) }

// Decision implements budgetapi.Ledger. One atomic load plus a map lookup;
// tokens first seen since the last publish are combined on the fly from the
// same immutable snapshot.
func (e *budgetExtension) Decision(key string) budgetapi.Decision {
	s := e.snap.Load()
	if d, ok := s.byToken[key]; ok {
		return d
	}
	res := e.resolver.Lookup(key)
	results := make([]decision.LedgerResult, 0, 1+len(res.Groups))
	results = append(results, s.byLedger[res.Key])
	for _, g := range res.Groups {
		results = append(results, s.byLedger[e.resolver.GroupLedgerID(g)])
	}
	return e.engine.Combine(results, s.stale, s.healthy, s.at)
}

// Record implements budgetapi.Ledger: atomic adds only.
func (e *budgetExtension) Record(key string, sig budgetapi.Signal, class budgetapi.PriceClass, u budgetapi.Usage) {
	res := e.resolver.Lookup(key)
	ps := e.cur.Load()
	cost := e.model.Cost(sig, class, u)
	e.add(ps, res.Key, sig, class, cost, u.SeriesHash)
	for _, g := range res.Groups {
		e.add(ps, e.resolver.GroupLedgerID(g), sig, class, cost, u.SeriesHash)
	}
}

func (e *budgetExtension) add(ps *periodState, id string, sig budgetapi.Signal, class budgetapi.PriceClass, cost costmodel.Cost, series []uint64) {
	if !cost.IsZero() {
		if micro := ps.local.Cell(id).Counter(sig, class).Add(cost); micro != 0 {
			e.sync.Add(ps.id, id, sig, class, micro)
		}
	}
	if len(series) > 0 {
		v, ok := e.series.Load(id)
		if !ok {
			v, _ = e.series.LoadOrStore(id, &seriesCounters{})
		}
		v.(*seriesCounters)[class].Insert(series)
	}
}

// RecordOffered implements budgetapi.Ledger.
// Offered usage is priced at hot prices and synced like spend, under
// offered:<ledger ID>, so de-escalation can use what a key would send.
func (e *budgetExtension) RecordOffered(key string, sig budgetapi.Signal, u budgetapi.Usage) {
	ps := e.cur.Load()
	if u.Bytes != 0 {
		ps.offered.Cell(budgetapi.KeyName(key)).Counter(sig, budgetapi.PriceHot).AddMicro(u.Bytes)
	}
	cost := e.model.Cost(sig, budgetapi.PriceHot, u)
	if cost.IsZero() {
		return
	}
	res := e.resolver.Lookup(key)
	e.add(ps, res.OfferedKey, sig, budgetapi.PriceHot, cost, nil)
	for _, g := range res.Groups {
		e.add(ps, e.resolver.OfferedGroupLedgerID(g), sig, budgetapi.PriceHot, cost, nil)
	}
}

// RecordDropped implements budgetapi.DropReporter.
func (e *budgetExtension) RecordDropped(key string, sig budgetapi.Signal, reason budgetapi.DropReason, n int64) {
	if n == 0 {
		return
	}
	ps := e.cur.Load()
	name := budgetapi.KeyName(key)
	v, ok := ps.dropped.Load(name)
	if !ok {
		v, _ = ps.dropped.LoadOrStore(name, &dropCounters{})
	}
	v.(*dropCounters)[sig][reason].Add(n)
}

// Healthy implements budgetapi.Ledger.
func (e *budgetExtension) Healthy() bool { return e.started.Load() && e.snap.Load().healthy }

// PricesSeries implements budgetapi.SeriesPricer.
func (e *budgetExtension) PricesSeries() bool {
	for _, p := range e.model.Prices {
		if p.SeriesHour > 0 {
			return true
		}
	}
	return false
}

// AlwaysPass implements budgetapi.PolicyProvider.
func (e *budgetExtension) AlwaysPass() budgetapi.AlwaysPass { return e.alwaysPass }

// budgetFor returns the budget of a ledger ID.
func (e *budgetExtension) budgetFor(id string) int64 {
	if name, ok := strings.CutPrefix(id, keying.GroupPrefix); ok {
		return e.budgets[name]
	}
	res := e.resolver.Lookup(id)
	if res.KeyRule < 0 {
		return 0
	}
	return e.budgets[e.resolver.RuleName(res.KeyRule)]
}

// tick runs one decision step.
func (e *budgetExtension) tick(now time.Time) {
	e.tickMu.Lock()
	defer e.tickMu.Unlock()
	// ticks may queue on the lock with an older time; never go back
	if now.Before(e.lastTick) {
		now = e.lastTick
	}
	e.lastTick = now

	e.rollover(now)
	e.rotateSeries(now)

	// a zero as-of (never synced) makes everything stale and fail open
	totals, asOf, _ := e.sync.Totals()
	divisor := e.sync.Divisor()

	ids := map[string]struct{}{}
	for id := range totals {
		if !strings.HasPrefix(id, keying.OfferedPrefix) {
			ids[id] = struct{}{}
		}
	}
	e.resolver.RangeResolutions(func(r *keying.Resolution) {
		ids[r.Key] = struct{}{}
		for _, g := range r.Groups {
			ids[e.resolver.GroupLedgerID(g)] = struct{}{}
		}
	})
	for id := range e.carry {
		if !strings.HasPrefix(id, keying.OfferedPrefix) {
			ids[id] = struct{}{}
		}
	}

	in := decision.Input{Now: now, AsOf: asOf, PeriodLength: e.period.Length(now), Ledgers: make(map[string]decision.LedgerInput, len(ids))}
	for id := range ids {
		sp := totals[id]
		spend := sp.Total()
		e.lastTotals[id] = spend
		oid := keying.OfferedPrefix + id
		osp := totals[oid]
		offered := osp.Total()
		e.lastTotals[oid] = offered
		b := e.budgetFor(id)
		_, ex := e.exempt[id]
		in.Ledgers[id] = decision.LedgerInput{
			Cumulative: e.carry[id] + spend, Spend: spend, Budget: b / divisor, Exempt: ex,
			OfferedCumulative: e.carry[oid] + offered,
		}
	}
	res := e.engine.Step(in)
	if ts, ok := e.sync.(spendsync.TierSync); ok {
		own := make(map[string]int, len(res.Ledgers))
		for id, r := range res.Ledgers {
			if r.Internal > 0 {
				own[id] = r.Internal
			}
		}
		ts.PublishTiers(own)
		applyFleetTiers(&res, ts.FleetTiers(now, e.cfg.StaleAfter))
	}

	snap := &snapshot{byLedger: res.Ledgers, stale: res.Stale, healthy: res.Healthy, at: now, byToken: map[string]budgetapi.Decision{}}
	e.resolver.RangeResolutions(func(r *keying.Resolution) {
		results := make([]decision.LedgerResult, 0, 1+len(r.Groups))
		results = append(results, res.Ledgers[r.Key])
		for _, g := range r.Groups {
			results = append(results, res.Ledgers[e.resolver.GroupLedgerID(g)])
		}
		snap.byToken[r.Token] = e.engine.Combine(results, res.Stale, res.Healthy, now)
	})
	e.snap.Store(snap)

	e.emitTelemetry(res)
}

// applyFleetTiers raises each ledger's published tier to the highest tier
// another live replica decided, so replicas that read fleet totals at slightly
// different times still enforce the same tier. Exempt ledgers, ledgers without
// a budget, and failing open keep tier 0.
func applyFleetTiers(res *decision.Result, fleet map[string]int) {
	if !res.Healthy {
		return
	}
	for id, r := range res.Ledgers {
		if ft := fleet[id]; ft > r.Tier && !r.Exempt && r.Budget > 0 {
			r.Tier = ft
			res.Ledgers[id] = r
		}
	}
}

func (e *budgetExtension) rollover(now time.Time) {
	id := e.period.ID(now)
	ps := e.cur.Load()
	if ps.id == id {
		return
	}
	// ledgers without spend in the ending period stop being tracked
	carry := make(map[string]int64, len(e.lastTotals))
	for k, v := range e.lastTotals {
		if v > 0 {
			carry[k] = e.carry[k] + v
		}
	}
	e.carry = carry
	e.lastTotals = map[string]int64{}
	e.sync.SetPeriod(id)
	e.cur.Store(newPeriodState(id))
	e.resolver.ResetPeriod()
	e.tele.spend, e.tele.offered, e.tele.dropped, e.tele.overflows = nil, nil, nil, 0
	e.logger.Info("budget period rolled over", zap.String("period", id))
}

// rotateSeries prices the ending hour of active series.
func (e *budgetExtension) rotateSeries(now time.Time) {
	h := now.Truncate(time.Hour)
	if !h.After(e.hour) {
		return
	}
	e.hour = h
	ps := e.cur.Load()
	e.series.Range(func(k, v any) bool {
		id := k.(string)
		sc := v.(*seriesCounters)
		for _, class := range []budgetapi.PriceClass{budgetapi.PriceHot, budgetapi.PriceCold} {
			est := sc[class].Rotate()
			if est == 0 {
				continue
			}
			if micro := ps.local.Cell(id).Counter(budgetapi.SignalMetrics, class).Add(e.model.SeriesCost(class, int64(est))); micro != 0 {
				e.sync.Add(ps.id, id, budgetapi.SignalMetrics, class, micro)
			}
		}
		return true
	})
}

// telemetryKey applies the cardinality guard.
func (e *budgetExtension) telemetryKey(id string) string {
	if e.tele.top[id] {
		return id
	}
	return keying.OtherKey
}

func (e *budgetExtension) emitTelemetry(res decision.Result) {
	ctx := context.Background()
	ps := e.cur.Load()
	spend := ps.local.Snapshot()

	// rank by spend for the cardinality guard (fleet spend from the decision)
	type kv struct {
		id string
		v  int64
	}
	rank := make([]kv, 0, len(res.Ledgers))
	for id, r := range res.Ledgers {
		rank = append(rank, kv{id, r.Spend})
	}
	sort.Slice(rank, func(i, j int) bool {
		if rank[i].v != rank[j].v {
			return rank[i].v > rank[j].v
		}
		return rank[i].id < rank[j].id
	})
	top := make(map[string]bool, e.cfg.Telemetry.MaxKeys)
	for i := 0; i < len(rank) && i < e.cfg.Telemetry.MaxKeys; i++ {
		top[rank[i].id] = true
	}
	e.tele.top = top

	if e.tele.spend == nil {
		e.tele.spend = map[string]ledger.Spend{}
	}
	for id, s := range spend {
		if strings.HasPrefix(id, keying.OfferedPrefix) {
			continue
		}
		prev := e.tele.spend[id]
		for _, sig := range []budgetapi.Signal{budgetapi.SignalLogs, budgetapi.SignalTraces, budgetapi.SignalMetrics} {
			for _, cl := range []budgetapi.PriceClass{budgetapi.PriceHot, budgetapi.PriceCold} {
				if d := s[sig][cl] - prev[sig][cl]; d > 0 {
					e.tb.BudgetSpend.Add(ctx, d, metric.WithAttributes(
						attrKey.String(e.telemetryKey(id)), attrSignal.String(sig.String()), attrPriceClass.String(cl.String()),
					))
				}
			}
		}
		e.tele.spend[id] = s
	}

	if e.tele.offered == nil {
		e.tele.offered = map[string]ledger.Spend{}
	}
	for id, s := range ps.offered.Snapshot() {
		prev := e.tele.offered[id]
		for _, sig := range []budgetapi.Signal{budgetapi.SignalLogs, budgetapi.SignalTraces, budgetapi.SignalMetrics} {
			if d := s[sig][budgetapi.PriceHot] - prev[sig][budgetapi.PriceHot]; d > 0 {
				e.tb.BudgetOfferedSize.Add(ctx, d, metric.WithAttributes(
					attrKey.String(e.telemetryKey(id)), attrSignal.String(sig.String()),
				))
			}
		}
		e.tele.offered[id] = s
	}

	if e.tele.dropped == nil {
		e.tele.dropped = map[string][budgetapi.NumSignals][budgetapi.NumDropReasons]int64{}
	}
	ps.dropped.Range(func(k, v any) bool {
		id := k.(string)
		dc := v.(*dropCounters)
		prev := e.tele.dropped[id]
		var cur [budgetapi.NumSignals][budgetapi.NumDropReasons]int64
		for s := range cur {
			for r := range cur[s] {
				cur[s][r] = dc[s][r].Load()
				if d := cur[s][r] - prev[s][r]; d > 0 {
					e.tb.BudgetDroppedItems.Add(ctx, d, metric.WithAttributes(
						attrKey.String(e.telemetryKey(id)), attrSignal.String(budgetapi.Signal(s).String()),
						attrDropReason.String(budgetapi.DropReason(r).String()),
					))
				}
			}
		}
		e.tele.dropped[id] = cur
		return true
	})

	// gauges: per key for top keys, aggregated (max tier, max burn, summed budget) for the rest
	type agg struct {
		budget, spend, tier, exempt int64
		short, long, offered        float64
		seen                        bool
	}
	var other agg
	for id, r := range res.Ledgers {
		if !top[id] {
			other.seen = true
			other.budget += r.Budget
			other.spend += r.Spend
			other.tier = max(other.tier, int64(r.Tier))
			other.short = max(other.short, r.BurnShort)
			other.long = max(other.long, r.BurnLong)
			other.offered = max(other.offered, r.OfferedBurnLong)
			if r.Exempt {
				other.exempt = 1
			}
			continue
		}
		e.recordGauges(ctx, id, r.Budget, r.Spend, int64(r.Tier), b2i(r.Exempt), r.BurnShort, r.BurnLong, r.OfferedBurnLong)
	}
	if other.seen {
		e.recordGauges(ctx, keying.OtherKey, other.budget, other.spend, other.tier, other.exempt, other.short, other.long, other.offered)
	}
	e.tb.BudgetSyncAge.Record(ctx, res.SyncAge.Seconds())
	e.tb.BudgetKeys.Record(ctx, e.resolver.Keys())
	if o := e.resolver.Overflows(); o > e.tele.overflows {
		e.tb.BudgetKeyOverflows.Add(ctx, o-e.tele.overflows)
		e.tele.overflows = o
	}
}

func (e *budgetExtension) recordGauges(ctx context.Context, key string, budget, spend, tier, exempt int64, short, long, offered float64) {
	k := attrKey.String(key)
	e.tb.BudgetLimit.Record(ctx, budget, metric.WithAttributes(k))
	e.tb.BudgetFleetSpend.Record(ctx, spend, metric.WithAttributes(k))
	e.tb.BudgetTier.Record(ctx, tier, metric.WithAttributes(k))
	e.tb.BudgetExempt.Record(ctx, exempt, metric.WithAttributes(k))
	e.tb.BudgetBurnRate.Record(ctx, short, metric.WithAttributes(k, attrWindow.String("short")))
	e.tb.BudgetBurnRate.Record(ctx, long, metric.WithAttributes(k, attrWindow.String("long")))
	e.tb.BudgetBurnRate.Record(ctx, offered, metric.WithAttributes(k, attrWindow.String("offered_long")))
}

func b2i(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
