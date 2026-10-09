package budgetextension

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/clock"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/costmodel"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/decision"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/metadatatest"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/testutil"
)

var (
	t0       = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	extID    = component.MustNewID("budget")
	interval = 5 * time.Second
)

// demoConfig mirrors examples/08-demo.yaml timing.
func demoConfig() *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.Period = "1h"
	cfg.Prices.Hot = PriceList{LogsGB: 0.40, TracesGB: 0.30, MetricsMillionPoints: 0.10, SeriesHour: 0.001}
	cfg.Windows = WindowsConfig{Short: time.Minute, Long: 6 * time.Minute}
	cfg.DecisionInterval = interval
	cfg.Hysteresis = HysteresisConfig{Ratio: 0.8, MinDwell: 3 * time.Minute}
	cfg.StaleAfter, cfg.FailOpenAfter = time.Minute, 5*time.Minute
	cfg.Budgets = []BudgetRule{{Name: "default", Match: Match{Default: true}, Budget: 1}}
	cfg.Tiers = []TierConfig{{Threshold: 1, Actions: ActionsConfig{DropBelowSeverity: "INFO"}}}
	return cfg
}

type env struct {
	t   *testing.T
	ext *budgetExtension
	clk *clock.Fake
	tel *componenttest.Telemetry
	lp  *driver
	mp  *driver
	out *consumertest.LogsSink
}

func newEnv(t *testing.T, cfg *Config, host testutil.Host) *env {
	t.Helper()
	require.NoError(t, cfg.Validate())
	e := &env{t: t, clk: clock.NewFake(t0), tel: componenttest.NewTelemetry(), out: &consumertest.LogsSink{}}
	set := metadatatest.NewSettings(e.tel)
	set.ID = extID
	var err error
	e.ext, err = newExtension(cfg, set, e.clk)
	require.NoError(t, err)
	if host == nil {
		host = testutil.Host{}
	}
	require.NoError(t, e.ext.Start(t.Context(), host))
	t.Cleanup(func() {
		require.NoError(t, e.ext.Shutdown(context.WithoutCancel(t.Context())))
		require.NoError(t, e.tel.Shutdown(context.WithoutCancel(t.Context())))
	})
	e.lp = &driver{ledger: e.ext, out: e.out}
	e.mp = e.lp
	return e
}

// step advances one decision interval and runs a tick.
func (e *env) step() {
	e.clk.Advance(e.ext.cfg.DecisionInterval)
	e.ext.tick(e.clk.Now())
}

// row is the published result of one ledger ID.
type row struct {
	LedgerID    string
	Tier        int
	SpendMicro  int64
	BudgetMicro int64
	Stale       bool
}

func (e *env) ledger(id string) row {
	s := e.ext.snap.Load()
	r, ok := s.byLedger[id]
	if !ok {
		return row{}
	}
	return row{LedgerID: id, Tier: r.Tier, SpendMicro: r.Spend, BudgetMicro: r.Budget, Stale: s.stale}
}

func (e *env) send(service string, recs []testutil.LogRecord) {
	ld := plog.NewLogs()
	testutil.AddLogs(ld, service, recs...)
	_ = e.lp.ConsumeLogs(context.Background(), ld)
}

func noisyBatch() []testutil.LogRecord {
	recs := testutil.Repeat(90, testutil.LogRecord{Severity: plog.SeverityNumberDebug, Body: "debug noise padding padding padding"})
	return append(recs, testutil.Repeat(10, testutil.LogRecord{Severity: plog.SeverityNumberInfo, Body: "info"})...)
}

func batchBytes() int64 {
	ld := plog.NewLogs()
	testutil.AddLogs(ld, "noisy", noisyBatch()...)
	var m plog.ProtoMarshaler
	return int64(m.LogsSize(ld))
}

// noisyBudget returns the hourly budget at which one noisy batch per
// interval burns at 3.0.
func noisyBudget() float64 {
	perHour := float64(batchBytes()) * float64(time.Hour/interval)
	return perHour * 0.40 / 1e9 / 3
}

// Phase 1 done criterion: a noisy log producer escalates its key to tier 1
// and accounted spend flattens.
func TestNoisyProducerEscalatesAndSpendFlattens(t *testing.T) {
	cfg := demoConfig()
	cfg.Budgets[0].Budget = noisyBudget()
	e := newEnv(t, cfg, nil)
	var spend []int64
	escalatedAt := -1
	for i := range 120 { // 10 minutes
		e.send("noisy", noisyBatch())
		e.step()
		l := e.ledger("noisy")
		spend = append(spend, l.SpendMicro)
		if escalatedAt < 0 && l.Tier == 1 {
			escalatedAt = i
		}
	}
	require.GreaterOrEqual(t, escalatedAt, 9, "no escalation during warm up")
	require.Less(t, escalatedAt, 24, "escalates within two minutes")
	before := spend[escalatedAt] - spend[escalatedAt-6]
	after := spend[escalatedAt+12] - spend[escalatedAt+6]
	t.Logf("escalated at step %d, spend per 30s before %d, after %d", escalatedAt, before, after)
	assert.Less(t, after*4, before, "accepted spend drops by more than 4x once DEBUG is dropped")
	assert.Equal(t, 1, e.ext.Decision("noisy").Tier)

	tel, err := e.tel.GetMetric("otelcol_budget_tier")
	require.NoError(t, err)
	assert.NotEmpty(t, tel.Data.(metricdata.Gauge[int64]).DataPoints)
	_, err = e.tel.GetMetric("otelcol_budget_spend")
	require.NoError(t, err)
	_, err = e.tel.GetMetric("otelcol_budget_dropped_items")
	require.NoError(t, err)
	_, err = e.tel.GetMetric("otelcol_budget_offered_size")
	require.NoError(t, err)
	fleet, err := e.tel.GetMetric("otelcol_budget_fleet_spend")
	require.NoError(t, err)
	assert.Equal(t, spend[len(spend)-1], fleet.Data.(metricdata.Gauge[int64]).DataPoints[0].Value, "fleet spend gauge follows the decision input")
}

// A key that stays noisy holds its tier: the DEBUG it loses at tier 1 still
// counts as offered spend, so it does not de-escalate and bounce back.
func TestSteadyNoisyKeyHoldsTier(t *testing.T) {
	cfg := demoConfig()
	cfg.Budgets[0].Budget = noisyBudget()
	e := newEnv(t, cfg, nil)
	for range int(30 * time.Minute / interval) {
		e.send("noisy", noisyBatch())
		e.step()
	}
	assert.Equal(t, 1, e.ext.Decision("noisy").Tier)
	e.ext.tickMu.Lock()
	transitions := e.ext.engine.Transitions()
	e.ext.tickMu.Unlock()
	assert.Equal(t, 1, transitions, "one escalation, no flapping")
	r := e.ext.snap.Load().byLedger["noisy"]
	assert.Greater(t, r.OfferedBurnLong, r.BurnLong, "offered burn includes what tier 1 drops")
}

// An exempt key escalates internally but is never enforced.
func TestExemptKey(t *testing.T) {
	cfg := demoConfig()
	cfg.Budgets[0].Budget = noisyBudget()
	cfg.Exempt = []string{"noisy"}
	e := newEnv(t, cfg, nil)
	for range 30 {
		e.send("noisy", noisyBatch())
		e.step()
	}
	d := e.ext.Decision("noisy")
	assert.True(t, d.Exempt)
	assert.Equal(t, 0, d.Tier)
	assert.Equal(t, 1, e.ext.snap.Load().byLedger["noisy"].Internal, "the engine still tracks the burn")
	e.send("noisy", noisyBatch())
	assert.Equal(t, 100, e.out.AllLogs()[len(e.out.AllLogs())-1].LogRecordCount(), "exempt key passes everything")
}

func TestHealthyFollowsSnapshot(t *testing.T) {
	e := newEnv(t, demoConfig(), nil)
	assert.True(t, e.ext.Healthy())
	s := *e.ext.snap.Load()
	s.healthy = false
	e.ext.snap.Store(&s)
	assert.False(t, e.ext.Healthy())
}

func TestGroupMostRestrictive(t *testing.T) {
	cfg := demoConfig()
	cfg.KeyAttributes = []string{"service.name"}
	cfg.Budgets = []BudgetRule{
		{Name: "payments-team", Match: Match{Attribute: map[string]string{"k8s.pod.label.team": "payments"}}, Scope: "group", Budget: noisyBudget() / 2},
		{Name: "default", Match: Match{Default: true}, Budget: 1000},
	}
	e := newEnv(t, cfg, nil)
	send := func(service string) {
		ld := plog.NewLogs()
		rl := testutil.AddLogs(ld, service, noisyBatch()...)
		rl.Resource().Attributes().PutStr("k8s.pod.label.team", "payments")
		_ = e.lp.ConsumeLogs(t.Context(), ld)
	}
	for range 30 {
		send("pay-api")
		send("pay-worker")
		e.step()
	}
	assert.Equal(t, 1, e.ledger("group:payments-team").Tier)
	assert.Equal(t, 0, e.ledger("pay-api").Tier, "the key alone is far below its budget")
	res := pcommon.NewResource()
	res.Attributes().PutStr("service.name", "pay-api")
	res.Attributes().PutStr("k8s.pod.label.team", "payments")
	tok := e.ext.ResolveKey(res)
	assert.Equal(t, 1, e.ext.Decision(tok).Tier, "the group tier wins")
	assert.Equal(t, "pay-api", budgetapi.KeyName(tok))

	// a token first seen after the last publish is combined on the fly
	res2 := pcommon.NewResource()
	res2.Attributes().PutStr("service.name", "pay-new")
	res2.Attributes().PutStr("k8s.pod.label.team", "payments")
	assert.Equal(t, 1, e.ext.Decision(e.ext.ResolveKey(res2)).Tier)

	// an exempt group exempts its members
	e.ext.tickMu.Lock()
	e.ext.exempt["group:payments-team"] = struct{}{}
	e.ext.tickMu.Unlock()
	e.step()
	assert.True(t, e.ext.Decision(tok).Exempt)
}

func TestShareDividesBudget(t *testing.T) {
	cfg := demoConfig()
	cfg.Sync.Mode = "share"
	cfg.Sync.Share.Replicas = 4
	e := newEnv(t, cfg, nil)
	e.send("a", testutil.Repeat(1, testutil.LogRecord{}))
	e.step()
	assert.Equal(t, costmodel.ToMicro(1)/4, e.ledger("a").BudgetMicro)
	assert.Equal(t, "share", e.ext.sync.Mode())

	cfg = demoConfig()
	cfg.Sync.Mode = "share"
	cfg.Sync.Share.ReplicasEnv = "BUDGET_TEST_UNSET_ENV"
	_, err := newExtension(cfg, metadatatest.NewSettings(componenttest.NewTelemetry()), nil)
	require.Error(t, err)
}

func TestStorageFleet(t *testing.T) {
	mr := miniredis.RunT(t)
	sid := component.MustNewIDWithName("redis_storage", "budget")
	mk := func(node string) (*env, *testutil.RedisClient) {
		cfg := demoConfig()
		cfg.Sync.Mode = "storage"
		cfg.Sync.Storage.Extension = &sid
		cfg.Sync.Storage.NodeID = node
		cfg.Sync.Storage.NodePrefix = "gw"
		cfg.Sync.Storage.MaxNodes = 2
		c := testutil.NewRedisClient(mr.Addr(), "")
		return newEnv(t, cfg, testutil.Host{sid: &testutil.StorageExtension{Client: c}}), c
	}
	a, ca := mk("gw-0")
	b, _ := mk("gw-1")
	assert.Equal(t, []component.ID{sid}, a.ext.Dependencies(), "storage starts first")
	a.send("checkout", testutil.Repeat(2000, testutil.LogRecord{Severity: plog.SeverityNumberInfo}))
	b.send("checkout", testutil.Repeat(2000, testutil.LogRecord{Severity: plog.SeverityNumberInfo}))
	for _, x := range []*env{a, b} {
		require.NoError(t, x.ext.sync.(interface{ Flush(context.Context) error }).Flush(t.Context()))
	}
	// keys refreshed by a flush are re-read after one read interval
	for _, x := range []*env{a, b} {
		x.clk.Advance(30 * time.Second)
		require.NoError(t, x.ext.sync.(interface{ Read(context.Context) error }).Read(t.Context()))
		x.ext.tick(x.clk.Now())
	}
	assert.Equal(t, "increment", a.ext.sync.Mode())
	assert.Equal(t, a.ledger("checkout").SpendMicro, b.ledger("checkout").SpendMicro)
	assert.Positive(t, a.ledger("checkout").SpendMicro)

	// storage down for longer than fail_open_after fails open
	ca.Fail.Store(true)
	for range 70 {
		a.step()
	}
	assert.False(t, a.ext.Healthy())
	assert.True(t, a.ledger("checkout").Stale)
}

func TestRolloverAndSeries(t *testing.T) {
	cfg := demoConfig()
	cfg.Budgets[0].Budget = 1000
	e := newEnv(t, cfg, nil)
	md := pmetric.NewMetrics()
	testutil.AddGauges(md, "m", 100, "http_requests")
	require.NoError(t, e.mp.ConsumeMetrics(t.Context(), md))
	e.step()
	pointsOnly := e.ledger("m").SpendMicro
	assert.Equal(t, int64(10), pointsOnly, "100 points at 0.10 per million")

	// cross the hour: series hours priced, period rolls over
	for range 720 {
		e.step()
	}
	assert.NotEqual(t, "1790812800", e.ext.cur.Load().id, "period rolled over")
	e.ext.tickMu.Lock()
	carry := e.ext.carry["m"]
	e.ext.tickMu.Unlock()
	assert.Positive(t, carry, "spend carried for burn windows")
	// the series cost lands in the old or the new period depending on order
	total := carry + e.ledger("m").SpendMicro
	assert.InDelta(t, 10+100*1000, float64(total), 5000, "about 100 series at 0.001 per hour")
}

func TestTelemetryCardinalityGuard(t *testing.T) {
	cfg := demoConfig()
	cfg.Telemetry.MaxKeys = 1
	e := newEnv(t, cfg, nil)
	e.send("big", testutil.Repeat(100, testutil.LogRecord{Severity: plog.SeverityNumberInfo}))
	e.send("small1", testutil.Repeat(1, testutil.LogRecord{Severity: plog.SeverityNumberInfo}))
	e.send("small2", testutil.Repeat(1, testutil.LogRecord{Severity: plog.SeverityNumberInfo}))
	e.step()
	e.send("small1", testutil.Repeat(1, testutil.LogRecord{Severity: plog.SeverityNumberInfo}))
	e.step()
	m, err := e.tel.GetMetric("otelcol_budget_tier")
	require.NoError(t, err)
	keys := map[string]bool{}
	for _, dp := range m.Data.(metricdata.Gauge[int64]).DataPoints {
		v, _ := dp.Attributes.Value(attrKey)
		keys[v.AsString()] = true
	}
	assert.Equal(t, map[string]bool{"big": true, "__other__": true}, keys)
	_, err = e.tel.GetMetric("otelcol_budget_keys")
	require.NoError(t, err)
	_, err = e.tel.GetMetric("otelcol_budget_sync_age")
	require.NoError(t, err)
}

func TestKeyOverflowTelemetry(t *testing.T) {
	cfg := demoConfig()
	cfg.MaxKeys = 1
	e := newEnv(t, cfg, nil)
	e.send("a", testutil.Repeat(1, testutil.LogRecord{}))
	e.send("b", testutil.Repeat(1, testutil.LogRecord{}))
	e.step()
	assert.Equal(t, "__other__", e.ledger("__other__").LedgerID)
	assert.Equal(t, int64(1), e.ext.resolver.Overflows())
	_, err := e.tel.GetMetric("otelcol_budget_key_overflows")
	require.NoError(t, err)
}

func TestLedgerBasics(t *testing.T) {
	e := newEnv(t, demoConfig(), nil)
	assert.True(t, e.ext.Healthy())
	assert.Equal(t, plog.SeverityNumberError, e.ext.AlwaysPass().LogSeverity)
	e.ext.RecordDropped("a", budgetapi.SignalLogs, budgetapi.DropSample, 0)
	e.ext.RecordOffered("a", budgetapi.SignalLogs, budgetapi.Usage{})
	e.ext.Record("a", budgetapi.SignalLogs, budgetapi.PriceHot, budgetapi.Usage{})
	_, err := e.tel.GetMetric("otelcol_budget_dropped_items")
	require.Error(t, err, "nothing recorded for zero drops")
}

func TestStartErrors(t *testing.T) {
	cfg := demoConfig()
	cfg.Sync.Mode = "storage"
	sid := component.MustNewID("missing")
	cfg.Sync.Storage.Extension = &sid
	ext, err := newExtension(cfg, metadatatest.NewSettings(componenttest.NewTelemetry()), nil)
	require.NoError(t, err)
	require.Error(t, ext.Start(t.Context(), testutil.Host{}))

	f := NewFactory()
	_, err = f.Create(t.Context(), metadatatest.NewSettings(componenttest.NewTelemetry()), demoConfig())
	require.NoError(t, err)
}

func BenchmarkRecord(b *testing.B) {
	ext, _ := newExtension(demoConfig(), metadatatest.NewSettings(componenttest.NewTelemetry()), clock.NewFake(t0))
	u := budgetapi.Usage{Bytes: 4096, Items: 10}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ext.Record("checkout", budgetapi.SignalLogs, budgetapi.PriceHot, u)
		}
	})
}

func BenchmarkRecordOffered(b *testing.B) {
	ext, _ := newExtension(demoConfig(), metadatatest.NewSettings(componenttest.NewTelemetry()), clock.NewFake(t0))
	u := budgetapi.Usage{Bytes: 4096, Items: 10}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ext.RecordOffered("checkout", budgetapi.SignalLogs, u)
		}
	})
}

func BenchmarkDecision(b *testing.B) {
	ext, _ := newExtension(demoConfig(), metadatatest.NewSettings(componenttest.NewTelemetry()), clock.NewFake(t0))
	for i := range 1000 {
		ext.Record(fmt.Sprintf("svc-%d", i), budgetapi.SignalLogs, budgetapi.PriceHot, budgetapi.Usage{Bytes: 1})
		res := pcommon.NewResource()
		res.Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", i))
		ext.ResolveKey(res)
	}
	ext.tick(t0)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = ext.Decision("svc-500")
		}
	})
}

func TestApplyFleetTiers(t *testing.T) {
	res := decision.Result{Healthy: true, Ledgers: map[string]decision.LedgerResult{
		"a":      {Tier: 1, Internal: 1, Budget: 1},
		"b":      {Tier: 2, Internal: 2, Budget: 1},
		"exempt": {Tier: 0, Internal: 2, Budget: 1, Exempt: true},
		"free":   {Budget: 0},
	}}
	applyFleetTiers(&res, map[string]int{"a": 2, "b": 1, "exempt": 2, "free": 1, "unknown": 3})
	assert.Equal(t, 2, res.Ledgers["a"].Tier, "raised to the fleet maximum")
	assert.Equal(t, 2, res.Ledgers["b"].Tier, "never lowered by the fleet")
	assert.Equal(t, 0, res.Ledgers["exempt"].Tier)
	assert.Equal(t, 0, res.Ledgers["free"].Tier)
	assert.NotContains(t, res.Ledgers, "unknown")

	open := decision.Result{Healthy: false, Ledgers: map[string]decision.LedgerResult{"a": {Budget: 1}}}
	applyFleetTiers(&open, map[string]int{"a": 2})
	assert.Equal(t, 0, open.Ledgers["a"].Tier, "failing open wins")
}
