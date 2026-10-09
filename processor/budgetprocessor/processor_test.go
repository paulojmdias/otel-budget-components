package budgetprocessor

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
	"github.com/paulojmdias/otel-budget-processor/processor/budgetprocessor/internal/testutil"
)

var ledgerID = component.MustNewID("budget")

type harness struct {
	ledger *testutil.FakeLedger
	logs   *consumertest.LogsSink
	traces *consumertest.TracesSink
	mets   *consumertest.MetricsSink
	lp     processor.Logs
	tp     processor.Traces
	mp     processor.Metrics
}

func newHarness(tb testing.TB, mutate func(*Config), host component.Host) *harness {
	tb.Helper()
	h := &harness{ledger: testutil.NewFakeLedger(), logs: &consumertest.LogsSink{}, traces: &consumertest.TracesSink{}, mets: &consumertest.MetricsSink{}}
	if host == nil {
		host = testutil.Host{ledgerID: h.ledger}
	} else if th, ok := host.(testutil.Host); ok {
		// a prepared fake ledger, configured before the processors start
		if l, ok := th[ledgerID].(*testutil.FakeLedger); ok {
			h.ledger = l
		}
	}
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	if mutate != nil {
		mutate(cfg)
	}
	set := processortest.NewNopSettings(f.Type())
	var err error
	h.lp, err = f.CreateLogs(tb.Context(), set, cfg, h.logs)
	require.NoError(tb, err)
	h.tp, err = f.CreateTraces(tb.Context(), set, cfg, h.traces)
	require.NoError(tb, err)
	h.mp, err = f.CreateMetrics(tb.Context(), set, cfg, h.mets)
	require.NoError(tb, err)
	for _, c := range []component.Component{h.lp, h.tp, h.mp} {
		require.NoError(tb, c.Start(tb.Context(), host))
		tb.Cleanup(func() { require.NoError(tb, c.Shutdown(context.WithoutCancel(tb.Context()))) })
	}
	return h
}

func tier(n int, a budgetapi.Action) budgetapi.Decision {
	if a.SampleRatio == 0 {
		a.SampleRatio = 1
	}
	_ = a.Compile()
	return budgetapi.Decision{Tier: n, Actions: a}
}

func recordsFor(calls []testutil.RecordCall, key string) []testutil.RecordCall {
	var out []testutil.RecordCall
	for _, c := range calls {
		if c.Key == key {
			out = append(out, c)
		}
	}
	return out
}

func TestLogsSeverityDropAndStamp(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.ledger.Decisions["noisy"] = tier(1, budgetapi.Action{DropBelowSeverity: plog.SeverityNumberInfo})
	ld := plog.NewLogs()
	testutil.AddLogs(ld, "noisy",
		testutil.LogRecord{Severity: plog.SeverityNumberDebug},
		testutil.LogRecord{Severity: plog.SeverityNumberUnspecified}, // treated as INFO, kept
		testutil.LogRecord{Severity: plog.SeverityNumberWarn},
		testutil.LogRecord{Severity: plog.SeverityNumberError})
	testutil.AddLogs(ld, "noisy", testutil.LogRecord{Severity: plog.SeverityNumberTrace}) // fully dropped resource
	testutil.AddLogs(ld, "quiet", testutil.LogRecord{Severity: plog.SeverityNumberDebug}) // tier 0
	require.NoError(t, h.lp.ConsumeLogs(t.Context(), ld))

	out := h.logs.AllLogs()[0]
	require.Equal(t, 2, out.ResourceLogs().Len(), "empty resource removed")
	noisy := out.ResourceLogs().At(0)
	assert.Equal(t, 3, noisy.ScopeLogs().At(0).LogRecords().Len())
	k, _ := noisy.Resource().Attributes().Get(AttrKey)
	assert.Equal(t, "noisy", k.Str())
	tr, _ := noisy.Resource().Attributes().Get(AttrTier)
	assert.Equal(t, int64(1), tr.Int())
	_, divert := noisy.Resource().Attributes().Get(AttrDivert)
	assert.False(t, divert)

	// exactly one Record and one RecordOffered per key per batch
	recs := recordsFor(h.ledger.Records, "noisy")
	require.Len(t, recs, 1)
	assert.Equal(t, int64(3), recs[0].Usage.Items)
	assert.Equal(t, budgetapi.PriceHot, recs[0].Class)
	off := recordsFor(h.ledger.Offered, "noisy")
	require.Len(t, off, 1)
	assert.Equal(t, int64(5), off[0].Usage.Items)
	assert.Greater(t, off[0].Usage.Bytes, recs[0].Usage.Bytes-64)
	require.Len(t, recordsFor(h.ledger.Records, "quiet"), 1)
	assert.Equal(t, []testutil.DropCall{{Key: "noisy", Sig: budgetapi.SignalLogs, Reason: budgetapi.DropSeverity, N: 2}}, h.ledger.Drops)
}

func TestLogsAlwaysPassAndSkip(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.ledger.Decisions["a"] = tier(1, budgetapi.Action{DropBelowSeverity: plog.SeverityNumberFatal, SampleRatio: 0.0001})
	ld := plog.NewLogs()
	testutil.AddLogs(ld, "a", testutil.Repeat(50, testutil.LogRecord{Severity: plog.SeverityNumberError})...)
	require.NoError(t, h.lp.ConsumeLogs(t.Context(), ld))
	assert.Equal(t, 50, h.logs.LogRecordCount(), "ERROR always passes")

	ld = plog.NewLogs()
	testutil.AddLogs(ld, "a", testutil.Repeat(10, testutil.LogRecord{Severity: plog.SeverityNumberInfo})...)
	require.NoError(t, h.lp.ConsumeLogs(t.Context(), ld))
	assert.Len(t, h.logs.AllLogs(), 1, "everything dropped, batch skipped")
	assert.Equal(t, 50, h.logs.LogRecordCount())
}

func TestLogsSampling(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.ledger.Decisions["a"] = tier(1, budgetapi.Action{SampleRatio: 0.25})
	ld := plog.NewLogs()
	testutil.AddLogs(ld, "a", testutil.Repeat(4000, testutil.LogRecord{Severity: plog.SeverityNumberInfo, Body: "x"})...)
	require.NoError(t, h.lp.ConsumeLogs(t.Context(), ld))
	kept := h.logs.LogRecordCount()
	assert.InDelta(t, 1000, kept, 120)
	drops := h.ledger.Drops[0]
	assert.Equal(t, budgetapi.DropSample, drops.Reason)
	assert.Equal(t, int64(4000-kept), drops.N)
}

// The same trace ID gets the same decision for logs and spans.
func TestSamplingConsistentAcrossSignals(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.ledger.Decisions["a"] = tier(1, budgetapi.Action{SampleRatio: 0.5})
	ld := plog.NewLogs()
	td := ptrace.NewTraces()
	var recs []testutil.LogRecord
	var spans []testutil.Span
	for i := uint64(1); i <= 500; i++ {
		recs = append(recs, testutil.LogRecord{Severity: plog.SeverityNumberInfo, TraceID: i})
		spans = append(spans, testutil.Span{TraceID: i}, testutil.Span{TraceID: i})
	}
	testutil.AddLogs(ld, "a", recs...)
	testutil.AddSpans(td, "a", spans...)
	require.NoError(t, h.lp.ConsumeLogs(t.Context(), ld))
	require.NoError(t, h.tp.ConsumeTraces(t.Context(), td))

	logIDs := map[[16]byte]bool{}
	lrs := h.logs.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	for i := range lrs.Len() {
		logIDs[lrs.At(i).TraceID()] = true
	}
	perTrace := map[[16]byte]int{}
	ss := h.traces.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	for i := range ss.Len() {
		perTrace[ss.At(i).TraceID()]++
	}
	assert.Len(t, perTrace, len(logIDs))
	for id, n := range perTrace {
		assert.True(t, logIDs[id], "span kept but log dropped for the same trace")
		assert.Equal(t, 2, n, "sampled traces are never partial")
	}
	assert.InDelta(t, 250, len(logIDs), 60)
}

func TestTracesErrorsAlwaysPass(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.ledger.Decisions["a"] = tier(1, budgetapi.Action{SampleRatio: 0.000001})
	td := ptrace.NewTraces()
	testutil.AddSpans(td, "a", testutil.Span{TraceID: 1, Error: true}, testutil.Span{TraceID: 2}, testutil.Span{TraceID: 3})
	require.NoError(t, h.tp.ConsumeTraces(t.Context(), td))
	assert.Equal(t, 1, h.traces.SpanCount())
	assert.Equal(t, int64(1), recordsFor(h.ledger.Records, "a")[0].Usage.Items)

	td = ptrace.NewTraces()
	testutil.AddSpans(td, "a", testutil.Span{TraceID: 2})
	require.NoError(t, h.tp.ConsumeTraces(t.Context(), td))
	assert.Equal(t, 1, h.traces.SpanCount(), "fully sampled out batch is skipped")
}

func TestDivert(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.ledger.Decisions["a"] = tier(2, budgetapi.Action{Divert: true})
	ld := plog.NewLogs()
	testutil.AddLogs(ld, "a", testutil.LogRecord{Severity: plog.SeverityNumberDebug})
	require.NoError(t, h.lp.ConsumeLogs(t.Context(), ld))
	d, ok := h.logs.AllLogs()[0].ResourceLogs().At(0).Resource().Attributes().Get(AttrDivert)
	require.True(t, ok)
	assert.True(t, d.Bool())
	assert.Empty(t, h.ledger.Records, "diverted data is accounted by the cold pipeline")
	assert.Len(t, h.ledger.Offered, 1)
}

func TestColdAccountOnly(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.PriceClass = "cold"; c.Enforce = false }, nil)
	h.ledger.Decisions["a"] = tier(2, budgetapi.Action{Divert: true, DropBelowSeverity: plog.SeverityNumberFatal})
	ld := plog.NewLogs()
	testutil.AddLogs(ld, "a", testutil.LogRecord{Severity: plog.SeverityNumberDebug})
	require.NoError(t, h.lp.ConsumeLogs(t.Context(), ld))
	assert.Equal(t, 1, h.logs.LogRecordCount(), "enforce false applies nothing")
	require.Len(t, h.ledger.Records, 1)
	assert.Equal(t, budgetapi.PriceCold, h.ledger.Records[0].Class)
	assert.Empty(t, h.ledger.Offered, "offered volume is counted once, in the hot pipeline")
}

func TestExemptAndFailOpen(t *testing.T) {
	h := newHarness(t, nil, nil)
	d := tier(1, budgetapi.Action{DropBelowSeverity: plog.SeverityNumberFatal})
	d.Exempt = true
	h.ledger.Decisions["a"] = d
	ld := plog.NewLogs()
	testutil.AddLogs(ld, "a", testutil.LogRecord{Severity: plog.SeverityNumberDebug})
	require.NoError(t, h.lp.ConsumeLogs(t.Context(), ld))
	assert.Equal(t, 1, h.logs.LogRecordCount())

	h.ledger.Decisions["a"] = tier(1, budgetapi.Action{DropBelowSeverity: plog.SeverityNumberFatal})
	h.ledger.Unhealthy = true
	ld = plog.NewLogs()
	testutil.AddLogs(ld, "a", testutil.LogRecord{Severity: plog.SeverityNumberDebug})
	require.NoError(t, h.lp.ConsumeLogs(t.Context(), ld))
	assert.Equal(t, 2, h.logs.LogRecordCount(), "unhealthy ledger fails open")
	tr, _ := h.logs.AllLogs()[1].ResourceLogs().At(0).Resource().Attributes().Get(AttrTier)
	assert.Equal(t, int64(0), tr.Int())
}

func TestMissingLedger(t *testing.T) {
	h := newHarness(t, nil, testutil.Host{})
	ld := plog.NewLogs()
	testutil.AddLogs(ld, "a", testutil.LogRecord{Severity: plog.SeverityNumberDebug})
	require.NoError(t, h.lp.ConsumeLogs(t.Context(), ld))
	assert.Equal(t, 1, h.logs.LogRecordCount(), "fail open passes data untouched")
	_, stamped := h.logs.AllLogs()[0].ResourceLogs().At(0).Resource().Attributes().Get(AttrKey)
	assert.False(t, stamped)
	td := ptrace.NewTraces()
	testutil.AddSpans(td, "a", testutil.Span{TraceID: 1})
	require.NoError(t, h.tp.ConsumeTraces(t.Context(), td))
	md := pmetric.NewMetrics()
	testutil.AddGauges(md, "a", 1, "m")
	require.NoError(t, h.mp.ConsumeMetrics(t.Context(), md))

	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.FailOpen = false
	p, err := f.CreateLogs(t.Context(), processortest.NewNopSettings(f.Type()), cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.Error(t, p.Start(t.Context(), testutil.Host{}))
	require.Error(t, p.Start(t.Context(), testutil.Host{ledgerID: testutil.NopComponent{}}))
}

func TestLogsSizedOnce(t *testing.T) {
	h := newHarness(t, nil, nil)
	exported := func(i int) int64 {
		return int64(logSizer.ResourceLogsSize(h.logs.AllLogs()[i].ResourceLogs().At(0)))
	}

	// nothing removed: kept reuses the offered size, stamps included
	ld := plog.NewLogs()
	testutil.AddLogs(ld, "a", testutil.Repeat(10, testutil.LogRecord{Severity: plog.SeverityNumberInfo, Body: "x"})...)
	require.NoError(t, h.lp.ConsumeLogs(t.Context(), ld))
	off, kept := h.ledger.Offered[0].Usage, recordsFor(h.ledger.Records, "a")[0].Usage
	assert.Equal(t, off, kept)
	assert.Equal(t, exported(0), kept.Bytes, "kept is what is exported")

	// records removed: kept is measured again
	h.ledger.Reset()
	h.ledger.Decisions["a"] = tier(1, budgetapi.Action{DropBelowSeverity: plog.SeverityNumberWarn})
	ld = plog.NewLogs()
	testutil.AddLogs(ld, "a", append(testutil.Repeat(5, testutil.LogRecord{Severity: plog.SeverityNumberInfo, Body: "x"}),
		testutil.Repeat(5, testutil.LogRecord{Severity: plog.SeverityNumberWarn, Body: "x"})...)...)
	require.NoError(t, h.lp.ConsumeLogs(t.Context(), ld))
	off, kept = h.ledger.Offered[0].Usage, recordsFor(h.ledger.Records, "a")[0].Usage
	assert.Equal(t, int64(10), off.Items)
	assert.Equal(t, int64(5), kept.Items)
	assert.Equal(t, exported(1), kept.Bytes)
	assert.Greater(t, off.Bytes, kept.Bytes)
}

func pricedSeries() testutil.Host {
	l := testutil.NewFakeLedger()
	l.SeriesPriced = true
	return testutil.Host{ledgerID: l}
}

func TestMetricsSeriesOnlyWhenPriced(t *testing.T) {
	h := newHarness(t, nil, nil)
	md := pmetric.NewMetrics()
	testutil.AddGauges(md, "a", 3, "http_requests")
	require.NoError(t, h.mp.ConsumeMetrics(t.Context(), md))
	rec := recordsFor(h.ledger.Records, "a")[0]
	assert.Equal(t, int64(3), rec.Usage.Items)
	assert.Empty(t, rec.Usage.SeriesHash, "no series hashing when series are not priced")
}

func TestMetricsDropAndSeries(t *testing.T) {
	h := newHarness(t, nil, pricedSeries())
	h.ledger.Decisions["a"] = tier(1, budgetapi.Action{DropMetrics: []string{"go_*", "*_bucket"}})
	md := pmetric.NewMetrics()
	testutil.AddGauges(md, "a", 3, "go_gc", "http_bucket", "http_requests", "queue_size")
	require.NoError(t, h.mp.ConsumeMetrics(t.Context(), md))
	out := h.mets.AllMetrics()[0]
	assert.Equal(t, 2, out.MetricCount())
	rec := recordsFor(h.ledger.Records, "a")[0]
	assert.Equal(t, int64(6), rec.Usage.Items)
	require.Len(t, rec.Usage.SeriesHash, 6)
	seen := map[uint64]bool{}
	for _, s := range rec.Usage.SeriesHash {
		seen[s] = true
	}
	assert.Len(t, seen, 6, "distinct series")
	assert.Equal(t, int64(12), h.ledger.Offered[0].Usage.Items)
	assert.Equal(t, budgetapi.DropMetricGlob, h.ledger.Drops[0].Reason)
	assert.Equal(t, int64(6), h.ledger.Drops[0].N)

	// series identity ignores budget stamps
	md2 := pmetric.NewMetrics()
	testutil.AddGauges(md2, "a", 3, "http_requests")
	h.ledger.Decisions["a"] = tier(2, budgetapi.Action{})
	h.ledger.Reset()
	require.NoError(t, h.mp.ConsumeMetrics(t.Context(), md2))
	assert.Subset(t, rec.Usage.SeriesHash, recordsFor(h.ledger.Records, "a")[0].Usage.SeriesHash)

	// all dropped skips the batch
	md3 := pmetric.NewMetrics()
	testutil.AddGauges(md3, "a", 1, "go_x")
	h.ledger.Decisions["a"] = tier(1, budgetapi.Action{DropMetrics: []string{"go_*"}})
	require.NoError(t, h.mp.ConsumeMetrics(t.Context(), md3))
	assert.Len(t, h.mets.AllMetrics(), 2)
}

func TestMetricTypes(t *testing.T) {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	sm.Metrics().AppendEmpty().SetEmptySum().DataPoints().AppendEmpty()
	sm.Metrics().AppendEmpty().SetEmptyHistogram().DataPoints().AppendEmpty()
	sm.Metrics().AppendEmpty().SetEmptyExponentialHistogram().DataPoints().AppendEmpty()
	sm.Metrics().AppendEmpty().SetEmptySummary().DataPoints().AppendEmpty()
	sm.Metrics().AppendEmpty()
	rm := md.ResourceMetrics().At(0)
	assert.Equal(t, int64(4), resourcePoints(rm))
	assert.Len(t, appendSeries(nil, rm), 4)
}

func TestConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, cfg.Validate())
	cfg.PriceClass = "warm"
	require.Error(t, cfg.Validate())
	cfg = &Config{PriceClass: "hot"}
	require.ErrorContains(t, cfg.Validate(), "ledger is required")
	assert.Equal(t, uint64(0), sampleThreshold(0))
	assert.Equal(t, uint64(1<<56), sampleThreshold(1))
}

// BenchmarkProcessor covers each signal at both enforcement levels and two
// batch shapes: one resource with many records, and many small resources.
// Tier 1 drops below DEBUG (nothing here), samples logs and spans at 50%,
// and drops go_* metrics (none here). series=on prices active series.
func BenchmarkProcessor(b *testing.B) {
	shapes := []struct {
		name               string
		resources, records int
	}{{"1x10000", 1, 10000}, {"100x100", 100, 100}}
	for _, sig := range []string{"logs", "traces", "metrics"} {
		for _, sh := range shapes {
			for _, t := range []int{0, 1} {
				series := []bool{false}
				if sig == "metrics" {
					series = []bool{false, true}
				}
				for _, priced := range series {
					name := fmt.Sprintf("%s/%s/tier%d", sig, sh.name, t)
					if sig == "metrics" {
						name += fmt.Sprintf("/series=%v", priced)
					}
					b.Run(name, func(b *testing.B) {
						benchProcessor(b, sig, sh.resources, sh.records, t, priced)
					})
				}
			}
		}
	}
}

// BenchmarkAppendSeries isolates series hashing (it only reads the batch):
// 10 metrics with 1,000 points each, two attributes per point.
func BenchmarkAppendSeries(b *testing.B) {
	md := pmetric.NewMetrics()
	rm := testutil.AddGauges(md, "svc", 1000, "m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9")
	sms := rm.ScopeMetrics()
	for i := range sms.Len() {
		ms := sms.At(i).Metrics()
		for j := range ms.Len() {
			dps := ms.At(j).Gauge().DataPoints()
			for k := range dps.Len() {
				dps.At(k).Attributes().PutStr("route", "/api/v1/items")
			}
		}
	}
	out := make([]uint64, 0, 10000)
	b.ReportAllocs()
	for b.Loop() {
		out = appendSeries(out[:0], rm)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*10000), "ns/point")
}

func benchProcessor(b *testing.B, sig string, resources, records, t int, priced bool) {
	l := testutil.NewFakeLedger()
	l.SeriesPriced = priced
	h := newHarness(b, nil, testutil.Host{ledgerID: l})
	ld, td, md := plog.NewLogs(), ptrace.NewTraces(), pmetric.NewMetrics()
	spans := make([]testutil.Span, records)
	for i := range spans {
		spans[i] = testutil.Span{TraceID: uint64(i + 1)}
	}
	for r := range resources {
		svc := fmt.Sprintf("svc-%d", r)
		if t == 1 {
			h.ledger.Decisions[svc] = tier(1, budgetapi.Action{DropBelowSeverity: plog.SeverityNumberDebug, SampleRatio: 0.5, DropMetrics: []string{"go_*"}})
		}
		switch sig {
		case "logs":
			testutil.AddLogs(ld, svc, testutil.Repeat(records, testutil.LogRecord{Severity: plog.SeverityNumberInfo, Body: "bench"})...)
		case "traces":
			testutil.AddSpans(td, svc, spans...)
		default:
			testutil.AddGauges(md, svc, records, "http_requests")
		}
	}
	ctx := b.Context()
	// collect only while the timer is stopped: the batch copies' garbage
	// would otherwise be collected inside the timed part
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		b.StopTimer()
		h.ledger.Reset()
		i++
		if i%16 == 0 {
			runtime.GC()
		}
		var err error
		switch sig {
		case "logs":
			c := plog.NewLogs()
			ld.CopyTo(c)
			b.StartTimer()
			err = h.lp.ConsumeLogs(ctx, c)
		case "traces":
			c := ptrace.NewTraces()
			td.CopyTo(c)
			b.StartTimer()
			err = h.tp.ConsumeTraces(ctx, c)
		default:
			c := pmetric.NewMetrics()
			md.CopyTo(c)
			b.StartTimer()
			err = h.mp.ConsumeMetrics(ctx, c)
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*resources*records), "ns/record")
}
