//go:build results

package budgetextension

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/clock"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/costmodel"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/metadatatest"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/testutil"
)

// Measurements for docs/results.md: `make results`.

var steps = int(time.Hour / interval) // one demo period

// mix is the traffic of one 5 s step.
type mix struct{ debug, info, warn, errors int }

var scenarios = []struct {
	name string
	at   func(step int) mix
}{
	{"normal", func(int) mix { return mix{info: 100, warn: 5, errors: 1} }},
	{"noisy deploy", func(i int) mix {
		m := mix{info: 100, warn: 5, errors: 1}
		if i >= 120 { // from minute 10
			m.debug = 400
		}
		return m
	}},
	{"incident", func(i int) mix {
		if i >= 240 && i < 480 { // minutes 20 to 40
			return mix{info: 300, warn: 15, errors: 50}
		}
		return mix{info: 100, warn: 5, errors: 1}
	}},
	{"growth", func(i int) mix {
		if i >= 120 {
			return mix{info: 300, warn: 15, errors: 3}
		}
		return mix{info: 100, warn: 5, errors: 1}
	}},
}

var traceSeq uint64

func records(m mix) []testutil.LogRecord {
	var out []testutil.LogRecord
	add := func(n int, sev plog.SeverityNumber, body string) {
		for range n {
			traceSeq++
			out = append(out, testutil.LogRecord{Severity: sev, Body: fmt.Sprintf("%s request=%d user=u%d latency_ms=%d", body, traceSeq, traceSeq%977, traceSeq%300), TraceID: traceSeq})
		}
	}
	add(m.debug, plog.SeverityNumberDebug, "cache lookup detail")
	add(m.info, plog.SeverityNumberInfo, "request served")
	add(m.warn, plog.SeverityNumberWarn, "slow dependency")
	add(m.errors, plog.SeverityNumberError, "payment failed")
	return out
}

type tally struct{ offered, kept [4]int } // by severity class: debug, info, warn, error

func class(sev plog.SeverityNumber) int {
	switch {
	case sev >= plog.SeverityNumberError:
		return 3
	case sev >= plog.SeverityNumberWarn:
		return 2
	case sev >= plog.SeverityNumberInfo:
		return 1
	}
	return 0
}

func pct(k, o int) float64 {
	if o == 0 {
		return 100
	}
	return 100 * float64(k) / float64(o)
}

type outcome struct {
	SpendVsBudget float64 `json:"spend_vs_budget"`
	DebugKeptPct  float64 `json:"debug_kept_pct"`
	InfoKeptPct   float64 `json:"info_kept_pct"`
	WarnKeptPct   float64 `json:"warn_kept_pct"`
	ErrorKeptPct  float64 `json:"error_kept_pct"`
	Transitions   int     `json:"tier_transitions,omitempty"`
}

func (t tally) outcome(spend, budget int64) outcome {
	return outcome{
		SpendVsBudget: float64(spend) / float64(budget),
		DebugKeptPct:  pct(t.kept[0], t.offered[0]),
		InfoKeptPct:   pct(t.kept[1], t.offered[1]),
		WarnKeptPct:   pct(t.kept[2], t.offered[2]),
		ErrorKeptPct:  pct(t.kept[3], t.offered[3]),
	}
}

func logsOf(recs []testutil.LogRecord) plog.Logs {
	ld := plog.NewLogs()
	testutil.AddLogs(ld, "svc", recs...)
	return ld
}

func count(ld plog.Logs, into *[4]int) {
	rls := ld.ResourceLogs()
	for i := range rls.Len() {
		sls := rls.At(i).ScopeLogs()
		for j := range sls.Len() {
			lrs := sls.At(j).LogRecords()
			for k := range lrs.Len() {
				into[class(lrs.At(k).SeverityNumber())]++
			}
		}
	}
}

// static applies a fixed rule to every batch and prices what is left.
func static(at func(int) mix, keep func(plog.LogRecord) bool, price int64) (tally, int64) {
	traceSeq = 0 // identical traffic for every strategy
	var t tally
	var sizer plog.ProtoMarshaler
	var bytes int64
	for i := range steps {
		ld := logsOf(records(at(i)))
		count(ld, &t.offered)
		ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
			rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
				sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool { return !keep(lr) })
				return sl.LogRecords().Len() == 0
			})
			return rl.ScopeLogs().Len() == 0
		})
		count(ld, &t.kept)
		bytes += int64(sizer.LogsSize(ld))
	}
	return t, costmodel.Scaled(bytes, price, 1_000_000_000).Micro
}

func sampled(lr plog.LogRecord, ratio float64) bool {
	tid := lr.TraceID()
	return binary.BigEndian.Uint64(tid[8:16])&(1<<56-1) < uint64(ratio*(1<<56))
}

func comparisonConfig(budget float64) *Config {
	cfg := demoConfig()
	cfg.Budgets[0].Budget = budget
	cfg.Tiers = []TierConfig{
		{Threshold: 1.0, Actions: ActionsConfig{DropBelowSeverity: "INFO"}},
		{Threshold: 2.0, Actions: ActionsConfig{DropBelowSeverity: "WARN", SampleRatio: 0.25}},
	}
	return cfg
}

// budgeted runs the real extension on the fake clock with the test driver.
// Spend is priced from the exported batches with the same sizer as static.
func budgeted(t *testing.T, cfg *Config, at func(int) mix, price int64) (tally, int64, int) {
	traceSeq = 0
	e := newEnv(t, cfg, nil)
	var tl tally
	var sizer plog.ProtoMarshaler
	var bytes int64
	for i := range steps {
		ld := logsOf(records(at(i)))
		count(ld, &tl.offered)
		before := len(e.out.AllLogs())
		require.NoError(t, e.lp.ConsumeLogs(t.Context(), ld))
		if len(e.out.AllLogs()) > before {
			sent := e.out.AllLogs()[len(e.out.AllLogs())-1]
			count(sent, &tl.kept)
			bytes += int64(sizer.LogsSize(sent))
		}
		e.step()
	}
	e.ext.tickMu.Lock()
	transitions := e.ext.engine.Transitions()
	e.ext.tickMu.Unlock()
	return tl, costmodel.Scaled(bytes, price, 1_000_000_000).Micro, transitions
}

func TestResults(t *testing.T) {
	out := map[string]any{}
	price := costmodel.ToMicro(0.40)

	// budget sized so normal traffic burns 0.5 over the hour
	_, normalSpend := static(scenarios[0].at, func(plog.LogRecord) bool { return true }, price)
	budget := float64(normalSpend) * 2 / 1e6
	budgetMicro := costmodel.ToMicro(budget)

	comparison := map[string]map[string]outcome{}
	for _, sc := range scenarios {
		row := map[string]outcome{}
		tl, sp := static(sc.at, func(plog.LogRecord) bool { return true }, price)
		row["none"] = tl.outcome(sp, budgetMicro)
		tl, sp = static(sc.at, func(lr plog.LogRecord) bool { return lr.SeverityNumber() >= plog.SeverityNumberInfo }, price)
		row["static filter (drop below INFO)"] = tl.outcome(sp, budgetMicro)
		tl, sp = static(sc.at, func(lr plog.LogRecord) bool { return sampled(lr, 0.25) }, price)
		row["static sampler (25%)"] = tl.outcome(sp, budgetMicro)
		var tr int
		tl, sp, tr = budgeted(t, comparisonConfig(budget), sc.at, price)
		o := tl.outcome(sp, budgetMicro)
		o.Transitions = tr
		row["budget"] = o
		comparison[sc.name] = row
	}
	out["comparison"] = comparison

	// hysteresis: a producer that is noisy for 2 minutes and quiet for 2
	osc := func(i int) mix {
		if (i/24)%2 == 0 {
			return mix{debug: 400, info: 100, warn: 5, errors: 1}
		}
		return mix{info: 100, warn: 5, errors: 1}
	}
	_, _, with := budgeted(t, comparisonConfig(budget), osc, price)
	noHyst := comparisonConfig(budget)
	noHyst.Hysteresis = HysteresisConfig{Ratio: 0.999999, MinDwell: 0}
	_, _, without := budgeted(t, noHyst, osc, price)
	out["hysteresis_oscillating"] = map[string]int{"transitions_with": with, "transitions_without": without}

	out["scale_10k_keys_demo_windows"] = scale(t, 10000)
	defaults := demoConfig()
	defaults.Period = "monthly"
	defaults.Windows = WindowsConfig{Short: time.Hour, Long: 6 * time.Hour}
	defaults.DecisionInterval = 10 * time.Second
	defaults.Hysteresis.MinDwell = 30 * time.Minute
	defaults.StaleAfter, defaults.FailOpenAfter = 5*time.Minute, 30*time.Minute
	out["scale_10k_keys_default_windows"] = scaleWith(t, defaults, 10000)

	b, err := json.MarshalIndent(out, "", "  ")
	require.NoError(t, err)
	t.Log(string(b))
	if p := os.Getenv("RESULTS_OUT"); p != "" {
		require.NoError(t, os.WriteFile(p, b, 0o600))
	}
}

// scale records usage for n keys and times one decision tick.
func scale(t *testing.T, n int) map[string]any { return scaleWith(t, demoConfig(), n) }

func scaleWith(t *testing.T, cfg *Config, n int) map[string]any {
	cfg.MaxKeys = n
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	ext, err := newExtension(cfg, metadatatest.NewSettings(componenttest.NewTelemetry()), clock.NewFake(t0))
	require.NoError(t, err)
	tokens := make([]string, n)
	for i := range n {
		res := pcommon.NewResource()
		res.Attributes().PutStr("service.name", fmt.Sprintf("svc-%05d", i))
		tokens[i] = ext.ResolveKey(res)
		ext.RecordOffered(tokens[i], budgetapi.SignalLogs, budgetapi.Usage{Bytes: 4096, Items: 10})
		ext.Record(tokens[i], budgetapi.SignalLogs, budgetapi.PriceHot, budgetapi.Usage{Bytes: 4096, Items: 10})
	}
	var ticks []time.Duration
	for i := range 5 {
		now := t0.Add(time.Duration(i+1) * interval)
		start := time.Now()
		ext.tick(now)
		ticks = append(ticks, time.Since(start))
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(ext)
	return map[string]any{
		"keys":              n,
		"tick_ms":           durationsMs(ticks),
		"heap_mb":           float64(after.HeapAlloc-before.HeapAlloc) / (1 << 20),
		"decision_interval": cfg.DecisionInterval.String(),
	}
}

func durationsMs(ds []time.Duration) []float64 {
	out := make([]float64, len(ds))
	for i, d := range ds {
		out[i] = float64(d.Microseconds()) / 1000
	}
	return out
}
