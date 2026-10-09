package budgetextension

import (
	"context"
	"encoding/binary"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/budgetapi"
)

// driver plays the processor side of the budgetapi contract, so the
// extension is tested without depending on the processor module: resolve,
// read the decision, drop logs below the tier's severity and sample by trace
// ID with the processor's formula (always pass and exemptions respected), and
// report one Record and RecordOffered per key per batch. Stamping is covered
// by the processor's own tests and by the end to end tests in examples/.
type driver struct {
	ledger budgetapi.Ledger
	out    *consumertest.LogsSink
}

type driverUsage struct {
	offered, kept budgetapi.Usage
	dropped       int64
}

func (d *driver) report(sig budgetapi.Signal, usage map[string]*driverUsage) {
	for token, u := range usage {
		d.ledger.RecordOffered(token, sig, u.offered)
		if u.kept.Items > 0 {
			d.ledger.Record(token, sig, budgetapi.PriceHot, u.kept)
		}
		if r, ok := d.ledger.(budgetapi.DropReporter); ok && u.dropped > 0 {
			r.RecordDropped(token, sig, budgetapi.DropSeverity, u.dropped)
		}
	}
}

func (d *driver) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	var sizer plog.ProtoMarshaler
	pass := d.ledger.(budgetapi.PolicyProvider).AlwaysPass().LogSeverity
	usage := map[string]*driverUsage{}
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		token := d.ledger.ResolveKey(rl.Resource())
		dec := d.ledger.Decision(token)
		u := usage[token]
		if u == nil {
			u = &driverUsage{}
			usage[token] = u
		}
		u.offered.Bytes += int64(sizer.ResourceLogsSize(rl))
		floor := dec.Actions.DropBelowSeverity
		sample := dec.Actions.Samples()
		threshold := uint64(dec.Actions.SampleRatio * (1 << 56))
		if !dec.Exempt && dec.Tier > 0 && d.ledger.Healthy() && (floor > 0 || sample) {
			rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
				sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
					sev := lr.SeverityNumber()
					if sev == plog.SeverityNumberUnspecified {
						sev = plog.SeverityNumberInfo
					}
					if sev >= pass {
						return false
					}
					if floor > 0 && sev < floor {
						u.dropped++
						return true
					}
					if tid := lr.TraceID(); sample && binary.BigEndian.Uint64(tid[8:16])&(1<<56-1) >= threshold {
						u.dropped++
						return true
					}
					return false
				})
				return sl.LogRecords().Len() == 0
			})
		}
		if rl.ScopeLogs().Len() == 0 {
			return true
		}
		u.kept.Bytes += int64(sizer.ResourceLogsSize(rl))
		for i := range rl.ScopeLogs().Len() {
			u.kept.Items += int64(rl.ScopeLogs().At(i).LogRecords().Len())
		}
		return false
	})
	d.report(budgetapi.SignalLogs, usage)
	if ld.ResourceLogs().Len() == 0 {
		return nil
	}
	return d.out.ConsumeLogs(ctx, ld)
}

func (d *driver) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	var sizer pmetric.ProtoMarshaler
	usage := map[string]*driverUsage{}
	for i := range md.ResourceMetrics().Len() {
		rm := md.ResourceMetrics().At(i)
		token := d.ledger.ResolveKey(rm.Resource())
		u := usage[token]
		if u == nil {
			u = &driverUsage{}
			usage[token] = u
		}
		size := int64(sizer.ResourceMetricsSize(rm))
		u.offered.Bytes += size
		u.kept.Bytes += size
		rh := budgetapi.MapHash(rm.Resource().Attributes(), "otelcol.budget.")
		for j := range rm.ScopeMetrics().Len() {
			sm := rm.ScopeMetrics().At(j)
			sh := budgetapi.NameHash(sm.Scope().Name())
			for k := range sm.Metrics().Len() {
				m := sm.Metrics().At(k)
				// gauges only: enough for the extension's pricing tests
				dps := m.Gauge().DataPoints()
				for p := range dps.Len() {
					u.kept.Items++
					u.kept.SeriesHash = append(u.kept.SeriesHash, budgetapi.SeriesHash(rh, sh, budgetapi.NameHash(m.Name()), budgetapi.MapHash(dps.At(p).Attributes(), "")))
				}
			}
		}
	}
	d.report(budgetapi.SignalMetrics, usage)
	return nil
}
