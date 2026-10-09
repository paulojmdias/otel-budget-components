package budgetprocessor // import "github.com/paulojmdias/otel-budget-processor/processor/budgetprocessor"

import (
	"context"
	"slices"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
)

var metricSizer pmetric.ProtoMarshaler

const stampPrefix = "otelcol.budget."

// forEachPointAttrs calls f with the attributes of every data point.
func forEachPointAttrs(m pmetric.Metric, f func(pcommon.Map)) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := range dps.Len() {
			f(dps.At(i).Attributes())
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := range dps.Len() {
			f(dps.At(i).Attributes())
		}
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := range dps.Len() {
			f(dps.At(i).Attributes())
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := range dps.Len() {
			f(dps.At(i).Attributes())
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := range dps.Len() {
			f(dps.At(i).Attributes())
		}
	}
}

func pointCount(m pmetric.Metric) int64 {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return int64(m.Gauge().DataPoints().Len())
	case pmetric.MetricTypeSum:
		return int64(m.Sum().DataPoints().Len())
	case pmetric.MetricTypeHistogram:
		return int64(m.Histogram().DataPoints().Len())
	case pmetric.MetricTypeExponentialHistogram:
		return int64(m.ExponentialHistogram().DataPoints().Len())
	case pmetric.MetricTypeSummary:
		return int64(m.Summary().DataPoints().Len())
	}
	return 0
}

func resourcePoints(rm pmetric.ResourceMetrics) int64 {
	var n int64
	sms := rm.ScopeMetrics()
	for i := range sms.Len() {
		ms := sms.At(i).Metrics()
		for j := range ms.Len() {
			n += pointCount(ms.At(j))
		}
	}
	return n
}

func (p *budgetProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	var b batchUsage
	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		if p.ledger == nil {
			return false
		}
		token := p.ledger.ResolveKey(rm.Resource())
		d, enforce := p.decide(token)
		u := b.get(token)
		divert := enforce && d.Actions.Divert
		// stamped before sizing, so the size is measured once and reused
		// for kept bytes unless metrics were removed
		stamp(rm.Resource(), token, d, divert)
		size, points := int64(metricSizer.ResourceMetricsSize(rm)), resourcePoints(rm)
		u.offered.Bytes += size
		u.offered.Items += points
		if enforce && len(d.Actions.DropMetrics) > 0 {
			if removed, changed := dropMetrics(rm, &d.Actions, u); changed {
				if rm.ScopeMetrics().Len() == 0 {
					return true
				}
				size, points = int64(metricSizer.ResourceMetricsSize(rm)), points-removed
			}
		}
		if !divert {
			u.kept.Bytes += size
			u.kept.Items += points
			if p.series {
				u.kept.SeriesHash = appendSeries(slices.Grow(u.kept.SeriesHash, int(points)), rm)
			}
		}
		return false
	})
	p.flush(budgetapi.SignalMetrics, &b)
	if md.ResourceMetrics().Len() == 0 {
		return md, processorhelper.ErrSkipProcessingData
	}
	return md, nil
}

// dropMetrics removes matching metrics and returns the points removed and
// whether anything was removed (a metric can have no points).
func dropMetrics(rm pmetric.ResourceMetrics, a *budgetapi.Action, u *keyUsage) (points int64, changed bool) {
	rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
		sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
			if a.DropsMetric(m.Name()) {
				n := pointCount(m)
				u.dropped[budgetapi.DropMetricGlob] += n
				points += n
				changed = true
				return true
			}
			return false
		})
		return sm.Metrics().Len() == 0
	})
	return points, changed
}

// appendSeries adds the identity of every surviving series. Budget stamps
// are excluded so a tier change does not create new series.
func appendSeries(out []uint64, rm pmetric.ResourceMetrics) []uint64 {
	rh := budgetapi.MapHash(rm.Resource().Attributes(), stampPrefix)
	sms := rm.ScopeMetrics()
	for i := range sms.Len() {
		sm := sms.At(i)
		sh := budgetapi.NameHash(sm.Scope().Name())
		ms := sm.Metrics()
		for j := range ms.Len() {
			m := ms.At(j)
			nh := budgetapi.NameHash(m.Name())
			forEachPointAttrs(m, func(attrs pcommon.Map) {
				out = append(out, budgetapi.SeriesHash(rh, sh, nh, budgetapi.MapHash(attrs, "")))
			})
		}
	}
	return out
}
