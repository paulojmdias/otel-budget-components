package budgetprocessor // import "github.com/paulojmdias/otel-budget-components/processor/budgetprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/budgetapi"
)

var traceSizer ptrace.ProtoMarshaler

func spanCount(rs ptrace.ResourceSpans) int64 {
	var n int64
	sss := rs.ScopeSpans()
	for i := range sss.Len() {
		n += int64(sss.At(i).Spans().Len())
	}
	return n
}

func (p *budgetProcessor) processTraces(_ context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	var b batchUsage
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		if p.ledger == nil {
			return false
		}
		token := p.ledger.ResolveKey(rs.Resource())
		d, enforce := p.decide(token)
		u := b.get(token)
		divert := enforce && d.Actions.Divert
		// stamped before sizing, so the size is measured once and reused
		// for kept bytes unless spans were removed
		stamp(rs.Resource(), token, d, divert)
		size, items := int64(traceSizer.ResourceSpansSize(rs)), spanCount(rs)
		u.offered.Bytes += size
		u.offered.Items += items
		if enforce {
			if removed := p.filterSpans(rs, d.Actions, u); removed > 0 {
				if rs.ScopeSpans().Len() == 0 {
					return true
				}
				size, items = int64(traceSizer.ResourceSpansSize(rs)), items-removed
			}
		}
		if !divert {
			u.kept.Bytes += size
			u.kept.Items += items
		}
		return false
	})
	p.flush(budgetapi.SignalTraces, &b)
	if td.ResourceSpans().Len() == 0 {
		return td, processorhelper.ErrSkipProcessingData
	}
	return td, nil
}

// filterSpans samples spans and returns how many it removed.
func (p *budgetProcessor) filterSpans(rs ptrace.ResourceSpans, a budgetapi.Action, u *keyUsage) int64 {
	if !a.Samples() {
		return 0
	}
	var removed int64
	threshold := sampleThreshold(a.SampleRatio)
	keepErrors := p.policy.SpanStatusError
	rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
		ss.Spans().RemoveIf(func(s ptrace.Span) bool {
			if keepErrors && s.Status().Code() == ptrace.StatusCodeError {
				return false
			}
			if keepTrace(s.TraceID(), threshold) {
				return false
			}
			u.dropped[budgetapi.DropSample]++
			removed++
			return true
		})
		return ss.Spans().Len() == 0
	})
	return removed
}
