package budgetprocessor // import "github.com/paulojmdias/otel-budget-processor/processor/budgetprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
)

var logSizer plog.ProtoMarshaler

func logRecordCount(rl plog.ResourceLogs) int64 {
	var n int64
	sls := rl.ScopeLogs()
	for i := range sls.Len() {
		n += int64(sls.At(i).LogRecords().Len())
	}
	return n
}

func (p *budgetProcessor) processLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	var b batchUsage
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		if p.ledger == nil {
			return false
		}
		token := p.ledger.ResolveKey(rl.Resource())
		d, enforce := p.decide(token)
		u := b.get(token)
		divert := enforce && d.Actions.Divert
		// stamped before sizing, so the size is measured once and reused
		// for kept bytes unless records were removed
		stamp(rl.Resource(), token, d, divert)
		size, items := int64(logSizer.ResourceLogsSize(rl)), logRecordCount(rl)
		u.offered.Bytes += size
		u.offered.Items += items
		if enforce {
			if removed := p.filterLogs(rl, d.Actions, u); removed > 0 {
				if rl.ScopeLogs().Len() == 0 {
					return true
				}
				size, items = int64(logSizer.ResourceLogsSize(rl)), items-removed
			}
		}
		if !divert {
			u.kept.Bytes += size
			u.kept.Items += items
		}
		return false
	})
	p.flush(budgetapi.SignalLogs, &b)
	if ld.ResourceLogs().Len() == 0 {
		return ld, processorhelper.ErrSkipProcessingData
	}
	return ld, nil
}

// filterLogs applies the actions and returns how many records it removed.
func (p *budgetProcessor) filterLogs(rl plog.ResourceLogs, a budgetapi.Action, u *keyUsage) int64 {
	floor := a.DropBelowSeverity
	sample := a.Samples()
	if floor == 0 && !sample {
		return 0
	}
	var removed int64
	threshold := sampleThreshold(a.SampleRatio)
	pass := p.policy.LogSeverity
	rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
		sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
			sev := lr.SeverityNumber()
			if sev == plog.SeverityNumberUnspecified {
				sev = plog.SeverityNumberInfo
			}
			if pass > 0 && sev >= pass {
				return false
			}
			if floor > 0 && sev < floor {
				u.dropped[budgetapi.DropSeverity]++
				removed++
				return true
			}
			if sample && !keepLog(lr, threshold) {
				u.dropped[budgetapi.DropSample]++
				removed++
				return true
			}
			return false
		})
		return sl.LogRecords().Len() == 0
	})
	return removed
}
