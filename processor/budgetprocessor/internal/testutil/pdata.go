package testutil // import "github.com/paulojmdias/otel-budget-components/processor/budgetprocessor/internal/testutil"

import (
	"encoding/binary"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

// TraceID returns a deterministic trace ID for i.
func TraceID(i uint64) pcommon.TraceID {
	var id pcommon.TraceID
	binary.BigEndian.PutUint64(id[:8], i*2654435761)
	binary.BigEndian.PutUint64(id[8:], i*0x9E3779B97F4A7C15)
	return id
}

// LogRecord describes one fixture record.
type LogRecord struct {
	Severity plog.SeverityNumber
	Body     string
	TraceID  uint64 // 0 means no trace ID
}

// AddLogs appends a resource with service.name and the records to ld.
func AddLogs(ld plog.Logs, service string, recs ...LogRecord) plog.ResourceLogs {
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(string(semconv.ServiceNameKey), service)
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("fixture")
	for i, r := range recs {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetSeverityNumber(r.Severity)
		body := r.Body
		if body == "" {
			body = "log line " + strconv.Itoa(i)
		}
		lr.Body().SetStr(body)
		lr.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000 + int64(i)))
		if r.TraceID != 0 {
			lr.SetTraceID(TraceID(r.TraceID))
		}
	}
	return rl
}

// Repeat returns n copies of r with distinct bodies.
func Repeat(n int, r LogRecord) []LogRecord {
	out := make([]LogRecord, n)
	for i := range out {
		out[i] = r
		out[i].Body = r.Body + " " + strconv.Itoa(i)
	}
	return out
}

// Span describes one fixture span.
type Span struct {
	TraceID uint64
	Error   bool
}

// AddSpans appends a resource with service.name and the spans to td.
func AddSpans(td ptrace.Traces, service string, spans ...Span) ptrace.ResourceSpans {
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr(string(semconv.ServiceNameKey), service)
	ss := rs.ScopeSpans().AppendEmpty()
	for i, s := range spans {
		sp := ss.Spans().AppendEmpty()
		sp.SetName("op")
		sp.SetTraceID(TraceID(s.TraceID))
		var sid pcommon.SpanID
		binary.BigEndian.PutUint64(sid[:], uint64(i+1))
		sp.SetSpanID(sid)
		if s.Error {
			sp.Status().SetCode(ptrace.StatusCodeError)
		}
	}
	return rs
}

// AddGauges appends a resource with one gauge per name, each with points
// data points (distinct attribute values).
func AddGauges(md pmetric.Metrics, service string, points int, names ...string) pmetric.ResourceMetrics {
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr(string(semconv.ServiceNameKey), service)
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("fixture")
	for _, n := range names {
		m := sm.Metrics().AppendEmpty()
		m.SetName(n)
		g := m.SetEmptyGauge()
		for i := range points {
			dp := g.DataPoints().AppendEmpty()
			dp.SetIntValue(int64(i))
			dp.Attributes().PutStr("i", strconv.Itoa(i))
		}
	}
	return rm
}
