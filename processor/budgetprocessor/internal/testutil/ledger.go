package testutil // import "github.com/paulojmdias/otel-budget-processor/processor/budgetprocessor/internal/testutil"

import (
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
)

// RecordCall is one Record or RecordOffered call.
type RecordCall struct {
	Key   string
	Sig   budgetapi.Signal
	Class budgetapi.PriceClass
	Usage budgetapi.Usage
}

// DropCall is one RecordDropped call.
type DropCall struct {
	Key    string
	Sig    budgetapi.Signal
	Reason budgetapi.DropReason
	N      int64
}

// FakeLedger is a scriptable budgetapi.Ledger keyed by service.name.
type FakeLedger struct {
	component.StartFunc
	component.ShutdownFunc

	mu        sync.Mutex
	Decisions map[string]budgetapi.Decision
	Unhealthy bool
	Policy    budgetapi.AlwaysPass
	// SeriesPriced is what PricesSeries reports.
	SeriesPriced bool
	Records      []RecordCall
	Offered      []RecordCall
	Drops        []DropCall
}

var (
	_ budgetapi.Ledger         = (*FakeLedger)(nil)
	_ budgetapi.DropReporter   = (*FakeLedger)(nil)
	_ budgetapi.PolicyProvider = (*FakeLedger)(nil)
	_ budgetapi.SeriesPricer   = (*FakeLedger)(nil)
)

// NewFakeLedger returns a fake with the default always pass policy.
func NewFakeLedger() *FakeLedger {
	return &FakeLedger{
		Decisions: map[string]budgetapi.Decision{},
		Policy:    budgetapi.AlwaysPass{LogSeverity: 17, SpanStatusError: true},
	}
}

// ResolveKey returns service.name.
func (*FakeLedger) ResolveKey(res pcommon.Resource) string {
	if v, ok := res.Attributes().Get(string(semconv.ServiceNameKey)); ok {
		return v.Str()
	}
	return "default"
}

// Decision returns the scripted decision, tier 0 by default.
func (f *FakeLedger) Decision(key string) budgetapi.Decision {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d, ok := f.Decisions[key]; ok {
		return d
	}
	return budgetapi.Decision{Actions: budgetapi.Action{SampleRatio: 1}}
}

// Record records the call.
func (f *FakeLedger) Record(key string, sig budgetapi.Signal, class budgetapi.PriceClass, u budgetapi.Usage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Records = append(f.Records, RecordCall{key, sig, class, u})
}

// RecordOffered records the call.
func (f *FakeLedger) RecordOffered(key string, sig budgetapi.Signal, u budgetapi.Usage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Offered = append(f.Offered, RecordCall{Key: key, Sig: sig, Usage: u})
}

// RecordDropped records the call.
func (f *FakeLedger) RecordDropped(key string, sig budgetapi.Signal, reason budgetapi.DropReason, n int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Drops = append(f.Drops, DropCall{key, sig, reason, n})
}

// Healthy reports !Unhealthy.
func (f *FakeLedger) Healthy() bool { return !f.Unhealthy }

// PricesSeries returns SeriesPriced.
func (f *FakeLedger) PricesSeries() bool { return f.SeriesPriced }

// AlwaysPass returns Policy.
func (f *FakeLedger) AlwaysPass() budgetapi.AlwaysPass { return f.Policy }

// Reset clears recorded calls.
func (f *FakeLedger) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Records, f.Offered, f.Drops = nil, nil, nil
}
