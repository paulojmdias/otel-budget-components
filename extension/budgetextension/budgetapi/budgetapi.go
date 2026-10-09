// Package budgetapi is the contract between the budget extension, which owns
// all state, and the budget processor, which applies decisions in pipelines.
package budgetapi // import "github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"

import (
	"fmt"
	"strings"
	"time"

	"github.com/gobwas/glob"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// Signal identifies a telemetry signal.
type Signal uint8

const (
	SignalLogs Signal = iota
	SignalTraces
	SignalMetrics
)

// NumSignals is the number of signals, for array sizing.
const NumSignals = 3

func (s Signal) String() string {
	switch s {
	case SignalLogs:
		return "logs"
	case SignalTraces:
		return "traces"
	case SignalMetrics:
		return "metrics"
	}
	return "unknown"
}

// PriceClass selects which price list usage is accounted at.
type PriceClass uint8

const (
	PriceHot PriceClass = iota
	PriceCold
)

// NumPriceClasses is the number of price classes, for array sizing.
const NumPriceClasses = 2

func (c PriceClass) String() string {
	if c == PriceCold {
		return "cold"
	}
	return "hot"
}

// ParsePriceClass parses "hot" or "cold".
func ParsePriceClass(s string) (PriceClass, error) {
	switch s {
	case "hot":
		return PriceHot, nil
	case "cold":
		return PriceCold, nil
	}
	return PriceHot, fmt.Errorf("invalid price_class %q, want hot or cold", s)
}

// Usage is what a processor measured for one key in one batch.
type Usage struct {
	Bytes      int64    // proto size of the data
	Items      int64    // log records, spans, or data points
	SeriesHash []uint64 // metrics only: series identities in this batch
}

// Action is what a tier asks the processor to do.
type Action struct {
	DropBelowSeverity plog.SeverityNumber // logs; 0 disables
	SampleRatio       float64             // logs and traces; 1.0 keeps all
	DropMetrics       []string            // metric name globs
	Divert            bool

	metricGlobs []glob.Glob
}

// Compile compiles DropMetrics into matchers. The extension calls it once per
// decision publish so the processor never compiles globs on the hot path.
func (a *Action) Compile() error {
	a.metricGlobs = a.metricGlobs[:0]
	for _, p := range a.DropMetrics {
		g, err := glob.Compile(p)
		if err != nil {
			return fmt.Errorf("invalid metric glob %q: %w", p, err)
		}
		a.metricGlobs = append(a.metricGlobs, g)
	}
	return nil
}

// DropsMetric reports whether a metric with this name must be dropped.
// Only valid after Compile.
func (a *Action) DropsMetric(name string) bool {
	for _, g := range a.metricGlobs {
		if g.Match(name) {
			return true
		}
	}
	return false
}

// Samples reports whether SampleRatio removes anything.
func (a Action) Samples() bool { return a.SampleRatio > 0 && a.SampleRatio < 1 }

// Decision is the precomputed enforcement state for a key.
type Decision struct {
	Tier      int
	Actions   Action
	Exempt    bool // key or one of its groups is listed in exempt
	Stale     bool // sync data older than stale_after
	UpdatedAt time.Time
}

// Ledger is implemented by the budget extension.
type Ledger interface {
	ResolveKey(res pcommon.Resource) string
	Decision(key string) Decision
	Record(key string, sig Signal, class PriceClass, u Usage)
	RecordOffered(key string, sig Signal, u Usage)
	Healthy() bool
}

// DropReason labels dropped records in self telemetry.
type DropReason uint8

const (
	DropSeverity DropReason = iota
	DropSample
	DropMetricGlob
)

// NumDropReasons is the number of drop reasons, for array sizing.
const NumDropReasons = 3

func (r DropReason) String() string {
	switch r {
	case DropSeverity:
		return "severity"
	case DropSample:
		return "sample"
	}
	return "metric_glob"
}

// DropReporter is optionally implemented by a Ledger so the processor can
// report dropped records without owning telemetry state.
type DropReporter interface {
	RecordDropped(key string, sig Signal, reason DropReason, n int64)
}

// AlwaysPass is the "never drop what matters" policy.
type AlwaysPass struct {
	LogSeverity     plog.SeverityNumber
	SpanStatusError bool
}

// PolicyProvider is optionally implemented by a Ledger to share the
// always pass policy with processors.
type PolicyProvider interface {
	AlwaysPass() AlwaysPass
}

// SeriesPricer is optionally implemented by a Ledger to say whether active
// series are priced. When they are not, processors skip hashing every data
// point for Usage.SeriesHash.
type SeriesPricer interface {
	PricesSeries() bool
}

// KeySeparator separates the plain key from the encoded rule membership in a
// key returned by Ledger.ResolveKey.
const KeySeparator = "\x1f"

// KeyName returns the plain budget key (as stamped in otelcol.budget.key) from a
// resolved key.
func KeyName(resolved string) string {
	if before, _, ok := strings.Cut(resolved, KeySeparator); ok {
		return before
	}
	return resolved
}

var severities = []struct {
	name string
	num  plog.SeverityNumber
}{
	{"TRACE", plog.SeverityNumberTrace},
	{"DEBUG", plog.SeverityNumberDebug},
	{"INFO", plog.SeverityNumberInfo},
	{"WARN", plog.SeverityNumberWarn},
	{"ERROR", plog.SeverityNumberError},
	{"FATAL", plog.SeverityNumberFatal},
}

// ParseSeverity parses TRACE, DEBUG, INFO, WARN, ERROR, or FATAL (case
// insensitive) into the lowest severity number of that range.
func ParseSeverity(s string) (plog.SeverityNumber, error) {
	u := strings.ToUpper(strings.TrimSpace(s))
	if u == "WARNING" {
		u = "WARN"
	}
	for _, sv := range severities {
		if sv.name == u {
			return sv.num, nil
		}
	}
	return 0, fmt.Errorf("invalid severity %q, want one of TRACE, DEBUG, INFO, WARN, ERROR, FATAL", s)
}

// MostRestrictive picks the decision with the higher tier. Tiers come from
// one shared list, so equal tiers carry identical actions. Exempt, Stale, and
// UpdatedAt are taken from a.
func MostRestrictive(a, b Decision) Decision {
	if b.Tier > a.Tier {
		b.Exempt, b.Stale, b.UpdatedAt = a.Exempt, a.Stale, a.UpdatedAt
		return b
	}
	return a
}
