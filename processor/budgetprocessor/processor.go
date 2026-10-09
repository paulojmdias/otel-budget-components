package budgetprocessor // import "github.com/paulojmdias/otel-budget-components/processor/budgetprocessor"

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/budgetapi"
)

// Resource attributes stamped by the processor.
const (
	AttrKey    = "otelcol.budget.key"
	AttrTier   = "otelcol.budget.tier"
	AttrDivert = "otelcol.budget.divert"
)

const sampleMask = 1<<56 - 1

type budgetProcessor struct {
	cfg    *Config
	logger *zap.Logger
	class  budgetapi.PriceClass

	ledger  budgetapi.Ledger
	dropper budgetapi.DropReporter
	policy  budgetapi.AlwaysPass
	// series reports whether metrics carry series hashes; a ledger that
	// does not say gets them
	series bool
}

func newProcessor(cfg *Config, logger *zap.Logger) *budgetProcessor {
	class, _ := budgetapi.ParsePriceClass(cfg.PriceClass)
	return &budgetProcessor{
		cfg: cfg, logger: logger, class: class,
		policy: budgetapi.AlwaysPass{LogSeverity: plog.SeverityNumberError, SpanStatusError: true},
	}
}

func (p *budgetProcessor) start(_ context.Context, host component.Host) error {
	ext, ok := host.GetExtensions()[p.cfg.Ledger]
	if !ok {
		if p.cfg.FailOpen {
			p.logger.Warn("budget ledger extension not found, failing open (tier 0, no accounting)", zap.String("ledger", p.cfg.Ledger.String()))
			return nil
		}
		return fmt.Errorf("budget ledger extension %q not found", p.cfg.Ledger)
	}
	l, ok := ext.(budgetapi.Ledger)
	if !ok {
		return fmt.Errorf("extension %q is not a budget ledger", p.cfg.Ledger)
	}
	p.ledger = l
	if d, ok := ext.(budgetapi.DropReporter); ok {
		p.dropper = d
	}
	if pp, ok := ext.(budgetapi.PolicyProvider); ok {
		p.policy = pp.AlwaysPass()
	}
	p.series = true
	if sp, ok := ext.(budgetapi.SeriesPricer); ok {
		p.series = sp.PricesSeries()
	}
	return nil
}

// decide returns the effective decision and whether actions apply.
func (p *budgetProcessor) decide(token string) (budgetapi.Decision, bool) {
	if p.ledger == nil {
		return budgetapi.Decision{Actions: budgetapi.Action{SampleRatio: 1}}, false
	}
	d := p.ledger.Decision(token)
	if p.cfg.FailOpen && !p.ledger.Healthy() {
		return budgetapi.Decision{Actions: budgetapi.Action{SampleRatio: 1}}, false
	}
	return d, p.cfg.Enforce && !d.Exempt && d.Tier > 0
}

func stamp(res pcommon.Resource, token string, d budgetapi.Decision, divert bool) {
	attrs := res.Attributes()
	attrs.PutStr(AttrKey, budgetapi.KeyName(token))
	attrs.PutInt(AttrTier, int64(d.Tier))
	if divert {
		attrs.PutBool(AttrDivert, true)
	}
}

// keyUsage accumulates usage for one key within a batch.
type keyUsage struct {
	token   string
	offered budgetapi.Usage
	kept    budgetapi.Usage
	dropped [budgetapi.NumDropReasons]int64
}

// batchUsage keeps one entry per key touched in the batch. Batches usually
// carry few keys, so a linear scan beats a map.
type batchUsage struct{ keys []keyUsage }

func (b *batchUsage) get(token string) *keyUsage {
	for i := range b.keys {
		if b.keys[i].token == token {
			return &b.keys[i]
		}
	}
	b.keys = append(b.keys, keyUsage{token: token})
	return &b.keys[len(b.keys)-1]
}

// flush reports exactly one Record and one RecordOffered per key.
func (p *budgetProcessor) flush(sig budgetapi.Signal, b *batchUsage) {
	if p.ledger == nil {
		return
	}
	for i := range b.keys {
		k := &b.keys[i]
		// the hot pipeline already counted offered volume for diverted data
		if p.class == budgetapi.PriceHot {
			p.ledger.RecordOffered(k.token, sig, k.offered)
		}
		if k.kept.Items > 0 || k.kept.Bytes > 0 {
			p.ledger.Record(k.token, sig, p.class, k.kept)
		}
		if p.dropper != nil {
			for r, n := range k.dropped {
				if n > 0 {
					p.dropper.RecordDropped(k.token, sig, budgetapi.DropReason(r), n)
				}
			}
		}
	}
}

// sampleThreshold converts a ratio to the 56 bit threshold.
func sampleThreshold(ratio float64) uint64 {
	if ratio >= 1 {
		return 1 << 56
	}
	if ratio <= 0 {
		return 0
	}
	return uint64(ratio * (1 << 56))
}

// keepTrace is the consistent sampling decision: the same trace ID gives the
// same answer on every replica and signal.
func keepTrace(id pcommon.TraceID, threshold uint64) bool {
	return binary.BigEndian.Uint64(id[8:16])&sampleMask < threshold
}

func keepLog(lr plog.LogRecord, threshold uint64) bool {
	if id := lr.TraceID(); !id.IsEmpty() {
		return keepTrace(id, threshold)
	}
	var d xxhash.Digest
	d.Reset()
	body := lr.Body()
	if body.Type() == pcommon.ValueTypeStr {
		_, _ = d.WriteString(body.Str())
	} else {
		_, _ = d.WriteString(body.AsString())
	}
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(lr.Timestamp()))
	_, _ = d.Write(ts[:])
	return d.Sum64()&sampleMask < threshold
}
