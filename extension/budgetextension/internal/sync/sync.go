// Package spendsync shares spend across Collector replicas. Modes: local,
// share, and storage (atomic increment or G-Counter over storage.Client).
package spendsync // import "github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/sync"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/clock"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/ledger"
)

// Modes.
const (
	ModeLocal     = "local"
	ModeShare     = "share"
	ModeStorage   = "storage"
	ModeIncrement = "increment"
	ModeGCounter  = "gcounter"
	ModeAuto      = "auto"
)

// StorageName is the storage client name requested from storage extensions.
const StorageName = "budget"

// SpendSync is the fleet view of spend.
type SpendSync interface {
	Start(ctx context.Context, host component.Host) error
	Add(periodID, ledgerID string, sig budgetapi.Signal, class budgetapi.PriceClass, micro int64)
	// Totals returns fleet totals of the current period per ledger ID and
	// when they were observed.
	Totals() (totals map[string]ledger.Spend, asOf time.Time, err error)
	Shutdown(ctx context.Context) error

	// SetPeriod switches the current period. The previous period is kept
	// until its last flush completes.
	SetPeriod(periodID string)
	// Mode names the effective mode for telemetry and the API.
	Mode() string
	// Divisor is N in share mode, 1 otherwise.
	Divisor() int64
}

// Settings are shared by all modes.
type Settings struct {
	Clock  clock.Clock
	Logger *zap.Logger
	// Owner is the ID of the component requesting storage clients.
	Owner component.ID
	// OnError is called for every sync error with the failed operation.
	OnError func(op string)
	// OnOverflow is called when a pending delta is dropped because
	// max_pending_keys was reached.
	OnOverflow func()
}

func (s *Settings) fill() {
	if s.Clock == nil {
		s.Clock = clock.Real()
	}
	if s.Logger == nil {
		s.Logger = zap.NewNop()
	}
	if s.OnError == nil {
		s.OnError = func(string) {}
	}
	if s.OnOverflow == nil {
		s.OnOverflow = func() {}
	}
}

func storageClient(ctx context.Context, host component.Host, ext, owner component.ID) (storage.Client, error) {
	e, ok := host.GetExtensions()[ext]
	if !ok {
		return nil, fmt.Errorf("storage extension %q not found", ext)
	}
	se, ok := e.(storage.Extension)
	if !ok {
		return nil, fmt.Errorf("extension %q is not a storage extension", ext)
	}
	return se.GetClient(ctx, component.KindExtension, owner, StorageName)
}

// periods holds the current and previous period tables.
type periods struct {
	cur  *ledger.Table
	prev *ledger.Table
}

func (p *periods) table(periodID string) *ledger.Table {
	if p.cur != nil && p.cur.ID == periodID {
		return p.cur
	}
	if p.prev != nil && p.prev.ID == periodID {
		return p.prev
	}
	return nil
}

var (
	signals = []budgetapi.Signal{budgetapi.SignalLogs, budgetapi.SignalTraces, budgetapi.SignalMetrics}
	classes = []budgetapi.PriceClass{budgetapi.PriceHot, budgetapi.PriceCold}
)

// Blobs are JSON (ledger ID -> [signal][class] micro units) compressed with
// zstd.
var (
	encoder, _ = zstd.NewWriter(nil)
	decoder, _ = zstd.NewReader(nil)
)

func encodeBlob(m map[string]ledger.Spend) ([]byte, error) {
	j, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return encoder.EncodeAll(j, nil), nil
}

func decodeBlob(data []byte) (map[string]ledger.Spend, error) {
	if len(data) == 0 {
		return map[string]ledger.Spend{}, nil
	}
	j, err := decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd: %w", err)
	}
	var m map[string]ledger.Spend
	if err := json.Unmarshal(j, &m); err != nil {
		return nil, fmt.Errorf("json: %w", err)
	}
	return m, nil
}

func seed(t *ledger.Table, m map[string]ledger.Spend) {
	for id, s := range m {
		c := t.Cell(id)
		for _, sig := range signals {
			for _, cl := range classes {
				c.Counter(sig, cl).AddMicro(s[sig][cl])
			}
		}
	}
}

var errNotStarted = errors.New("sync not started")
