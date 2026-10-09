package spendsync // import "github.com/paulojmdias/otel-budget-components/extension/budgetextension/internal/sync"

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"

	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/budgetapi"
	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/internal/ledger"
)

// LocalConfig configures local and share modes.
type LocalConfig struct {
	// Storage optionally names a storage extension for checkpoints.
	Storage            *component.ID
	CheckpointInterval time.Duration
	// Divisor is N for share mode; 0 or 1 means local mode.
	Divisor int64
}

// Local keeps in memory counters as the fleet view, with optional
// checkpoints. With Divisor > 1 it is share mode.
type Local struct {
	cfg    LocalConfig
	set    Settings
	p      atomic.Pointer[periods]
	client storage.Client
	stop   chan struct{}
	wg     sync.WaitGroup
}

var _ SpendSync = (*Local)(nil)

// NewLocal returns a local or share mode sync.
func NewLocal(cfg LocalConfig, set Settings) *Local {
	set.fill()
	if cfg.Divisor < 1 {
		cfg.Divisor = 1
	}
	l := &Local{cfg: cfg, set: set, stop: make(chan struct{})}
	l.p.Store(&periods{})
	return l
}

// Mode returns local or share.
func (l *Local) Mode() string {
	if l.cfg.Divisor > 1 {
		return ModeShare
	}
	return ModeLocal
}

// Divisor returns N.
func (l *Local) Divisor() int64 { return l.cfg.Divisor }

// SetPeriod switches the current period.
func (l *Local) SetPeriod(periodID string) {
	old := l.p.Load()
	if old.cur != nil && old.cur.ID == periodID {
		return
	}
	// the previous period is not flushed anywhere, so it can go now
	l.p.Store(&periods{cur: ledger.NewTable(periodID)})
}

// Start restores the last checkpoint and starts checkpointing.
func (l *Local) Start(ctx context.Context, host component.Host) error {
	if l.cfg.Storage == nil {
		return nil
	}
	c, err := storageClient(ctx, host, *l.cfg.Storage, l.set.Owner)
	if err != nil {
		return err
	}
	l.client = c
	if cur := l.p.Load().cur; cur != nil {
		data, err := c.Get(ctx, l.checkpointKey(cur.ID))
		if err != nil {
			l.set.OnError("restore")
			l.set.Logger.Warn("checkpoint restore failed, starting from zero", zap.Error(err))
		} else if m, err := decodeBlob(data); err != nil {
			l.set.OnError("restore")
			l.set.Logger.Warn("checkpoint is corrupt, starting from zero", zap.Error(err))
		} else {
			seed(cur, m)
		}
	}
	interval := l.cfg.CheckpointInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	tk := l.set.Clock.NewTicker(interval)
	l.wg.Add(1)
	lctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	go func() {
		defer cancel()
		defer l.wg.Done()
		defer tk.Stop()
		for {
			select {
			case <-l.stop:
				return
			case <-tk.C():
				_ = l.Checkpoint(lctx)
			}
		}
	}()
	return nil
}

func (*Local) checkpointKey(periodID string) string { return "local/" + periodID }

// Checkpoint writes the current period counters.
func (l *Local) Checkpoint(ctx context.Context) error {
	cur := l.p.Load().cur
	if l.client == nil || cur == nil {
		return errNotStarted
	}
	data, err := encodeBlob(cur.Snapshot())
	if err == nil {
		err = l.client.Set(ctx, l.checkpointKey(cur.ID), data)
	}
	if err != nil {
		l.set.OnError("checkpoint")
		l.set.Logger.Warn("checkpoint failed", zap.Error(err))
	}
	return err
}

// Add adds spend.
func (l *Local) Add(periodID, ledgerID string, sig budgetapi.Signal, class budgetapi.PriceClass, micro int64) {
	if t := l.p.Load().table(periodID); t != nil {
		t.Cell(ledgerID).Counter(sig, class).AddMicro(micro)
	}
}

// Totals returns the local counters, observed now.
func (l *Local) Totals() (map[string]ledger.Spend, time.Time, error) {
	cur := l.p.Load().cur
	if cur == nil {
		return map[string]ledger.Spend{}, l.set.Clock.Now(), nil
	}
	return cur.Snapshot(), l.set.Clock.Now(), nil
}

// Shutdown writes a final checkpoint.
func (l *Local) Shutdown(ctx context.Context) error {
	select {
	case <-l.stop:
	default:
		close(l.stop)
	}
	l.wg.Wait()
	if l.client == nil {
		return nil
	}
	_ = l.Checkpoint(ctx)
	err := l.client.Close(ctx)
	l.client = nil
	return err
}
