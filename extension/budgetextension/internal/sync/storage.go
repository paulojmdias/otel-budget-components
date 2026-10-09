package spendsync // import "github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/sync"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/ledger"
)

// Incrementer is the optional atomic increment capability of a storage
// client. A failed call may have partially applied.
type Incrementer interface {
	BatchIncrementBy(ctx context.Context, deltas map[string]int64) (map[string]int64, error)
}

// StorageConfig configures storage mode.
type StorageConfig struct {
	Extension      component.ID
	Mode           string // auto, increment, gcounter
	NodeID         string
	NodePrefix     string
	MaxNodes       int
	FlushInterval  time.Duration
	ReadInterval   time.Duration
	MaxPendingKeys int
}

// Storage syncs spend through a storage extension.
type Storage struct {
	cfg  StorageConfig
	set  Settings
	mode string

	client storage.Client
	inc    Incrementer

	// p holds pending deltas (increment) or own cumulative spend (gcounter).
	p atomic.Pointer[periods]
	// retired holds increment tables swapped out by the last flush; they
	// are drained once more on the next flush to catch late adds.
	retired []*ledger.Table

	mu          sync.Mutex
	fleetPeriod string
	fleet       map[string]ledger.Spend // increment: fleet totals; gcounter: other nodes
	refreshed   map[string]time.Time    // increment: storage key -> last refresh
	flushedKeys map[string]struct{}     // increment: counter keys in this node key index
	ownTotals   map[string]int64        // increment: this node's applied contribution per counter key
	inflight    map[string]int64        // increment: deltas drained by a flush and not yet applied or dropped
	markerSeen  int64                   // increment: last value of this node's alive marker
	markerOf    string                  // increment: the period markerSeen belongs to
	ownTiers    map[string]int          // this node's tiers, written on flush
	peerTiers   map[string]tierBlob     // other nodes' tiers, from the last read
	asOf        time.Time
	flushMu     sync.Mutex // serializes flush and read rounds

	stop chan struct{}
	wg   sync.WaitGroup
}

var _ SpendSync = (*Storage)(nil)

// NewStorage returns a storage mode sync.
func NewStorage(cfg StorageConfig, set Settings) *Storage {
	set.fill()
	// intervals and sizes come from the validated extension config
	s := &Storage{
		cfg:         cfg,
		set:         set,
		fleet:       map[string]ledger.Spend{},
		refreshed:   map[string]time.Time{},
		flushedKeys: map[string]struct{}{},
		ownTotals:   map[string]int64{},
		stop:        make(chan struct{}),
	}
	s.p.Store(&periods{})
	return s
}

// Mode returns increment or gcounter once started.
func (s *Storage) Mode() string {
	if s.mode == "" {
		return ModeStorage
	}
	return s.mode
}

// Divisor is always 1 in storage mode.
func (*Storage) Divisor() int64 { return 1 }

// SetPeriod switches the current period.
func (s *Storage) SetPeriod(periodID string) {
	// under mu, like a flush swaps tables: a flush in flight must not put
	// the old period back, or every Add for the new one would be dropped
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.p.Load()
	if old.cur != nil && old.cur.ID == periodID {
		return
	}
	s.p.Store(&periods{cur: ledger.NewTable(periodID), prev: old.cur})
	s.fleetPeriod = periodID
	s.fleet = map[string]ledger.Spend{}
	s.refreshed = map[string]time.Time{}
	s.flushedKeys = map[string]struct{}{}
	s.ownTotals = map[string]int64{}
	s.peerTiers = nil
}

// Start gets the storage client, detects the mode, restores own state, and
// starts the flush and read loops.
func (s *Storage) Start(ctx context.Context, host component.Host) error {
	c, err := storageClient(ctx, host, s.cfg.Extension, s.set.Owner)
	if err != nil {
		return err
	}
	inc, ok := c.(Incrementer)
	switch s.cfg.Mode {
	case ModeIncrement:
		if !ok {
			return fmt.Errorf("storage extension %q does not support atomic increments", s.cfg.Extension)
		}
		s.mode, s.inc = ModeIncrement, inc
	case ModeGCounter:
		s.mode = ModeGCounter
	default:
		if ok {
			s.mode, s.inc = ModeIncrement, inc
		} else {
			s.mode = ModeGCounter
		}
		s.set.Logger.Info("budget storage sync mode selected", zap.String("mode", s.mode), zap.String("extension", s.cfg.Extension.String()))
	}
	s.client = c
	if s.mode == ModeGCounter {
		if cur := s.p.Load().cur; cur != nil {
			data, err := c.Get(ctx, s.nodeKey(cur.ID, s.cfg.NodeID))
			if err != nil {
				s.set.OnError("restore")
				return fmt.Errorf("restore own counters: %w", err)
			}
			m, err := decodeBlob(data)
			if err != nil {
				s.set.OnError("restore")
				s.set.Logger.Warn("own counters are corrupt, starting from zero", zap.Error(err))
			} else {
				seed(cur, m)
			}
		}
	}
	_ = s.Read(ctx)
	ft := s.set.Clock.NewTicker(s.cfg.FlushInterval)
	rt := s.set.Clock.NewTicker(s.cfg.ReadInterval)
	s.wg.Add(1)
	lctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	go func() {
		defer cancel()
		defer s.wg.Done()
		defer ft.Stop()
		defer rt.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ft.C():
				_ = s.Flush(lctx)
			case <-rt.C():
				_ = s.Read(lctx)
			}
		}
	}()
	return nil
}

func (*Storage) nodeKey(periodID, node string) string { return periodID + "/node/" + node }

func counterKey(periodID, ledgerID string, sig budgetapi.Signal, class budgetapi.PriceClass) string {
	return periodID + "/" + url.PathEscape(ledgerID) + "/" + sig.String() + "/" + class.String()
}

func parseCounterKey(k string) (periodID, ledgerID string, sig budgetapi.Signal, class budgetapi.PriceClass, ok bool) {
	parts := strings.Split(k, "/")
	if len(parts) != 4 {
		return "", "", 0, 0, false
	}
	id, err := url.PathUnescape(parts[1])
	if err != nil {
		return "", "", 0, 0, false
	}
	si := slices.IndexFunc(signals, func(x budgetapi.Signal) bool { return x.String() == parts[2] })
	ci := slices.IndexFunc(classes, func(x budgetapi.PriceClass) bool { return x.String() == parts[3] })
	if si < 0 || ci < 0 {
		return "", "", 0, 0, false
	}
	return parts[0], id, signals[si], classes[ci], true
}

// Add adds spend: a pending delta in increment mode, own counters in
// G-Counter mode.
func (s *Storage) Add(periodID, ledgerID string, sig budgetapi.Signal, class budgetapi.PriceClass, micro int64) {
	t := s.p.Load().table(periodID)
	if t == nil {
		return
	}
	cell, ok := t.Existing(ledgerID)
	if !ok {
		if s.mode != ModeGCounter && t.Len() >= int64(s.cfg.MaxPendingKeys) {
			s.set.OnOverflow()
			return
		}
		cell = t.Cell(ledgerID)
	}
	cell.Counter(sig, class).AddMicro(micro)
}

// Totals returns the fleet view plus own unflushed or own live spend. It
// reads under mu, like a flush swaps and drains, so own spend is never
// missing mid flush: fleet totals only go back when storage loses data.
func (s *Storage) Totals() (map[string]ledger.Spend, time.Time, error) {
	s.mu.Lock()
	p := s.p.Load()
	out := make(map[string]ledger.Spend, len(s.fleet))
	maps.Copy(out, s.fleet)
	for k, d := range s.inflight {
		if periodID, id, sig, cl, ok := parseCounterKey(k); ok && periodID == s.fleetPeriod {
			v := out[id]
			v[sig][cl] += d
			out[id] = v
		}
	}
	if p.cur != nil {
		p.cur.Range(func(id string, c *ledger.Cell) {
			v := out[id]
			snap := c.Snapshot()
			v.Add(&snap)
			out[id] = v
		})
	}
	asOf := s.asOf
	s.mu.Unlock()
	if asOf.IsZero() {
		return out, asOf, errNotStarted
	}
	return out, asOf, nil
}

// Flush pushes deltas (increment) or the own blob (gcounter).
func (s *Storage) Flush(ctx context.Context) error {
	if s.client == nil {
		return errNotStarted
	}
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	var err error
	if s.mode == ModeIncrement {
		err = s.flushIncrement(ctx)
	} else {
		err = s.flushGCounter(ctx)
	}
	if cur := s.p.Load().cur; err == nil && cur != nil {
		err = s.writeTiers(ctx, cur.ID)
	}
	return err
}

func drain(t *ledger.Table, into map[string]int64) {
	t.Range(func(id string, c *ledger.Cell) {
		for _, sig := range signals {
			for _, cl := range classes {
				if v := c.Counter(sig, cl).Swap(0); v != 0 {
					into[counterKey(t.ID, id, sig, cl)] += v
				}
			}
		}
	})
}

func (s *Storage) flushIncrement(ctx context.Context) error {
	s.mu.Lock()
	old := s.p.Load()
	next := &periods{}
	if old.cur != nil {
		next.cur = ledger.NewTable(old.cur.ID)
	}
	s.p.Store(next)
	deltas := map[string]int64{}
	for _, t := range s.retired {
		drain(t, deltas)
	}
	s.retired = s.retired[:0]
	for _, t := range []*ledger.Table{old.prev, old.cur} {
		if t != nil {
			drain(t, deltas)
			s.retired = append(s.retired, t)
		}
	}
	s.inflight = maps.Clone(deltas)
	s.mu.Unlock()
	if len(deltas) == 0 {
		return nil
	}
	// The alive marker counts this node's flushes in the same pipeline. If it
	// comes back lower than expected, the storage lost its data: this node
	// then adds its own earlier contribution back, once. Every node restores
	// only its own share, so there is no race between nodes.
	var marker string
	if old.cur != nil {
		marker = s.markerKey(old.cur.ID)
		deltas[marker] = 1
	}
	res, err := s.inc.BatchIncrementBy(ctx, deltas)
	if err != nil {
		delete(deltas, marker)
		s.mu.Lock()
		if notSent(err) {
			// nothing reached storage: the deltas go back to pending, so
			// an outage delays spend instead of losing it
			s.requeue(deltas)
			s.set.Logger.Warn("budget increment flush failed before sending, keeping deltas", zap.Int("keys", len(deltas)), zap.Error(err))
		} else {
			// may have partially applied: drop the in flight deltas, never retry
			s.set.Logger.Warn("budget increment flush failed, dropping in flight deltas", zap.Int("keys", len(deltas)), zap.Error(err))
		}
		s.inflight = nil
		s.mu.Unlock()
		s.set.OnError("flush")
		return err
	}
	lost := false
	if marker != "" {
		v := res[marker]
		delete(res, marker)
		delete(deltas, marker)
		// a flush of the previous period can land after a rollover: only
		// compare markers of the same period
		lost = s.markerOf == old.cur.ID && v <= s.markerSeen
		s.markerSeen, s.markerOf = v, old.cur.ID
	}
	if lost {
		if err := s.restoreOwn(ctx, old.cur.ID, deltas, res); err != nil {
			s.mu.Lock()
			s.inflight = nil
			s.mu.Unlock()
			return err
		}
	}
	now := s.set.Clock.Now()
	s.mu.Lock()
	for k, v := range res {
		if s.applyCounter(k, v) {
			s.refreshed[k] = now
		}
	}
	for k, d := range deltas {
		s.ownTotals[k] += d
	}
	s.inflight = nil
	s.asOf = now
	var idx []byte
	if old.cur != nil {
		for k := range deltas {
			if periodID, _, _, _, ok := parseCounterKey(k); ok && periodID == old.cur.ID {
				if _, seen := s.flushedKeys[k]; !seen {
					s.flushedKeys[k] = struct{}{}
					idx = []byte{}
				}
			}
		}
		if idx != nil {
			keys := make([]string, 0, len(s.flushedKeys))
			for k := range s.flushedKeys {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			j, _ := json.Marshal(keys)
			idx = encoder.EncodeAll(j, nil) // the index is read by every replica: compress it like blobs
		}
	}
	s.mu.Unlock()
	if idx != nil {
		if err := s.client.Set(ctx, s.keysIndexKey(old.cur.ID, s.cfg.NodeID), idx); err != nil {
			s.set.OnError("flush_index")
			s.mu.Lock()
			s.flushedKeys = map[string]struct{}{}
			s.mu.Unlock()
			return err
		}
	}
	return nil
}

// notSent reports an error raised before the request left this process
// (dialing failed), so a retry cannot count anything twice.
func notSent(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

// requeue returns deltas that never reached storage to pending; callers
// hold mu.
func (s *Storage) requeue(deltas map[string]int64) {
	tables := append([]*ledger.Table{s.p.Load().cur}, s.retired...)
	for k, d := range deltas {
		periodID, id, sig, cl, ok := parseCounterKey(k)
		for _, t := range tables {
			if ok && t != nil && t.ID == periodID {
				t.Cell(id).Counter(sig, cl).AddMicro(d)
				break
			}
		}
	}
}

// applyCounter stores a fleet counter value and reports whether it belongs
// to the current period; callers hold mu.
func (s *Storage) applyCounter(k string, v int64) bool {
	periodID, id, sig, cl, ok := parseCounterKey(k)
	if !ok || periodID != s.fleetPeriod {
		return false
	}
	sp := s.fleet[id]
	sp[sig][cl] = v
	s.fleet[id] = sp
	return true
}

func (s *Storage) flushGCounter(ctx context.Context) error {
	p := s.p.Load()
	ops := make([]*storage.Operation, 0, 2)
	for _, t := range []*ledger.Table{p.prev, p.cur} {
		if t == nil {
			continue
		}
		data, err := encodeBlob(t.Snapshot())
		if err != nil {
			return err
		}
		ops = append(ops, storage.SetOperation(s.nodeKey(t.ID, s.cfg.NodeID), data))
	}
	if len(ops) == 0 {
		return nil
	}
	if err := s.client.Batch(ctx, ops...); err != nil {
		s.set.OnError("flush")
		s.set.Logger.Warn("budget gcounter flush failed", zap.Error(err))
		return err
	}
	if p.prev != nil {
		s.p.CompareAndSwap(p, &periods{cur: p.cur})
	}
	return nil
}

// Read refreshes the fleet view.
func (s *Storage) Read(ctx context.Context) error {
	if s.client == nil {
		return errNotStarted
	}
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	now := s.set.Clock.Now()
	var err error
	if s.mode == ModeIncrement {
		err = s.readIncrement(ctx, now)
	} else {
		err = s.readGCounter(ctx, now)
	}
	if err == nil {
		s.mu.Lock()
		period := s.fleetPeriod
		s.mu.Unlock()
		err = s.readTiers(ctx, period)
	}
	if err == nil {
		s.mu.Lock()
		s.asOf = now
		s.mu.Unlock()
	}
	return err
}

func (s *Storage) markerKey(periodID string) string { return periodID + "/alive/" + s.cfg.NodeID }

// restoreOwn adds this node's contribution from before a storage data loss
// back, updates res with the restored values, and forces the key index to be
// rewritten. Called by flushIncrement, which holds flushMu.
func (s *Storage) restoreOwn(ctx context.Context, periodID string, deltas, res map[string]int64) error {
	s.mu.Lock()
	restore := make(map[string]int64, len(s.ownTotals))
	for k, v := range s.ownTotals {
		if p, _, _, _, ok := parseCounterKey(k); ok && p == periodID && v != 0 {
			restore[k] = v
		}
	}
	s.flushedKeys = map[string]struct{}{}
	s.mu.Unlock()
	s.set.Logger.Warn("budget storage lost its counters, restoring this node's contribution", zap.Int("keys", len(restore)))
	s.set.OnError("storage_reset")
	if len(restore) == 0 {
		return nil
	}
	got, err := s.inc.BatchIncrementBy(ctx, restore)
	if err != nil {
		s.set.OnError("restore")
		return err
	}
	for k, v := range got {
		res[k] = v
		if _, ok := deltas[k]; !ok {
			deltas[k] = 0 // so the key index lists it again
		}
	}
	return nil
}

func (*Storage) keysIndexKey(periodID, node string) string { return periodID + "/keys/" + node }

// fleetLedgerIDs returns the ledger IDs flushed by any node this period, so
// a replica also learns keys it has never seen itself.
// fleetCounterKeys returns the counter keys any node has written this
// period, so a replica also reads keys it has never seen itself, and only
// counters that exist.
func (s *Storage) fleetCounterKeys(ctx context.Context, period string) (map[string]struct{}, error) {
	nodes := s.nodes()
	ops := make([]*storage.Operation, len(nodes))
	for i, n := range nodes {
		ops[i] = storage.GetOperation(s.keysIndexKey(period, n))
	}
	if err := s.client.Batch(ctx, ops...); err != nil {
		return nil, err
	}
	keys := map[string]struct{}{}
	for _, op := range ops {
		if op.Value == nil {
			continue
		}
		j, err := decoder.DecodeAll(op.Value, nil)
		if err != nil {
			continue
		}
		var l []string
		if json.Unmarshal(j, &l) == nil {
			for _, k := range l {
				keys[k] = struct{}{}
			}
		}
	}
	return keys, nil
}

func (s *Storage) readIncrement(ctx context.Context, now time.Time) error {
	s.mu.Lock()
	period := s.fleetPeriod
	s.mu.Unlock()
	keys, err := s.fleetCounterKeys(ctx, period)
	if err != nil {
		s.set.OnError("read")
		s.set.Logger.Warn("budget increment key index read failed", zap.Error(err))
		return err
	}
	// skip counters this replica's own flush refreshed recently
	zero := make(map[string]int64, len(keys))
	s.mu.Lock()
	for k := range keys {
		if t, ok := s.refreshed[k]; ok && now.Sub(t) < s.cfg.ReadInterval {
			continue
		}
		zero[k] = 0
	}
	s.mu.Unlock()
	if len(zero) == 0 {
		return nil
	}
	// adding zero reads every counter in one pipelined round trip; the keys
	// come from the index, so they already exist
	res, err := s.inc.BatchIncrementBy(ctx, zero)
	if err != nil {
		s.set.OnError("read")
		s.set.Logger.Warn("budget increment read failed", zap.Error(err))
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range res {
		// reads do not mark keys refreshed: only a flush response does, so
		// keys this replica never flushes are read on every read interval
		s.applyCounter(k, v)
	}
	return nil
}

func (s *Storage) nodes() []string {
	out := make([]string, 0, s.cfg.MaxNodes+1)
	for i := range s.cfg.MaxNodes {
		out = append(out, s.cfg.NodePrefix+"-"+strconv.Itoa(i))
	}
	if !slices.Contains(out, s.cfg.NodeID) {
		out = append(out, s.cfg.NodeID)
	}
	return out
}

func (s *Storage) readGCounter(ctx context.Context, _ time.Time) error {
	s.mu.Lock()
	period := s.fleetPeriod
	s.mu.Unlock()
	if period == "" {
		return nil
	}
	var ops []*storage.Operation
	for _, n := range s.nodes() {
		if n != s.cfg.NodeID {
			ops = append(ops, storage.GetOperation(s.nodeKey(period, n)))
		}
	}
	if err := s.client.Batch(ctx, ops...); err != nil {
		s.set.OnError("read")
		s.set.Logger.Warn("budget gcounter read failed", zap.Error(err))
		return err
	}
	others := map[string]ledger.Spend{}
	for _, op := range ops {
		if op.Value == nil {
			continue
		}
		m, err := decodeBlob(op.Value)
		if err != nil {
			s.set.OnError("read")
			s.set.Logger.Warn("skipping corrupt node blob", zap.String("key", op.Key), zap.Error(err))
			continue
		}
		for id, sp := range m {
			v := others[id]
			v.Add(&sp)
			others[id] = v
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fleetPeriod == period {
		s.fleet = others
	}
	return nil
}

// Shutdown flushes once more and closes the client.
func (s *Storage) Shutdown(ctx context.Context) error {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	s.wg.Wait()
	if s.client == nil {
		return nil
	}
	_ = s.Flush(ctx)
	err := s.client.Close(ctx)
	s.client = nil
	return err
}
