package spendsync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/redisstorageextension"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"

	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/budgetapi"
	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/internal/clock"
	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/internal/ledger"
	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/internal/testutil"
)

var (
	t0      = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	extID   = component.MustNewIDWithName("redis_storage", "budget")
	ownerID = component.MustNewID("budget")
	logs    = budgetapi.SignalLogs
	hot     = budgetapi.PriceHot
)

func total(t *testing.T, s SpendSync, id string) int64 {
	t.Helper()
	m, _, _ := s.Totals()
	v := m[id]
	return v.Total()
}

// withIntervals fills the sizes and intervals the extension config always
// sets, keeping the ones a test sets explicitly.
func withIntervals(c StorageConfig) StorageConfig {
	if c.MaxNodes == 0 {
		c.MaxNodes = 32
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = 5 * time.Second
	}
	if c.ReadInterval == 0 {
		c.ReadInterval = 30 * time.Second
	}
	if c.MaxPendingKeys == 0 {
		c.MaxPendingKeys = 10000
	}
	return c
}

func hostFor(c storage.Client) testutil.Host {
	return testutil.Host{extID: &testutil.StorageExtension{Client: c}}
}

func TestLocal(t *testing.T) {
	clk := clock.NewFake(t0)
	l := NewLocal(LocalConfig{}, Settings{Clock: clk})
	assert.Equal(t, ModeLocal, l.Mode())
	assert.Equal(t, int64(1), l.Divisor())
	m, asOf, err := l.Totals()
	require.NoError(t, err)
	assert.Empty(t, m)
	assert.Equal(t, t0, asOf)
	require.NoError(t, l.Start(t.Context(), testutil.Host{}))
	l.SetPeriod("p1")
	l.SetPeriod("p1")
	l.Add("p1", "a", logs, hot, 5)
	l.Add("other", "a", logs, hot, 5)
	assert.Equal(t, int64(5), total(t, l, "a"))
	l.SetPeriod("p2")
	assert.Equal(t, int64(0), total(t, l, "a"))
	require.ErrorIs(t, l.Checkpoint(t.Context()), errNotStarted)
	require.NoError(t, l.Shutdown(t.Context()))
	require.NoError(t, l.Shutdown(t.Context()))

	share := NewLocal(LocalConfig{Divisor: 3}, Settings{})
	assert.Equal(t, ModeShare, share.Mode())
	assert.Equal(t, int64(3), share.Divisor())
}

func TestLocalCheckpoint(t *testing.T) {
	mr := miniredis.RunT(t)
	c := testutil.NewRedisClient(mr.Addr(), "")
	clk := clock.NewFake(t0)
	sid := extID
	var errs []string
	set := Settings{Clock: clk, OnError: func(op string) { errs = append(errs, op) }}

	l := NewLocal(LocalConfig{Storage: &sid, CheckpointInterval: 30 * time.Second}, set)
	l.SetPeriod("p1")
	require.NoError(t, l.Start(t.Context(), hostFor(c)))
	l.Add("p1", "a", logs, hot, 100)
	require.NoError(t, l.Checkpoint(t.Context()))
	l.Add("p1", "a", logs, hot, 7)
	require.NoError(t, l.Shutdown(t.Context()), "shutdown writes a final checkpoint")

	l2 := NewLocal(LocalConfig{Storage: &sid}, set)
	l2.SetPeriod("p1")
	require.NoError(t, l2.Start(t.Context(), hostFor(c)))
	assert.Equal(t, int64(107), total(t, l2, "a"))
	c.Fail.Store(true)
	require.Error(t, l2.Checkpoint(t.Context()))
	require.NoError(t, l2.Shutdown(t.Context()))
	assert.Contains(t, errs, "checkpoint")

	// restore errors start from zero
	l3 := NewLocal(LocalConfig{Storage: &sid}, set)
	l3.SetPeriod("p1")
	require.NoError(t, l3.Start(t.Context(), hostFor(c)))
	assert.Equal(t, int64(0), total(t, l3, "a"))
	assert.Contains(t, errs, "restore")
	c.Fail.Store(false)
	require.NoError(t, c.Set(t.Context(), "local/p1", []byte("garbage")))
	l4 := NewLocal(LocalConfig{Storage: &sid}, set)
	l4.SetPeriod("p1")
	require.NoError(t, l4.Start(t.Context(), hostFor(c)))
	assert.Equal(t, int64(0), total(t, l4, "a"))
	require.NoError(t, l4.Shutdown(t.Context()))

	// missing or wrong extension
	l5 := NewLocal(LocalConfig{Storage: &sid}, set)
	require.Error(t, l5.Start(t.Context(), testutil.Host{}))
	require.Error(t, l5.Start(t.Context(), testutil.Host{extID: testutil.NopComponent{}}))
}

func TestLocalCheckpointLoop(t *testing.T) {
	mr := miniredis.RunT(t)
	c := testutil.NewRedisClient(mr.Addr(), "")
	clk := clock.NewFake(t0)
	sid := extID
	l := NewLocal(LocalConfig{Storage: &sid, CheckpointInterval: time.Second}, Settings{Clock: clk})
	l.SetPeriod("p1")
	require.NoError(t, l.Start(t.Context(), hostFor(c)))
	l.Add("p1", "a", logs, hot, 3)
	clk.Advance(time.Second)
	require.Eventually(t, func() bool { return mr.Exists("local/p1") }, 5*time.Second, time.Millisecond)
	require.NoError(t, l.Shutdown(t.Context()))
}

type fleet struct {
	mr      *miniredis.Miniredis
	clk     *clock.Fake
	clients []*testutil.RedisClient
	nodes   []*Storage
	errs    []string
	loops   bool
}

func newFleet(t *testing.T, n int, mode string, plain bool) *fleet {
	f := &fleet{mr: miniredis.RunT(t), clk: clock.NewFake(t0)}
	for i := range n {
		f.start(t, i, mode, plain)
	}
	return f
}

func (f *fleet) start(t *testing.T, i int, mode string, plain bool) *Storage {
	c := testutil.NewRedisClient(f.mr.Addr(), "")
	var sc storage.Client = c
	if plain {
		sc = testutil.PlainClient{Client: c}
	}
	s := NewStorage(withIntervals(StorageConfig{
		Extension: extID, Mode: mode, NodeID: "gw-" + string(rune('0'+i)), NodePrefix: "gw", MaxNodes: 4,
		FlushInterval: 5 * time.Second, ReadInterval: 30 * time.Second, MaxPendingKeys: 3,
	}), Settings{Clock: f.clk, Owner: ownerID, Logger: zap.NewNop(), OnError: func(op string) { f.errs = append(f.errs, op) }})
	s.SetPeriod("p1")
	require.NoError(t, s.Start(t.Context(), hostFor(sc)))
	if !f.loops {
		crash(s) // tests drive Flush and Read by hand
		s.stop = make(chan struct{})
	}
	if i < len(f.nodes) {
		f.nodes[i], f.clients[i] = s, c
	} else {
		f.nodes = append(f.nodes, s)
		f.clients = append(f.clients, c)
	}
	return s
}

func (f *fleet) flushAll(t *testing.T) {
	for _, s := range f.nodes {
		require.NoError(t, s.Flush(t.Context()))
	}
}

func (f *fleet) readAll(t *testing.T) {
	f.clk.Advance(30 * time.Second)
	for _, s := range f.nodes {
		require.NoError(t, s.Read(t.Context()))
	}
}

func TestStorageConvergence(t *testing.T) {
	for _, mode := range []string{ModeIncrement, ModeGCounter} {
		t.Run(mode, func(t *testing.T) {
			f := newFleet(t, 3, mode, false)
			for i, s := range f.nodes {
				assert.Equal(t, mode, s.Mode())
				assert.Equal(t, int64(1), s.Divisor())
				s.Add("p1", "a", logs, hot, int64(100*(i+1)))
				s.Add("p1", "b", budgetapi.SignalTraces, budgetapi.PriceCold, 10)
			}
			f.flushAll(t)
			f.readAll(t)
			for _, s := range f.nodes {
				assert.Equal(t, int64(600), total(t, s, "a"))
				assert.Equal(t, int64(30), total(t, s, "b"))
				_, asOf, err := s.Totals()
				require.NoError(t, err)
				assert.Equal(t, f.clk.Now(), asOf)
			}
			for _, s := range f.nodes {
				require.NoError(t, s.Shutdown(t.Context()))
			}
		})
	}
}

// A replica that never sees a key still picks up its fleet total on every
// read interval: only its own flush responses make it skip a read.
func TestIncrementReaderSeesEveryRead(t *testing.T) {
	f := newFleet(t, 2, ModeIncrement, false)
	writer, reader := f.nodes[0], f.nodes[1]
	for round, want := range []int64{10, 20, 30} {
		writer.Add("p1", "a", logs, hot, 10)
		require.NoError(t, writer.Flush(t.Context()))
		f.clk.Advance(30*time.Second - time.Millisecond) // one read interval, minus ticker jitter
		require.NoError(t, reader.Read(t.Context()))
		assert.Equal(t, want, total(t, reader, "a"), "round %d", round)
	}
}

// Redis losing its data (restart without persistence, FLUSHALL) must not
// lose the period's spend: G-Counter rewrites each node's blob from memory,
// and increment mode restores each node's own contribution.
func TestStorageDataLossHeals(t *testing.T) {
	for _, mode := range []string{ModeIncrement, ModeGCounter} {
		t.Run(mode, func(t *testing.T) {
			f := newFleet(t, 2, mode, false)
			a, b := f.nodes[0], f.nodes[1]
			a.Add("p1", "k", logs, hot, 100)
			b.Add("p1", "k", logs, hot, 50)
			f.flushAll(t)
			f.readAll(t)
			require.Equal(t, int64(150), total(t, a, "k"))

			f.mr.FlushAll() // storage lost everything
			var errs []string
			a.set.OnError = func(op string) { errs = append(errs, op) }
			a.Add("p1", "k", logs, hot, 1)
			b.Add("p1", "k", logs, hot, 2)
			f.flushAll(t)
			f.readAll(t)
			assert.Equal(t, int64(153), total(t, a, "k"))
			assert.Equal(t, int64(153), total(t, b, "k"))
			if mode == ModeIncrement {
				assert.Contains(t, errs, "storage_reset", "data loss is reported")
				// and it does not restore twice
				a.Add("p1", "k", logs, hot, 1)
				f.flushAll(t)
				f.readAll(t)
				assert.Equal(t, int64(154), total(t, b, "k"))
			}
		})
	}
}

// Replicas share their tiers: a replica sees the highest tier any other live
// replica decided, and a replica that stopped writing no longer counts.
func TestFleetTiers(t *testing.T) {
	for _, mode := range []string{ModeIncrement, ModeGCounter} {
		t.Run(mode, func(t *testing.T) {
			f := newFleet(t, 3, mode, false)
			a, b, c := f.nodes[0], f.nodes[1], f.nodes[2]
			a.PublishTiers(map[string]int{"k": 2, "group:g": 1})
			b.PublishTiers(map[string]int{"k": 1})
			f.flushAll(t)
			f.readAll(t)
			now := f.clk.Now()
			assert.Equal(t, map[string]int{"k": 2, "group:g": 1}, c.FleetTiers(now, time.Minute))
			assert.Equal(t, map[string]int{"k": 1}, a.FleetTiers(now, time.Minute), "a does not see its own tiers")

			// a stops writing; after max age its tier no longer counts
			b.PublishTiers(map[string]int{"k": 1})
			f.clk.Advance(2 * time.Minute)
			require.NoError(t, b.Flush(t.Context()))
			require.NoError(t, c.Read(t.Context()))
			assert.Equal(t, map[string]int{"k": 1}, c.FleetTiers(f.clk.Now(), time.Minute))

			c.SetPeriod("p2")
			assert.Empty(t, c.FleetTiers(f.clk.Now(), time.Minute), "a new period starts clean")
		})
	}
}

func TestStorageRestartLosesAtMostOneFlush(t *testing.T) {
	for _, mode := range []string{ModeIncrement, ModeGCounter} {
		t.Run(mode, func(t *testing.T) {
			f := newFleet(t, 2, mode, false)
			f.nodes[0].Add("p1", "a", logs, hot, 1000)
			f.nodes[1].Add("p1", "a", logs, hot, 1000)
			f.flushAll(t)
			f.nodes[0].Add("p1", "a", logs, hot, 5) // never flushed: lost in the crash
			// crash node 0 without shutdown, then restart it with the same node ID
			f.start(t, 0, mode, false)
			f.nodes[0].Add("p1", "a", logs, hot, 1)
			f.flushAll(t)
			f.readAll(t)
			for _, s := range f.nodes {
				assert.Equal(t, int64(2001), total(t, s, "a"))
			}
		})
	}
}

func TestStorageErrors(t *testing.T) {
	f := newFleet(t, 1, ModeIncrement, false)
	s, c := f.nodes[0], f.clients[0]
	s.Add("p1", "a", logs, hot, 10)
	c.Fail.Store(true)
	require.Error(t, s.Flush(t.Context()))
	f.clk.Advance(time.Minute)
	require.Error(t, s.Read(t.Context()))
	_, asOf, _ := s.Totals()
	assert.Equal(t, t0, asOf, "failed reads do not refresh as-of")
	c.Fail.Store(false)
	s.Add("p1", "a", logs, hot, 1)
	require.NoError(t, s.Flush(t.Context()))
	f.readAll(t)
	assert.Equal(t, int64(1), total(t, s, "a"), "failed batch dropped, not retried")

	c.FailAfterApply.Store(true)
	s.Add("p1", "a", logs, hot, 4)
	require.Error(t, s.Flush(t.Context()))
	c.FailAfterApply.Store(false)
	require.NoError(t, s.Flush(t.Context()), "nothing pending, no retry")
	f.readAll(t)
	assert.Equal(t, int64(5), total(t, s, "a"), "partial apply counted exactly once")
	assert.Contains(t, f.errs, "flush")
	assert.Contains(t, f.errs, "read")

	// pending keys are bounded
	overflows := 0
	s.set.OnOverflow = func() { overflows++ }
	for _, id := range []string{"k1", "k2", "k3", "k4", "k5"} {
		s.Add("p1", id, logs, hot, 1)
	}
	assert.Equal(t, 2, overflows)
}

func TestStorageOutageKeepsSpend(t *testing.T) {
	f := newFleet(t, 2, ModeIncrement, false)
	a, b := f.nodes[0], f.nodes[1]
	a.Add("p1", "k", logs, hot, 10)
	f.flushAll(t)
	f.readAll(t)

	// storage unreachable: flushes fail before sending and keep the spend
	f.clients[0].FailDial.Store(true)
	a.Add("p1", "k", logs, hot, 5)
	require.Error(t, a.Flush(t.Context()))
	a.Add("p1", "k", logs, hot, 5)
	require.Error(t, a.Flush(t.Context()))
	assert.Equal(t, int64(20), total(t, a, "k"), "own spend never goes back during an outage")

	f.clients[0].FailDial.Store(false)
	require.NoError(t, a.Flush(t.Context()))
	f.readAll(t)
	assert.Equal(t, int64(20), total(t, a, "k"))
	assert.Equal(t, int64(20), total(t, b, "k"), "the outage's spend reaches the fleet once")
}

func TestStorageTotalsIncludeInFlight(t *testing.T) {
	f := newFleet(t, 1, ModeIncrement, false)
	s := f.nodes[0]
	s.Add("p1", "k", logs, hot, 7)
	var during int64
	f.clients[0].BeforeIncrement = func() { during = total(t, s, "k") }
	require.NoError(t, s.Flush(t.Context()))
	assert.Equal(t, int64(7), during, "a tick during a flush still sees the drained spend")
	assert.Equal(t, int64(7), total(t, s, "k"))
}

func TestStorageRolloverDuringFlush(t *testing.T) {
	f := newFleet(t, 1, ModeIncrement, false)
	s := f.nodes[0]
	for i := range 5000 {
		s.Add(s.p.Load().cur.ID, "k", logs, hot, 1)
		period := fmt.Sprintf("p%d", i+2)
		done := make(chan struct{})
		go func() {
			defer close(done)
			assert.NoError(t, s.Flush(t.Context()))
		}()
		s.SetPeriod(period)
		<-done
		require.Equal(t, period, s.p.Load().cur.ID, "a flush racing the rollover put the old period back: new spend would be dropped")
	}
}

func TestStorageRolloverIsNotDataLoss(t *testing.T) {
	f := newFleet(t, 1, ModeIncrement, false)
	s := f.nodes[0]
	for range 3 {
		s.Add("p1", "k", logs, hot, 1)
		require.NoError(t, s.Flush(t.Context()))
	}
	// the period rolls over while a flush of the old one is in flight
	f.clients[0].BeforeIncrement = func() {
		f.clients[0].BeforeIncrement = nil
		s.SetPeriod("p2")
	}
	s.Add("p1", "k", logs, hot, 1)
	require.NoError(t, s.Flush(t.Context()))
	s.Add("p2", "k", logs, hot, 1)
	require.NoError(t, s.Flush(t.Context()))
	assert.NotContains(t, f.errs, "storage_reset", "the new period's first marker is not a data loss")
}

func TestStorageGCounterErrors(t *testing.T) {
	f := newFleet(t, 1, ModeGCounter, true)
	s, c := f.nodes[0], f.clients[0]
	c.Fail.Store(true)
	require.Error(t, s.Flush(t.Context()))
	require.Error(t, s.Read(t.Context()))
	c.Fail.Store(false)
	require.NoError(t, c.Set(t.Context(), "p1/node/gw-1", []byte("junk")))
	require.NoError(t, s.Read(t.Context()))
	// a restart with a corrupt own blob starts from zero
	require.NoError(t, c.Set(t.Context(), "p1/node/gw-0", []byte("junk")))
	f.start(t, 0, ModeGCounter, true)
	c.Fail.Store(true)
	s2 := NewStorage(withIntervals(StorageConfig{Extension: extID, Mode: ModeGCounter, NodeID: "gw-0", NodePrefix: "gw"}), Settings{Clock: f.clk})
	s2.SetPeriod("p1")
	require.Error(t, s2.Start(t.Context(), hostFor(c)), "cannot restore own counters")
}

func TestStorageRollover(t *testing.T) {
	for _, mode := range []string{ModeIncrement, ModeGCounter} {
		t.Run(mode, func(t *testing.T) {
			f := newFleet(t, 2, mode, false)
			a := f.nodes[0]
			a.Add("p1", "a", logs, hot, 10)
			a.SetPeriod("p2")
			a.Add("p1", "a", logs, hot, 5) // straggler for the old period
			a.Add("p2", "a", logs, hot, 1)
			a.Add("p0", "a", logs, hot, 99) // unknown period ignored
			assert.Equal(t, int64(1), total(t, a, "a"))
			require.NoError(t, a.Flush(t.Context()))
			f.nodes[1].SetPeriod("p2")
			f.readAll(t)
			assert.Equal(t, int64(1), total(t, f.nodes[1], "a"))
			assert.Nil(t, a.p.Load().prev, "previous period dropped after its flush")
			if mode == ModeIncrement {
				assert.Equal(t, "15", mustGet(t, f.mr, "p1/a/logs/hot"))
			} else {
				assert.True(t, f.mr.Exists("p1/node/gw-0"))
			}
		})
	}
}

func crash(s *Storage) {
	close(s.stop)
	s.wg.Wait()
}

func mustGet(t *testing.T, mr *miniredis.Miniredis, k string) string {
	v, err := mr.Get(k)
	require.NoError(t, err)
	return v
}

func TestStorageModes(t *testing.T) {
	mr := miniredis.RunT(t)
	c := testutil.NewRedisClient(mr.Addr(), "")
	s := NewStorage(withIntervals(StorageConfig{Extension: extID, Mode: ModeIncrement}), Settings{})
	assert.Equal(t, ModeStorage, s.Mode())
	require.Error(t, s.Start(t.Context(), hostFor(testutil.PlainClient{Client: c})))
	require.ErrorIs(t, s.Flush(t.Context()), errNotStarted)
	require.ErrorIs(t, s.Read(t.Context()), errNotStarted)
	_, _, err := s.Totals()
	require.ErrorIs(t, err, errNotStarted)
	require.NoError(t, s.Shutdown(t.Context()))

	auto := NewStorage(withIntervals(StorageConfig{Extension: extID, Mode: ModeAuto}), Settings{})
	auto.SetPeriod("p1")
	require.NoError(t, auto.Start(t.Context(), hostFor(c)))
	assert.Equal(t, ModeIncrement, auto.Mode())
	require.NoError(t, auto.Shutdown(t.Context()))
}

// The real redis_storage extension, whichever version the module is built
// with: released contrib (no increments, auto selects G-Counter) or the
// BatchIncrementBy change from contrib PR #51568 (auto selects increment).
// `make test-redis-pr` runs this file against the PR.
func TestRealRedisStorage(t *testing.T) {
	newExt := func(t *testing.T) (component.Component, bool) {
		mr := miniredis.RunT(t)
		f := redisstorageextension.NewFactory()
		cfg := f.CreateDefaultConfig().(*redisstorageextension.Config)
		cfg.Endpoint = mr.Addr()
		cfg.TLS.Insecure = true
		ext, err := f.Create(t.Context(), extensiontest.NewNopSettings(f.Type()), cfg)
		require.NoError(t, err)
		require.NoError(t, ext.Start(t.Context(), testutil.Host{}))
		t.Cleanup(func() { require.NoError(t, ext.Shutdown(context.WithoutCancel(t.Context()))) })
		c, err := ext.(storage.Extension).GetClient(t.Context(), component.KindExtension, ownerID, StorageName)
		require.NoError(t, err)
		_, inc := c.(Incrementer)
		return ext, inc
	}
	_, supported := newExt(t)
	t.Logf("redis_storage supports BatchIncrementBy: %v", supported)

	for _, mode := range []string{ModeAuto, ModeGCounter, ModeIncrement} {
		t.Run(mode, func(t *testing.T) {
			ext, inc := newExt(t)
			clk := clock.NewFake(t0)
			start := func(i int) *Storage {
				s := NewStorage(withIntervals(StorageConfig{Extension: extID, Mode: mode, NodeID: "gw-" + string(rune('0'+i)), NodePrefix: "gw", MaxNodes: 3}), Settings{Clock: clk, Owner: ownerID})
				s.SetPeriod("p1")
				err := s.Start(t.Context(), testutil.Host{extID: ext})
				if mode == ModeIncrement && !inc {
					require.Error(t, err, "increment mode needs BatchIncrementBy")
					return nil
				}
				require.NoError(t, err)
				return s
			}
			nodes := make([]*Storage, 3)
			for i := range nodes {
				if nodes[i] = start(i); nodes[i] == nil {
					return
				}
			}
			want := map[string]string{ModeAuto: ModeGCounter, ModeGCounter: ModeGCounter, ModeIncrement: ModeIncrement}[mode]
			if mode == ModeAuto && inc {
				want = ModeIncrement
			}
			assert.Equal(t, want, nodes[0].Mode())

			for i, s := range nodes {
				s.Add("p1", "a", logs, hot, int64(10*(i+1)))
			}
			for _, s := range nodes {
				require.NoError(t, s.Flush(t.Context()))
			}
			// a restarted replica keeps what it flushed
			require.NoError(t, nodes[0].Shutdown(t.Context()))
			nodes[0] = start(0)
			nodes[0].Add("p1", "a", logs, hot, 1)
			require.NoError(t, nodes[0].Flush(t.Context()))

			clk.Advance(30 * time.Second)
			for _, s := range nodes {
				require.NoError(t, s.Read(t.Context()))
				assert.Equal(t, int64(61), total(t, s, "a"), "all replicas converge after one read interval")
			}
			for _, s := range nodes {
				require.NoError(t, s.Shutdown(t.Context()))
			}
		})
	}
}

func TestLoopsTick(t *testing.T) {
	f := &fleet{mr: miniredis.RunT(t), clk: clock.NewFake(t0), loops: true}
	s := f.start(t, 0, ModeIncrement, false)
	s.Add("p1", "a", logs, hot, 3)
	f.clk.Advance(5 * time.Second)
	require.Eventually(t, func() bool { return f.clients[0].Increments.Load() > 0 }, 5*time.Second, time.Millisecond)
	require.NoError(t, s.Shutdown(t.Context()))
}

func TestBlobRoundTrip(t *testing.T) {
	var sp ledger.Spend
	sp[budgetapi.SignalMetrics][budgetapi.PriceCold] = 42
	data, err := encodeBlob(map[string]ledger.Spend{"a": sp, "z": {}})
	require.NoError(t, err)
	m, err := decodeBlob(data)
	require.NoError(t, err)
	assert.Equal(t, int64(42), m["a"][budgetapi.SignalMetrics][budgetapi.PriceCold])
	m, err = decodeBlob(nil)
	require.NoError(t, err)
	assert.Empty(t, m)
	_, err = decodeBlob([]byte("nope"))
	require.Error(t, err)
	bad := encoder.EncodeAll([]byte("{"), nil)
	_, err = decodeBlob(bad)
	require.Error(t, err)

	for _, k := range []string{"p1/a%2Fb/logs/hot", "x", "p/%zz/logs/hot", "p/a/bad/hot", "p/a/logs/bad"} {
		_, id, _, _, ok := parseCounterKey(k)
		if k == "p1/a%2Fb/logs/hot" {
			assert.True(t, ok)
			assert.Equal(t, "a/b", id)
		} else {
			assert.False(t, ok, k)
		}
	}
}
