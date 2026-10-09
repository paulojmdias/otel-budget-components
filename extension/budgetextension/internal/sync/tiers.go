package spendsync // import "github.com/paulojmdias/otel-budget-components/extension/budgetextension/internal/sync"

import (
	"context"
	"encoding/json"
	"maps"
	"time"

	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"
)

// TierSync is implemented by syncs that share each replica's tiers, so the
// fleet can apply the most restrictive tier any live replica decided.
// Replicas decide on fleet totals read at slightly different times; near a
// threshold one may escalate and another not, and hysteresis would keep them
// apart. Taking the maximum makes every replica agree within one read
// interval, without a leader.
type TierSync interface {
	// PublishTiers sets this replica's tiers per ledger ID (non zero only).
	PublishTiers(tiers map[string]int)
	// FleetTiers returns, per ledger ID, the highest tier published by the
	// other replicas in the last maxAge.
	FleetTiers(now time.Time, maxAge time.Duration) map[string]int
}

var _ TierSync = (*Storage)(nil)

type tierBlob struct {
	At    int64          `json:"at"` // Unix nanoseconds, writer's clock
	Tiers map[string]int `json:"tiers"`
}

func (*Storage) tiersKey(periodID, node string) string { return periodID + "/tiers/" + node }

// PublishTiers records this replica's tiers; they are written on the next flush.
func (s *Storage) PublishTiers(tiers map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ownTiers = maps.Clone(tiers)
}

// FleetTiers returns the maximum tier per ledger ID over the other replicas'
// recent blobs.
func (s *Storage) FleetTiers(now time.Time, maxAge time.Duration) map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int{}
	for _, b := range s.peerTiers {
		if now.UnixNano()-b.At > int64(maxAge) {
			continue // the replica stopped writing: it must not pin a tier
		}
		for id, t := range b.Tiers {
			out[id] = max(out[id], t)
		}
	}
	return out
}

// writeTiers stores this replica's tiers with a timestamp; called on every
// flush so the timestamp also works as a heartbeat. Caller holds flushMu.
func (s *Storage) writeTiers(ctx context.Context, periodID string) error {
	s.mu.Lock()
	b := tierBlob{At: s.set.Clock.Now().UnixNano(), Tiers: s.ownTiers}
	s.mu.Unlock()
	j, err := json.Marshal(b)
	if err != nil {
		return err
	}
	if err := s.client.Set(ctx, s.tiersKey(periodID, s.cfg.NodeID), encoder.EncodeAll(j, nil)); err != nil {
		s.set.OnError("write_tiers")
		return err
	}
	return nil
}

// readTiers loads the other replicas' tiers. Caller holds flushMu.
func (s *Storage) readTiers(ctx context.Context, periodID string) error {
	var ops []*storage.Operation
	for _, n := range s.nodes() {
		if n != s.cfg.NodeID {
			ops = append(ops, storage.GetOperation(s.tiersKey(periodID, n)))
		}
	}
	if err := s.client.Batch(ctx, ops...); err != nil {
		s.set.OnError("read_tiers")
		return err
	}
	peers := map[string]tierBlob{}
	for _, op := range ops {
		if op.Value == nil {
			continue
		}
		j, err := decoder.DecodeAll(op.Value, nil)
		var b tierBlob
		if err == nil {
			err = json.Unmarshal(j, &b)
		}
		if err != nil {
			s.set.Logger.Warn("skipping corrupt tiers blob", zap.String("key", op.Key), zap.Error(err))
			continue
		}
		peers[op.Key] = b
	}
	s.mu.Lock()
	s.peerTiers = peers
	s.mu.Unlock()
	return nil
}
