//go:build results

package spendsync

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/redisstorageextension"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/extensiontest"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/clock"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/testutil"
)

// Redis load per replica per sync cycle (one flush and one read), increment
// versus G-Counter, against a real Redis (REDIS_ADDR) and the real
// redis_storage extension. Increment mode needs the BatchIncrementBy change
// from contrib PR #51568: `make results-redis` builds against it.
func TestResultsRedisLoad(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	admin := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = admin.Close() }()
	ctx := context.Background()

	stats := func() (cmds, in, out int64) {
		info, err := admin.Info(ctx, "stats").Result()
		require.NoError(t, err)
		for _, line := range strings.Split(info, "\r\n") {
			k, v, ok := strings.Cut(line, ":")
			if !ok {
				continue
			}
			n, _ := strconv.ParseInt(v, 10, 64)
			switch k {
			case "total_commands_processed":
				cmds = n
			case "total_net_input_bytes":
				in = n
			case "total_net_output_bytes":
				out = n
			}
		}
		return cmds, in, out
	}

	type row struct {
		Mode             string  `json:"mode"`
		Replicas         int     `json:"replicas"`
		KeysPerReplica   int     `json:"keys_per_replica"`
		CommandsPerCycle float64 `json:"redis_commands_per_replica_cycle"`
		BytesInPerCycle  float64 `json:"redis_bytes_in_per_replica_cycle"`
		BytesOutPerCycle float64 `json:"redis_bytes_out_per_replica_cycle"`
		SyncMsPerReplica float64 `json:"flush_plus_read_ms_per_replica"`
		Converged        bool    `json:"converged"`
	}
	var rows []row
	const cycles = 5
	for _, mode := range []string{ModeIncrement, ModeGCounter} {
		for _, replicas := range []int{3, 10, 32} {
			for _, keys := range []int{100, 1000, 10000} {
				require.NoError(t, admin.FlushAll(ctx).Err())
				f := redisstorageextension.NewFactory()
				cfg := f.CreateDefaultConfig().(*redisstorageextension.Config)
				cfg.Endpoint = addr
				cfg.TLS.Insecure = true
				ext, err := f.Create(ctx, extensiontest.NewNopSettings(f.Type()), cfg)
				require.NoError(t, err)
				require.NoError(t, ext.Start(ctx, testutil.Host{}))

				clk := clock.NewFake(t0)
				nodes := make([]*Storage, replicas)
				for i := range nodes {
					nodes[i] = NewStorage(StorageConfig{
						Extension: extID, Mode: mode, NodeID: fmt.Sprintf("gw-%d", i), NodePrefix: "gw",
						MaxNodes: 32, FlushInterval: 5 * time.Second, ReadInterval: 30 * time.Second, MaxPendingKeys: 100000,
					}, Settings{Clock: clk, Owner: component.MustNewID("budget")})
					nodes[i].SetPeriod("p1")
					require.NoError(t, nodes[i].Start(ctx, testutil.Host{extID: ext}))
					// count only the explicit flush and read below, not the loops
					close(nodes[i].stop)
					nodes[i].wg.Wait()
					nodes[i].stop = make(chan struct{})
				}
				c0, in0, out0 := stats()
				start := time.Now()
				for range cycles {
					for _, n := range nodes {
						for k := range keys {
							n.Add("p1", fmt.Sprintf("svc-%04d", k), logs, hot, 1)
						}
					}
					for _, n := range nodes {
						require.NoError(t, n.Flush(ctx))
					}
					clk.Advance(30 * time.Second)
					for _, n := range nodes {
						require.NoError(t, n.Read(ctx))
					}
				}
				elapsed := time.Since(start)
				c1, in1, out1 := stats()
				// one more read after a full read interval: every replica must agree
				clk.Advance(31 * time.Second)
				for _, n := range nodes {
					require.NoError(t, n.Read(ctx))
				}
				per := float64(replicas * cycles)
				want := int64(replicas * cycles)
				converged := true
				for _, n := range nodes {
					if total(t, n, "svc-0000") != want {
						converged = false
					}
					require.NoError(t, n.Shutdown(ctx))
				}
				require.NoError(t, ext.Shutdown(ctx))
				rows = append(rows, row{
					Mode: mode, Replicas: replicas, KeysPerReplica: keys,
					CommandsPerCycle: float64(c1-c0) / per,
					BytesInPerCycle:  float64(in1-in0) / per,
					BytesOutPerCycle: float64(out1-out0) / per,
					SyncMsPerReplica: float64(elapsed.Microseconds()) / 1000 / per,
					Converged:        converged,
				})
				t.Logf("%s replicas=%d keys=%d done", mode, replicas, keys)
			}
		}
	}
	b, err := json.MarshalIndent(map[string]any{"redis_load": rows}, "", "  ")
	require.NoError(t, err)
	t.Log(string(b))
	if p := os.Getenv("RESULTS_OUT"); p != "" {
		require.NoError(t, os.WriteFile(p, b, 0o600))
	}
}
