# Decisions

Choices made where the specification was silent, deviations from it, and the reason for each. The fixed design decisions (extension holds state, lock free hot path, account what survives, staleness tolerance, fail open, never drop what matters, diversion by the routing connector, backend agnostic, three sync modes, no membership protocol) are implemented as specified.

## Scope changes requested by the maintainer

| Change | Reason |
|---|---|
| No HTTP API | The extension has no listener. State is observed through self telemetry; budgets and exemptions come from configuration only. An earlier version had `/v1/state`, runtime overrides with TTLs, and an Alertmanager webhook. |
| Static `exempt` list instead of overrides | Keys or `group:<rule>` listed in `exempt` are accounted but never enforced. No TTLs and no fleet propagation; every replica reads the same config. |
| No batch processor anywhere | Exporters batch through `sending_queue::batch`. The file exporter has no queue and writes directly. |
| `otlphttp` replaced by `otlp_http` | The old alias is deprecated in v0.162.0. |
| Self contained components | Everything the extension needs lives under `extension/budgetextension` (`internal/*` plus the exported `budgetapi` subpackage). The processor imports only `budgetapi`. This matches contrib, where each component is its own module. |

## Versions

- Collector core `v1.68.0` / `v0.162.0`, contrib `v0.162.0` (k8sattributes is `v1.1.0`, it moved to the stable module set). Go `1.26.0` as required by that release.
- `github.com/gobwas/glob` is pinned to `v0.2.3`: `v1.0.0` changed its API under the same module path and breaks `confmap`.
- Code generators (`mdatagen`, `schemagen`, `builder`) are built from `internal/tools/go.mod` because `go install ...@version` refuses modules with replace directives.

## Hot path

- **No mutex on the processor path.** `ResolveKey`, `Decision`, `Record`, `RecordOffered`, and `RecordDropped` use `sync.Map` loads, atomics, CAS, and one atomic pointer load. `sync.Map` takes its internal lock only the first time a key is stored (new key per period, new ledger ID), so the steady state is lock free.
- **Key cache.** The "bounded LRU" for resource attribute hash to resolution is a direct mapped array of 4096 atomic pointers. A true LRU needs a lock on every hit. Entries store the attribute values and are verified on hit, so a hash collision can only cost a miss, never a wrong key.
- **Resolved key format.** `ResolveKey` returns the plain key when no rule matches on resource attributes. Otherwise it returns `key \x1f <key rule index> ; <group rule indexes>` so `Record` and `Decision` know the attribute based group membership without seeing the resource. `budgetapi.KeyName` strips the suffix; `otelcol.budget.key` is always the plain key. Tokens from an earlier period are decoded, not rejected.
- **Decisions for new keys.** The published map is immutable. A key first seen after the last publish is combined on the fly from the per ledger results of the same snapshot.
- **Sub micro remainders.** Pricing returns whole micro units plus a remainder in 1e-9 micro units (128 bit intermediate math). The ledger counter carries the remainder with a CAS loop, so a stream of small batches is not rounded to zero. Only whole micro units are forwarded to the sync layer.
- **Compression ratio** is applied as an integer parts per million factor; the fractional byte per batch is dropped.
- **Series identity.** The spec asks for xxhash over sorted attributes. Sorting allocates per data point, so attribute maps are hashed by summing per entry hashes, which is order independent and gives the same identity semantics. Resource attributes starting with `budget.` are excluded so stamping does not create new series. The helpers live in `budgetapi` because every processor must hash the same way.
- **Active series sketch.** `axiomhq/hyperloglog` sketches are not safe for concurrent inserts and would need a lock in `Record`. The extension uses its own HyperLogLog (precision 12, dense, 8 bit registers packed four per `atomic.Uint32`, CAS max). Measured error stays under 5% from 100 to 200k series. Sketches exist per ledger ID and price class, so group series counts are unions, not sums.

## Accounting

- **Offered volume** is recorded only by `price_class: hot` processors. The cold pipeline sees data the hot pipeline already offered.
- **Accepted bytes** are measured after actions and after stamping, which is what the exporter sends.
- **Diverted data** is stamped and not recorded by the hot processor; the cold processor records it at cold prices.
- **Metrics `drop_metrics`** count dropped data points under reason `metric_glob`.
- **Samples by trace ID.** Logs without a trace ID are sampled by xxhash of body and timestamp. ERROR spans always pass, so a sampled out trace can keep only its error spans; apart from that, a trace is kept or dropped as a whole on every replica and signal.

## Decision engine

- `sample_ratio: 0` (unset) means `1.0`.
- Burn rates are computed on a monotonic cumulative total (previous periods plus the current period), so a period rollover does not reset the windows or restart warm up. Ledgers without spend in the ending period stop being carried.
- Ring samples are stamped with the sync as-of time, not the tick time, so a frozen sync layer produces no new samples. Escalation is skipped when stale; de-escalation still runs.
- Escalation can jump several tiers at once; de-escalation is one step at a time and needs `min_dwell` below `threshold * ratio` again for the next step.
- **De-escalation uses offered spend too.** Deciding only on accepted spend made a noisy key bounce: tier 1 drops its DEBUG, accepted burn falls, it de-escalates, and escalates again (19 tier changes in 2 hours in the engine test, 13 per hour in the extension). Offered usage is now priced at hot prices and synced as `offered:<id>`; a key de-escalates only when both burns are low. Escalation still uses accepted spend, which is what costs money. With an older processor that reports no offered usage, behaviour falls back to accepted only.
- **Fixed size ring buffers.** One sample per decision interval over a 6 h window was 2,160 samples per key, 1.4 GB of heap for 10,000 keys. Rings now keep 32 samples per window, so 10,000 keys take about 42 MB whatever the windows. Burn rates stay exact over the span they cover; the span starts at most 1/32 of the window early.
- The hard cap is not applied while stale (it is an escalation).
- An exempt key keeps its internal tier; the published tier is 0. If it is removed from `exempt` (config reload), it resumes at the tier its burn justifies.
- Exempting `group:<name>` exempts every member.
- The default rule must have `scope: key`, so every key has its own budget.
- Ticks are serialized and their time never goes backwards. A tick that waited on the lock with an older timestamp once rolled the period back in a test.

## Sync

- `SpendSync` has three methods beyond the spec: `SetPeriod` (the sync layer needs the current period to read fleet keys), `Mode`, and `Divisor`.
- **Increment mode key discovery.** A replica must read keys it has never seen itself. Each node keeps `{period}/keys/{node_id}`, the zstd compressed list of counter keys it has written; readers union the indexes of `node_prefix-0..max_nodes-1`. Only counters that exist are read, and they are read with `BatchIncrementBy` and a zero delta, one pipelined round trip (the storage client's `Batch` issues GETs one by one). Only a replica's own flush responses mark a counter fresh.
- **As-of.** It advances only when a whole read round (every counter key) succeeds, or after a successful increment flush. Before the first successful read the as-of is zero, so a replica that cannot reach storage at startup fails open.
- **Increment flush** swaps in a fresh pending table and drains the old one; a table retired by the previous flush is drained once more to catch adds that raced with the swap. The swap, the drain, and the in flight batch are visible to `Totals` under one lock, so a decision tick never sees own spend vanish mid flush. When the batch fails before it is sent (dialing failed, as while storage is down) it goes back to pending and is retried: an outage delays spend, it does not lose it. Any other error may have partially applied, so that batch is dropped and never retried. A period rollover swaps the tables under the same lock, so a flush in flight never puts the old period back, and alive markers are compared within one period only, so the first flush of a new period is not taken for a data loss.
- **Storage data loss heals.** A Redis restart without persistence, an eviction, or a `FLUSHALL` would lose the period's spend. In G-Counter mode each node rewrites its blob from memory on the next flush. In increment mode each flush also increments a per node marker (`{period}/alive/{node}`) in the same pipeline; when it comes back lower than expected, the node adds its own earlier contribution back once and rewrites its key index. Each node restores only its own share, so nodes do not race. The event is counted as a `storage_reset` sync error. A node that restarted itself cannot restore what it flushed before its own restart, but that part is still in Redis unless both happened together.
- **Replicas share tiers.** Each replica decides on fleet totals read at slightly different times, so near a threshold one replica could escalate and another not, and hysteresis kept them apart (found by the Kubernetes gates: payments at burn 2.1 against a 2.0 threshold sat at tier 2 on one replica and tier 1 on two). In storage mode each replica writes its own tiers with a timestamp on every flush (`{period}/tiers/{node}`) and reads the others' on every read; the published tier is the highest tier any replica decided in the last `stale_after`. Escalation spreads within one read interval, de-escalation needs every replica to agree, and a replica that stops writing stops counting. No leader, no membership. Share and local modes stay per replica.
- **Totals going backwards restart the windows.** If a ledger's fleet total ever decreases, the engine restarts its burn rate windows instead of computing negative burn rates for up to a long window.
- **Extension start order.** The extension implements `extensioncapabilities.Dependent` and returns its storage extension, so the Collector starts storage first. Without it `redis_storage` could hand out a client before creating its Redis connection (seen as a crash in a 3 replica deployment).
- **The distribution pins the `redis_storage` PR.** The OCB manifest replaces `redis_storage` with the PR #51568 branch at a fixed commit, so the default build exercises increment mode. Components themselves do not depend on it: with released `redis_storage` they use G-Counter.
- **`node_id` and `node_prefix` are required in every storage mode.** Both modes find the other replicas by those names (G-Counter blobs, increment key index). Without `node_prefix` a replica would silently see only its own spend.
- **Upstream `redis_storage` and TLS.** Its default TLS config is not insecure; plain Redis needs `tls: { insecure: true }`.
- **Increment support.** Upstream `redis_storage` v0.162.0 has no `BatchIncrementBy`; auto mode selects G-Counter. The change is proposed in open-telemetry/opentelemetry-collector-contrib#51568, whose signature matches the `Incrementer` interface here. `make build REDIS_FORK=<module>@<version>` or a local path builds against it.

## Self telemetry

- Counters (`spend`, `offered_size`, `dropped_items`) are emitted by the decision loop as deltas of atomic counters, never from the hot path.
- `spend` is this replica's local spend. Fleet spend is the sum across replicas.
- The cardinality guard ranks ledger IDs by fleet spend. Gauges for keys outside the top N are aggregated under `__other__` as max tier, max burn, and summed budget.
- `pending_overflow` is reported as a `sync_errors` op.

## Tests and tooling

- No test sleeps; time comes from the fake clock. Tests that run a real Collector or background loops wait on conditions with `require.Eventually`.
- Each component is its own Go module, like in contrib. The extension tests use a small in-test driver that plays the processor side of `budgetapi`, so the extension module does not depend on the processor module. The real processor and extension are tested together in the root module (`examples/`), through in-process Collectors.
- Lint uses contrib's `.golangci.yml` with the import prefix changed and the exclusion rules for contrib-only paths removed.
- The em dash and en dash rule is enforced by `make lint`.
