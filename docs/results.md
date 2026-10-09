# Results

Measured on 2026-10-04 on an Apple M4 (10 cores), Go 1.27.1, Collector v0.162.0, with `redis_storage` from contrib PR #51568 (commit `e9a5c2f`). Each section says how to reproduce it and what it does not show.

| Claim | Evidence |
|---|---|
| Keeps spend within budget, and adapts where static rules cannot | [Scenarios](#1-scenarios-compared-with-static-rules) |
| Never drops errors | [Scenarios](#1-scenarios-compared-with-static-rules), [Kubernetes run](#7-kubernetes-run) |
| Does not flap | [Tier stability](#2-tier-stability) |
| Negligible overhead | [Overhead](#3-overhead), [Latency](#4-pipeline-latency) |
| Scales to many keys and replicas | [Scale](#5-scale) |
| Replicas agree on fleet spend | [Fleet convergence](#6-fleet-convergence), [Kubernetes run](#7-kubernetes-run) |
| Accounting is exact | [Accounting](#8-accounting) |

## 1. Scenarios compared with static rules

`make results`. The real extension on a fake clock over one hour, with a driver that applies decisions like the processor (severity drop, consistent sampling by trace ID). Static strategies apply a fixed rule to the same traffic. Spend is the proto size of what is exported, priced at 0.40 per GB, divided by the budget. The budget is sized so normal traffic burns 0.5 (half the budget). Budget tiers: tier 1 at burn 1.0 drops below INFO; tier 2 at burn 2.0 drops below WARN and keeps 25%.

Traffic every 5 s: normal is 100 INFO, 5 WARN, 1 ERROR.

| Scenario | Strategy | Spend vs budget | DEBUG kept | INFO kept | WARN kept | ERROR kept | Tier changes |
|---|---|---|---|---|---|---|---|
| **normal** | none | 0.50 | | 100% | 100% | 100% | |
| | static filter (drop below INFO) | 0.50 | | 100% | 100% | 100% | |
| | static sampler (25%) | 0.13 | | 25% | 25% | 24% | |
| | **budget** | **0.50** | | **100%** | **100%** | **100%** | 0 |
| **noisy deploy** (+400 DEBUG from minute 10) | none | 2.17 | 100% | 100% | 100% | 100% | |
| | static filter | 0.50 | 0% | 100% | 100% | 100% | |
| | static sampler | 0.54 | 25% | 25% | 25% | 24% | |
| | **budget** | **0.55** | 3% | **100%** | **100%** | **100%** | 1 |
| **incident** (3x volume, 50x ERROR, minutes 20 to 40) | none | 0.91 | | 100% | 100% | 100% | |
| | static filter | 0.91 | | 100% | 100% | 100% | |
| | static sampler | 0.23 | | 25% | 25% | 25% | |
| | **budget** | **0.91** | | **100%** | **100%** | **100%** | 2 |
| **growth** (INFO triples from minute 10) | none | 1.34 | | 100% | 100% | 100% | |
| | static filter | 1.34 | | 100% | 100% | 100% | |
| | static sampler | 0.34 | | 25% | 25% | 25% | |
| | **budget** | **1.01** | | 75% | 81% | **100%** | 2 |

What it shows:

- On a normal day the budget changes nothing; a static sampler throws away three quarters of everything, errors included, to save money that was not at risk.
- A DEBUG flood is cut at the source (3% kept) while INFO, WARN, and ERROR are untouched. The static filter matches it here, but only because the rule was written for exactly this case.
- During an incident everything is kept, including the 50x ERROR spike. The key escalates (tier 1 drops below INFO, and there is no DEBUG to drop) and comes back.
- Organic growth that no static rule anticipates is contained at the budget: the hard cap holds tier 2 once the period spend reaches the budget, so spend ends at 1.01x instead of 1.34x, and errors still pass.

What it does not show: the traffic is synthetic, and one hour with minute windows is a compressed version of a month with hour windows. Savings depend on the mix; the stable result is the shape (no loss when under budget, containment when over, errors always kept).

## 2. Tier stability

Decisions escalate on accepted spend (what costs money) and de-escalate only when both accepted and offered spend are low. Deciding on accepted spend alone makes a still noisy key bounce: tier 1 drops its DEBUG, accepted burn falls, the key de-escalates and escalates again.

| | Tier changes |
|---|---|
| Engine, 2 h of a key at 1.5x with 90% DEBUG, accepted spend only (previous behaviour) | 19 |
| Same, with offered spend | 1 |
| Extension, noisy key for 30 min (`TestSteadyNoisyKeyHoldsTier`) | 1 |
| Scenario harness, producer noisy 2 min / quiet 2 min for 1 h, with hysteresis | 1 |
| Same, without hysteresis | 2 |

`TestDroppingDataDoesNotFlap` (engine) and `TestSteadyNoisyKeyHoldsTier` (extension) keep this from regressing.

## 3. Overhead

### Per record (`make bench`)

Processor benchmarks use a fake ledger so they measure only the processor; the extension calls are measured separately. Tier 1 drops below DEBUG, samples logs and spans at 50%, and has a metric glob that matches nothing here. `series=on` prices active series. Apple M4, `-count=10`, medians:

| Signal | Batch shape | Tier 0 | Tier 1 | allocs per batch |
|---|---|---|---|---|
| logs | 1 resource x 10k | 5.1 ns/record | 30.5 ns/record | 12 to 13 |
| logs | 100 resources x 100 | 8.5 ns/record | 30.7 ns/record | 429 to 437 |
| traces | 1 resource x 10k | 5.4 ns/span | 14.1 ns/span | 12 to 13 |
| traces | 100 resources x 100 | 8.5 ns/span | 19.2 ns/span | 429 to 437 |
| metrics, series off | 1 resource x 10k | 6.5 ns/point | 6.7 ns/point | 12 |
| metrics, series off | 100 resources x 100 | 11.1 ns/point | 11.5 ns/point | 429 |
| metrics, series on | 1 resource x 10k | 25.2 ns/point | 24.1 ns/point | 13 |
| metrics, series on | 100 resources x 100 | 30.6 ns/point | 32.9 ns/point | 529 |

Tier 1 logs cost more per record because sampling hashes records without a trace ID. Per resource allocations come from stamping `budget.*` attributes.

A performance pass, each change measured with benchstat at `-count=10` (all p < 0.02), geomean over the 16 cases: -62% time per record, -69% bytes, -37% allocations.

- **Size each resource once.** Offered and kept bytes were two full proto sizings even when nothing was removed. The resource is now stamped first and sized once; it is sized again only when records were removed. Tier 0 logs and traces: -47% to -58%. Offered bytes now include the stamps, like kept bytes, so the two are comparable.
- **Hash series only when they are priced.** Metrics hashed every data point for active series even without a `series_hour` price. The extension now tells the processor (`budgetapi.SeriesPricer`); unpriced metrics: -84% to -90% time, up to -99.8% bytes.
- **Cheaper series hashes when priced.** One shot xxhash per key and value instead of a streaming digest per entry, ints hashed without allocating, and the metric name hashed once per metric: series hashing -46% (44.7 to 24.0 ns per point), priced metrics -53% to -66% overall.
- **Tried, not kept:** reserving attribute capacity before stamping cut allocations by 23% but cost 6% to 11% more time per record.

| Extension call (once per key per batch) | ns/op | allocs/op |
|---|---|---|
| `ResolveKey`, cache hit | 36 | 0 |
| `ResolveKey`, cache miss (new key) | 352 | 9 |
| `Decision` (parallel) | 2.3 | 0 |
| `Record` (10 goroutines on one key) | 301 | 0 |
| `RecordOffered` (10 goroutines on one key) | 311 | 0 |

### CPU at a fixed rate

20,000 logs per second from telemetrygen (4 workers at 5,000 each) for 60 s after a 15 s warm up, OTLP gRPC in, file exporter out, one local process; CPU time read from `ps`. Measured with a local script that is not part of this repository.

| Pipeline | CPU seconds | % of one core |
|---|---|---|
| without the budget processor | 2.55 | 4.2 |
| with it at tier 0 (accounting only) | 2.61 | 4.3 |
| with it enforcing (drop below WARN, 50% sampling) | 1.65 | 2.8 |

At tier 0 the difference is within run to run noise. Enforcing lowers CPU because less data reaches the exporter.

## 4. Pipeline latency

`make results`. In-process Collector, 2,000 synchronous OTLP/HTTP posts of 100 log records each (the receiver answers once the pipeline has taken the batch), after 200 warm up posts.

| Pipeline | p50 | p99 | max |
|---|---|---|---|
| `memory_limiter` only | 81 µs | 302 µs | 513 µs |
| with the budget processor, tier 0 | 78 µs | 285 µs | 447 µs |
| with the budget processor, enforcing | 76 µs | 276 µs | 414 µs |

The differences are run to run noise.

## 5. Scale

### Keys (`make results`)

10,000 keys, each with recorded and offered usage, in one extension.

| Windows | Heap | Decision tick |
|---|---|---|
| 1 m / 6 m, 5 s interval | 42 MB | 16 to 28 ms |
| 1 h / 6 h, 10 s interval (defaults) | 42 MB | 16 to 25 ms |

A tick every 10 s at 20 ms is 0.2% of one core. Before ring buffers had a fixed size, the default windows needed 1.4 GB for the same 10,000 keys.

### Redis load (`make results-redis`)

A real Valkey 8.1 container (Redis protocol, BSD licensed; Redis 7 gave the same numbers) and the real `redis_storage` extension (from the PR, so both modes are available). N replicas each add spend to K keys, then flush and read, 5 cycles. Per replica per cycle:

| Mode | Replicas | Keys | Redis commands | Bytes in | Bytes out | Flush + read time |
|---|---|---|---|---|---|---|
| G-Counter | 3 | 1,000 | 64 | 4.2 KB | 1.5 KB | 12 ms |
| G-Counter | 32 | 1,000 | 64 | 4.2 KB | 19 KB | 23 ms |
| G-Counter | 32 | 10,000 | 64 | 7.8 KB | 130 KB | 138 ms |
| increment | 3 | 1,000 | 2,065 | 148 KB | 11 KB | 14 ms |
| increment | 32 | 1,000 | 2,065 | 148 KB | 30 KB | 15 ms |
| increment | 32 | 10,000 | 20,065 | 1.4 MB | 255 KB | 52 ms |

Each cycle includes tier sharing (one write of the replica's tiers, one read per replica slot: 33 commands).

Every run ended with all replicas agreeing on every key.

- **G-Counter** sends little to Redis (one blob write and one blob read per replica slot), but every replica decodes every other replica's blob: its CPU and time grow with replicas × keys.
- **Increment** costs one write and one read per changed counter (two Redis commands per key, pipelined), independent of the number of replicas: at 32 replicas and 10,000 keys it syncs 2.7x faster than G-Counter.
- Pick G-Counter to minimise Redis load, increment for large fleets where sync time per replica matters. `auto` picks increment when the storage client supports it.

This measurement found and fixed two problems in increment mode: reads went through `Batch`, which `redis_storage` executes as one GET per key (13 commands per key, over 1 s per cycle at 1,000 keys), and the key index was uncompressed (14.5 MB per read at 32 × 10,000). Reads now use the pipelined `BatchIncrementBy` with a zero delta, and the index is compressed.

## 6. Fleet convergence

Three local replicas share one Valkey 8.1 container (`flush_interval: 5s`, `read_interval: 10s`, `decision_interval: 1s`). 2,000 logs per second go to replica 0 only for 10 s. The time is measured from the end of the load until the `otelcol_budget_fleet_spend` gauge of all three replicas agrees within 1%. Measured with a local script that is not part of this repository.

| `storage.mode` | Selected | Runs (s) |
|---|---|---|
| `auto` | increment | 10.79, 11.09, 11.07 |
| `increment` | increment | 10.60, 11.03, 10.86 |
| `gcounter` | G-Counter | 10.73, 0.73, 11.03 |

A run under one second means the load ended just before a read.

Every run ended with identical totals. Both modes converge within about one `read_interval` after the last flush. The measurement found a bug, now fixed: in increment mode a replica marked a key fresh when it read it, and with ticker jitter idle replicas could lag by two read intervals (21 s); only a replica's own flush responses mark keys fresh now (`TestIncrementReaderSeesEveryRead`).

## 7. Kubernetes run

A local kind cluster (setup not part of this repository): Valkey 8.1, three gateway replicas, telemetrygen jobs, budget 0.01 per hour per service, `period: 1h`, windows 1 m / 6 m. Traffic: checkout and search steady and under budget; payments sends only ERROR logs at about 2x its budget; recommender turns noisy (400 DEBUG/s plus 100 INFO/s). An automated gate runner reads every replica's self telemetry every 20 s and checks:

| Gate | Passes when | storage (increment) | share |
|---|---|---|---|
| G1 deploy | 3 replicas ready, expected sync mode | PASS | PASS |
| G2 baseline | calm keys tier 0 everywhere, payments escalated | PASS | PASS |
| G3 escalation | recommender tier 1 or more on all replicas within 3 min of the noise | PASS (40 s) | PASS (30 s) |
| G4 containment | accepted below offered burn while escalated, severity drops grow | PASS | PASS |
| G5 errors never dropped | payments drops are 0 on every replica for the whole run | PASS | PASS |
| G6 agreement | every replica reports the same tier for every key in every sample | PASS (24 samples) | n/a |
| G7 stability | recommender changes tier at most once in 8 min of steady noise | PASS (0 changes) | PASS (0 changes) |
| G8 fleet spend | median spread of the fleet spend gauge across replicas 5% or less | PASS (1.7%) | n/a |
| G9 exempt | exempting recommender gives tier 0 everywhere with no drops; clearing re-escalates | PASS (41 s / 62 s) | PASS (41 s / 62 s) |
| G10 fail open | Redis scaled to 0: every key at tier 0 within `fail_open_after`, sync errors grow, no restarts; Redis back (empty): re-escalation | PASS (322 s / 10 s) | n/a |

Share mode runs each generator as one job per replica (even load); with a single gRPC connection per generator all of a key's traffic lands on one replica, which then enforces budget / N against the whole key and escalates it too early. That is the documented limitation of share mode.

The gates found four problems, all fixed before the passing runs:

- **Replicas disagreed near a threshold.** Payments at burn 2.1 against a 2.0 threshold sat at tier 2 on one replica and tier 1 on two, held apart by hysteresis. Replicas now share their tiers through storage and enforce the highest tier any live replica decided (`TestFleetTiers`).
- **Redis losing its data lost the period's spend** in increment mode, and fleet totals going backwards produced negative burn rates for up to a long window. Increment mode now restores each node's contribution after a data loss, and the engine restarts the windows when totals go back (`TestStorageDataLossHeals`, `TestTotalsGoingBackRestartWindows`). In G10, recommender was back at tier 2 ten seconds after an empty Redis returned.
- **The demo's exempt script wrote an invalid config** (`exempt: [""]`) when clearing the list; the extension rejected it at startup, as it should.
- **Generators died on gateway restarts.** They now retry, like SDK exporters do.

A second scenario follows one 5 minute period (`period: 5m`, windows 15 s / 90 s) through six services sending logs, traces, and metrics from 19 generators: an organic growth that reaches the hard cap, a noisy deploy, an incident, a static exemption, and Redis down for 75 s then back empty. Each phase is checked through the Grafana dashboard's own queries (Prometheus self telemetry, Loki, and Tempo span metrics), not the collector's endpoints. Two consecutive runs passed all 23 checks:

| Check | Observed |
|---|---|
| noisy deploy (logs, metrics) | recommender tier 1 after 25 s; DEBUG logs 0/s and the `debug.*` histogram gone, INFO 30/s, WARN 2/s, and the requests metric unchanged |
| incident (logs) | every payments ERROR in Loki (252/s offered, 251.9/s received) while payments sat at tier 1 |
| hard cap (logs, traces) | search reached its budget at +2:00 and held tier 2: spend rate cut from 0.93% to 0.14% of budget per second, 105% of budget 15 s after the cap, INFO logs 0/s, spans 120/s to 31/s (26%), error spans 2.00/s to 2.02/s |
| exempt | checkout at about 1.7x burn, tier 0 throughout |
| fail open | every service at tier 0 after Redis was gone 45 s; tiers back 4 s after it returned |
| catch-up | no offered burn rise once windows were warm again; the only tier rise was recommender reaching its hard cap, because its flood went through while every service was failed open |
| agreement | replicas on the same tier in 99.5% of 222 samples |

It found three more problems:

- **The catch-up after an outage read as spend.** Replicas reconnect and restore their lost counters (or flush what they held) seconds apart, so fleet totals climbed back in steps after the windows restarted: offered burn showed 46 for a key at 1.3, and payments went from tier 1 to tier 2. Windows now restart at every sample for `stale_after` plus one flush and one read after a gap or a reset, and tiers hold while windows are cold (`TestScenarios`: staggered restore, staggered catch-up, tier holds while windows are cold).
- **Increment mode lost the spend of an outage.** A failed flush dropped its batch, because a batch can partially apply. While storage is down every flush fails, so each replica silently discarded what it accepted during the outage, and its totals went back, restarting its windows. A batch that failed before it was sent (dialing failed) now goes back to pending; other errors still drop it. Totals also include the batch in flight, so a decision tick during a flush no longer sees own spend vanish (`TestStorageOutageKeepsSpend`, `TestStorageTotalsIncludeInFlight`). In the demo, every failed flush during the outage was a refused dial, and was kept.
- **A period rollover could lose a replica's whole period.** A flush that raced the rollover stored a fresh table for the old period over the new one, so every later add for the new period was dropped on that replica until the next rollover. In a live run one gateway's injected traffic never reached the fleet total, so its service never escalated. The same race made the first flush of a new period look like a storage data loss. The rollover now swaps tables under the flush lock, and alive markers are compared per period (`TestStorageRolloverDuringFlush`, which failed in 2 of 3 stress runs before the fix, `TestStorageRolloverIsNotDataLoss`). Both reproduce on an in-memory Redis: the bug was in this code, not in Valkey.

## 8. Accounting

`make results`. An in-process Collector exports to a sink that decodes every request and sums the proto size of what it received; prices are set so one micro unit is one byte.

| Accounted bytes | Exported bytes | Error |
|---|---|---|
| 16,114,474 | 16,114,474 | 0% |

This shows accounting is exact and consistent: nothing is counted twice or lost. It is not a comparison with a bill: the processor measures uncompressed proto size, and backends bill compressed bytes, indexing, and retention. Use `compression_ratio` and the prices to approximate a specific backend.
