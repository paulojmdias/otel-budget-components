# otelcol-budget

Telemetry spend budgets enforced inside the OpenTelemetry Collector.

Observability bills grow with whatever services decide to emit, and one noisy service can spend a month of budget in a day. `otelcol-budget` gives each service, tenant, or team a budget in money, measures what it actually sends through the Collector, and, when a key burns its budget too fast, degrades its telemetry step by step instead of cutting it off: drop debug logs first, then sample traces, drop noisy metric families, and finally divert to cheap cold storage. Errors are never dropped (at the divert tier they go to cold storage with the rest of the key's data).

## How it works

Two components work together:

| Component | Role |
|---|---|
| [`budget` extension](extension/budgetextension/README.md) | Owns all state: resolves keys from resource attributes, prices usage, keeps per key ledgers, shares spend across Collector replicas through a storage extension, and computes burn rates and tiers. |
| [`budget` processor](processor/budgetprocessor/README.md) | Runs in each pipeline: reads the decision for each key, applies the tier's actions, stamps `otelcol.budget.key` / `otelcol.budget.tier` / `otelcol.budget.divert`, and reports accepted usage. |

Decisions use SLO style burn rates. With a monthly budget of 100 EUR, a burn rate of 1.0 means spending exactly 100 EUR per month. A typical setup:

| Tier | Reached when burn rate on both windows is at least | Actions |
|---|---|---|
| 0 | | everything passes |
| 1 | 1.0 | drop logs below INFO |
| 2 | 1.5 | drop logs below WARN, keep 25% of logs and traces |
| 3 | 3.0 | divert to cold storage |

Escalation needs both a short and a long window to agree, de-escalation has hysteresis, and a hard cap holds a minimum tier once the period's budget is spent. If the extension is missing or its fleet data is too old, everything passes (fail open).

## Results at a glance

From [docs/results.md](docs/results.md):

- **Adapts where static rules cannot.** Over one hour of normal traffic it keeps 100% of logs (a 25% sampler keeps 25%, errors included). A DEBUG flood goes from 2.17x the budget to 0.55x with INFO, WARN, and ERROR untouched. Organic growth goes from 1.34x to 1.01x. ERROR is never dropped.
- **Stable.** A key that stays noisy changes tier once, not every few minutes.
- **Cheap.** About 50 ns per record in large batches; no measurable CPU or latency difference at tier 0 at 20,000 logs per second.
- **Scales.** 10,000 keys take 42 MB and a 20 ms decision tick every 10 s; 32 replicas share spend through Redis and converge on the same fleet totals.
- **Verified in Kubernetes.** Ten automated gates (escalation, containment, errors never dropped, replica agreement, stability, exemptions, fail open when Redis disappears, recovery after it comes back empty) pass in both storage and share mode.

## Requirements

- **To build:** Go 1.26 and make. `make build` produces a Collector with both components plus otlp, otlp_http, debug, file, memory_limiter, k8sattributes, routing, file_storage, and redis_storage.
- **To run:** the `budget` extension must be listed in `service.extensions`. The processor finds it by component ID (`ledger: budget`); without it the processor passes everything through untouched.
- **Only for several Collector replicas:** Redis or Valkey and the `redis_storage` extension, see [Fleet sync with Redis](#fleet-sync-with-redis).
- **Only for diverting to cold storage:** the `routing` connector, a cold exporter, and a second `budget` processor with `price_class: cold`, see the [processor README](processor/budgetprocessor/README.md#divert-to-cold-storage).

## Quick start

```
make build
```

Save this as `config.yaml`:

```yaml
receivers:
  otlp:
    protocols: { grpc: { endpoint: 0.0.0.0:4317 } }
processors:
  budget: { ledger: budget }
exporters:
  debug: {}
extensions:
  budget:
    prices: { hot: { logs_gb: 0.40, traces_gb: 0.30 } }
    budgets: [{ name: default, match: default, budget: 100 }]
    tiers: [{ threshold: 1.0, actions: { drop_below_severity: INFO } }]
service:
  extensions: [budget]
  pipelines:
    logs:   { receivers: [otlp], processors: [budget], exporters: [debug] }
    traces: { receivers: [otlp], processors: [budget], exporters: [debug] }
```

```
bin/otelcol-budget/otelcol-budget --config=config.yaml
```

What the numbers mean:

- `budget: 100` is 100 EUR per calendar month (`period: monthly`), for each service. Each `service.name` gets its own budget from the `default` rule.
- `logs_gb: 0.40` prices accepted logs at 0.40 per GB. Use your backend's price, so the budget means real money.
- `threshold: 1.0` is a burn rate: the service is spending fast enough to use exactly its monthly budget. At or above it, logs below INFO are dropped.
- The burn rate is measured over a 1 hour and a 6 hour window, so with these defaults nothing changes for roughly the first hour. To see it react in minutes, run [examples/08-demo.yaml](examples/08-demo.yaml) (one hour period, 1 minute and 6 minute windows). It prints each resource with its `otelcol.budget.tier` stamp, and `curl -s localhost:8888/metrics | grep otelcol_budget_` shows the burn rate and tier per service.

The extension's internal telemetry (spend, burn rate, and tier per service) shows what it decided; see the [extension README](extension/budgetextension/README.md#telemetry).

## Choosing a sync mode

| You run | Use | Notes |
|---|---|---|
| One Collector | `sync: { mode: local }` (default) | Nothing else needed. |
| Several replicas behind a load balancer | `sync: { mode: storage }` with Redis | Every replica sees the fleet's spend. See below. |
| Several replicas, even load, no Redis | `sync: { mode: share }` | Each replica enforces budget / N. An approximation. |

More configurations, each validated in tests:

| Example | Shows |
|---|---|
| [01-minimal.yaml](examples/01-minimal.yaml) | One default budget for logs and traces |
| [02-tenant-team.yaml](examples/02-tenant-team.yaml) | Tenant and service keys, a team budget from a Kubernetes label, exemptions |
| [03-fleet-redis.yaml](examples/03-fleet-redis.yaml) | Spend shared across replicas through Redis |
| [04-share.yaml](examples/04-share.yaml) | Each of N replicas enforces budget / N |
| [05-divert-cold.yaml](examples/05-divert-cold.yaml) | Divert to a cold pipeline with the routing connector |
| [06-metrics.yaml](examples/06-metrics.yaml) | Metrics priced per data point and active series |
| [07-production.yaml](examples/07-production.yaml) | Three tiers, hard cap, hysteresis |
| [08-demo.yaml](examples/08-demo.yaml) | One hour period with minute windows, for trying it out |

## Fleet sync with Redis

With several replicas, each one must know what the others spent. In `storage` mode the extension shares spend through a storage extension, in practice `redis_storage`, which works with Redis or Valkey (the measurements and the Kubernetes runs use Valkey 8):

```yaml
extensions:
  redis_storage/budget:
    endpoint: redis:6379
    expiration: 960h          # longer than the budget period
    tls: { insecure: true }   # plain Redis; the default expects TLS
  budget:
    # ... prices, budgets, tiers ...
    sync:
      mode: storage
      storage:
        extension: redis_storage/budget
        node_id: ${env:POD_NAME}    # e.g. otelcol-gw-0 in a StatefulSet
        node_prefix: otelcol-gw     # replicas are otelcol-gw-0, -1, ...
service:
  extensions: [redis_storage/budget, budget]
```

`node_id` and `node_prefix` are required: replicas find each other as `<node_prefix>-0 .. <node_prefix>-<max_nodes-1>`, which matches StatefulSet pod names.

**Which `redis_storage` this build uses.** The distribution in this repository builds `redis_storage` from [opentelemetry-collector-contrib#51568](https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/51568), which adds atomic increments (pinned to commit `e9a5c2f` of the PR branch in [builder-config.yaml](cmd/otelcol-budget/builder-config.yaml)). With it, `mode: auto` uses **increment** mode: each replica adds its spend to shared counters, and the others see it after their next read.

With the upstream `redis_storage` (any released contrib version so far), `auto` falls back to **G-Counter** mode: each replica writes its own totals and reads everyone else's. It is equally correct. Both modes converge within about one `read_interval` (measured 5 to 11 s with a 10 s read interval, see [docs/results.md](docs/results.md)).

To build against a newer commit of the PR, or a local checkout:

```
make build REDIS_FORK=github.com/paulojmdias/opentelemetry-collector-contrib/extension/storage/redisstorageextension@<commit>
make build REDIS_FORK=/path/to/opentelemetry-collector-contrib/extension/storage/redisstorageextension
```

## Using the components in your own distribution

The components are Go modules:

- `github.com/paulojmdias/otel-budget-processor/extension/budgetextension`
- `github.com/paulojmdias/otel-budget-processor/processor/budgetprocessor`

They are not tagged yet, so they cannot be fetched by version. Until they are, build from this repository, or point your OCB manifest at a local clone:

```yaml
extensions:
  - gomod: github.com/paulojmdias/otel-budget-processor/extension/budgetextension v0.0.0
    path: /path/to/otel-budget-processor/extension/budgetextension
processors:
  - gomod: github.com/paulojmdias/otel-budget-processor/processor/budgetprocessor v0.0.0
    path: /path/to/otel-budget-processor/processor/budgetprocessor
```

Once tagged (`extension/budgetextension/vX.Y.Z` and `processor/budgetprocessor/vX.Y.Z`), drop the `path:` lines and use the version. Add the `replaces:` line from [builder-config.yaml](cmd/otelcol-budget/builder-config.yaml) if you want increment mode with Redis.

## Development

Each component is its own Go module with a Makefile that includes `Makefile.Common`, so `make test` and `make lint` also work inside a component directory. From the root they run in every module:

```
make generate            # mdatagen and config.schema.yaml for both components
make lint                # golangci-lint with contrib's configuration
make test                # go test -race in every module
make bench               # benchmarks
make results             # in-process measurements
make validate-examples   # validate every example with the built binary
```

Every file in `examples/` and every YAML snippet in the READMEs is loaded and validated by the tests (`examples/examples_test.go`, `examples/docs_test.go`), so documented configuration cannot drift from the code.

Tools are installed into `./bin` on first use. Collector v0.162.0, Go 1.26.

## Documentation

- [docs/design.md](docs/design.md): architecture, formulas, limitations
- [docs/decisions.md](docs/decisions.md): design decisions and trade offs
- [docs/results.md](docs/results.md): overhead, accuracy, and savings measurements
