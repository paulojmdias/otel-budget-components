//go:build results

package examples

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
)

// Accounting accuracy: bytes accounted by the budget processor versus the
// proto size of what the hot exporter actually delivered. Prices are set to
// 1000 per GB so one micro unit is exactly one byte.
func TestResultsAccountingAccuracy(t *testing.T) {
	var sent atomic.Int64
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		req := plogotlp.NewExportRequest()
		if err := req.UnmarshalProto(body); err == nil {
			var m plog.ProtoMarshaler
			rls := req.Logs().ResourceLogs()
			for i := range rls.Len() {
				sent.Add(int64(m.ResourceLogsSize(rls.At(i))))
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer sink.Close()

	httpEP := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	reader, apiEP := telemetryReader(t)
	uris := []string{"file:01-minimal.yaml"}
	for _, o := range []string{
		fmt.Sprintf("receivers::otlp::protocols::grpc::endpoint: 127.0.0.1:%d", freePort(t)),
		"receivers::otlp::protocols::http::endpoint: " + httpEP,
		"exporters::otlp_http::endpoint: " + sink.URL,
		"exporters::otlp_http::compression: none",
		"exporters::otlp_http::sending_queue::batch::flush_timeout: 10ms",
		"service::pipelines::logs::exporters: [otlp_http]",
		"extensions::budget::prices::hot::logs_gb: 1000",
		"extensions::budget::budgets: [{name: default, match: default, budget: 1000000}]",
		"extensions::budget::decision_interval: 50ms",
		reader,
	} {
		uris = append(uris, "yaml:"+o)
	}
	col, err := otelcol.NewCollector(otelcol.CollectorSettings{
		BuildInfo: component.NewDefaultBuildInfo(),
		Factories: func() (otelcol.Factories, error) { return factories(t), nil },
		ConfigProviderSettings: otelcol.ConfigProviderSettings{ResolverSettings: confmap.ResolverSettings{
			URIs: uris, ProviderFactories: []confmap.ProviderFactory{fileprovider.NewFactory(), yamlprovider.NewFactory()},
		}},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- col.Run(ctx) }()
	require.Eventually(t, func() bool {
		select {
		case err := <-done:
			require.NoError(t, err, "collector exited")
		default:
		}
		return col.GetState() == otelcol.StateRunning
	}, 20*time.Second, 10*time.Millisecond)

	rng := rand.New(rand.NewPCG(1, 2))
	services := []string{"checkout", "search", "payments", "recommender"}
	for range 200 {
		ld := plog.NewLogs()
		for range 1 + rng.IntN(4) {
			rl := ld.ResourceLogs().AppendEmpty()
			rl.Resource().Attributes().PutStr("service.name", services[rng.IntN(len(services))])
			lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
			for range 1 + rng.IntN(200) {
				lr := lrs.AppendEmpty()
				lr.SetSeverityNumber(plog.SeverityNumberInfo)
				lr.Body().SetStr(strings.Repeat("y", 20+rng.IntN(500)))
				lr.Attributes().PutInt("n", rng.Int64())
			}
		}
		postLogs(t, httpEP, ld)
	}
	accountedNow := func() int64 {
		var total float64
		for _, s := range services {
			total += state(t, apiEP, s).SpendHot
		}
		return int64(total*1e6 + 0.5)
	}
	// wait until the last export landed and the last decision tick published
	var accounted int64
	assert.Eventually(t, func() bool {
		accounted = accountedNow()
		return accounted > 0 && accounted == sent.Load()
	}, 20*time.Second, 50*time.Millisecond)
	accounted = accountedNow()
	cancel()
	require.NoError(t, <-done)
	res := map[string]any{
		"accounted_bytes": accounted,
		"exported_bytes":  sent.Load(),
		"error_pct":       100 * float64(accounted-sent.Load()) / float64(sent.Load()),
	}
	b, _ := json.MarshalIndent(map[string]any{"accounting_accuracy": res}, "", "  ")
	t.Log(string(b))
	if p := os.Getenv("RESULTS_OUT"); p != "" {
		require.NoError(t, os.WriteFile(p, b, 0o600))
	}
}

// Pipeline latency: round trip of synchronous OTLP/HTTP posts (the receiver
// answers once the pipeline has taken the batch) without the budget processor,
// with it at tier 0, and with it enforcing.
func TestResultsLatency(t *testing.T) {
	sinkSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer sinkSrv.Close()
	run := func(name string, extra ...string) map[string]float64 {
		httpEP := fmt.Sprintf("127.0.0.1:%d", freePort(t))
		uris := []string{"file:01-minimal.yaml"}
		for _, o := range append([]string{
			fmt.Sprintf("receivers::otlp::protocols::grpc::endpoint: 127.0.0.1:%d", freePort(t)),
			"receivers::otlp::protocols::http::endpoint: " + httpEP,
			"exporters::otlp_http::endpoint: " + sinkSrv.URL,
			"service::pipelines::logs::exporters: [otlp_http]",
			"service::pipelines::traces::exporters: [debug]",
			"extensions::budget::decision_interval: 100ms",
			"extensions::budget::period: 10m",
			"extensions::budget::windows: {short: 5s, long: 15s}",
		}, extra...) {
			uris = append(uris, "yaml:"+o)
		}
		col, err := otelcol.NewCollector(otelcol.CollectorSettings{
			BuildInfo: component.NewDefaultBuildInfo(),
			Factories: func() (otelcol.Factories, error) { return factories(t), nil },
			ConfigProviderSettings: otelcol.ConfigProviderSettings{ResolverSettings: confmap.ResolverSettings{
				URIs: uris, ProviderFactories: []confmap.ProviderFactory{fileprovider.NewFactory(), yamlprovider.NewFactory()},
			}},
		})
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- col.Run(ctx) }()
		defer func() {
			cancel()
			require.NoError(t, <-done)
		}()
		require.Eventually(t, func() bool { return col.GetState() == otelcol.StateRunning }, 20*time.Second, 10*time.Millisecond)

		var m plog.ProtoMarshaler
		body, err := m.MarshalLogs(logsBatch("svc", 50, 50))
		require.NoError(t, err)
		post := func() time.Duration {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://"+httpEP+"/v1/logs", bytes.NewReader(body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-protobuf")
			start := time.Now()
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return time.Since(start)
		}
		for range 200 { // warm up, and let the enforcing run escalate
			post()
		}
		lat := make([]float64, 0, 2000)
		for range 2000 {
			lat = append(lat, float64(post().Microseconds()))
		}
		slices.Sort(lat)
		return map[string]float64{"p50_us": lat[len(lat)/2], "p99_us": lat[len(lat)*99/100], "max_us": lat[len(lat)-1]}
	}
	res := map[string]any{
		"without_budget_processor": run("without", "service::pipelines::logs::processors: [memory_limiter]"),
		"budget_tier0":             run("tier0", "extensions::budget::budgets: [{name: default, match: default, budget: 1000000}]"),
		"budget_enforcing":         run("enforcing", "extensions::budget::budgets: [{name: default, match: default, budget: 0.000001}]"),
	}
	b, _ := json.MarshalIndent(map[string]any{"latency_100_record_posts": res}, "", "  ")
	t.Log(string(b))
}
