package examples

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
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
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

type ledgerRow struct {
	Tier      int
	SpendHot  float64
	SpendCold float64
}

// telemetryReader returns the config override that exposes self telemetry
// on a Prometheus endpoint, and that endpoint.
func telemetryReader(t *testing.T) (string, string) {
	port := freePort(t)
	return fmt.Sprintf("service::telemetry::metrics::readers: [{pull: {exporter: {prometheus: {host: 127.0.0.1, port: %d, without_units: true, without_scope_info: true}}}}]", port),
		fmt.Sprintf("127.0.0.1:%d", port)
}

// state reads a key's tier and spend (in currency units) from the
// extension's self telemetry.
func state(t *testing.T, endpoint, key string) ledgerRow {
	resp, err := http.Get("http://" + endpoint + "/metrics") //nolint:noctx // test helper
	if err != nil {
		return ledgerRow{}
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var row ledgerRow
	want := `otelcol_budget_key="` + key + `"`
	for line := range strings.SplitSeq(string(body), "\n") {
		if strings.HasPrefix(line, "#") || !strings.Contains(line, want) {
			continue
		}
		fields := strings.Fields(line)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		spend := strings.HasPrefix(line, "otelcol_budget_spend{") || strings.HasPrefix(line, "otelcol_budget_spend_total{")
		switch {
		case strings.HasPrefix(line, "otelcol_budget_tier{"):
			row.Tier = int(v)
		case spend && strings.Contains(line, `otelcol_budget_price_class="hot"`):
			row.SpendHot += v / 1e6
		case spend && strings.Contains(line, `otelcol_budget_price_class="cold"`):
			row.SpendCold += v / 1e6
		}
	}
	return row
}

func postLogs(t *testing.T, endpoint string, ld plog.Logs) {
	var m plog.ProtoMarshaler
	body, err := m.MarshalLogs(ld)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+endpoint+"/v1/logs", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func bigLogs(service string, n int) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", service)
	lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
	for i := range n {
		lr := lrs.AppendEmpty()
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		lr.Body().SetStr(fmt.Sprintf("%d %s", i, strings.Repeat("x", 1024)))
	}
	return ld
}

// Phase 3 done criterion: with examples/05-divert-cold.yaml, a key over its
// budget is diverted by the routing connector to the cold pipeline, reaches
// the file exporter, and is accounted at cold prices.
func TestDivertColdInProcess(t *testing.T) {
	var lokiBatches atomic.Int64
	loki := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		lokiBatches.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer loki.Close()

	dir := t.TempDir()
	coldFile := filepath.Join(dir, "cold.jsonl")
	httpEP := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	reader, apiEP := telemetryReader(t)
	overrides := []string{
		fmt.Sprintf("receivers::otlp::protocols::grpc::endpoint: 127.0.0.1:%d", freePort(t)),
		"receivers::otlp::protocols::http::endpoint: " + httpEP,
		"exporters::otlp_http/loki::endpoint: " + loki.URL,
		"exporters::file/cold::path: " + coldFile,
		"exporters::file/cold::flush_interval: 10ms",
		"exporters::otlp_http/loki::sending_queue::batch::flush_timeout: 10ms",
		"extensions::budget::hard_cap: divert",
		"extensions::budget::decision_interval: 50ms",
		"extensions::budget::budgets: [{name: default, match: default, budget: 0.000001}]",
		reader,
	}
	uris := []string{"file:05-divert-cold.yaml"}
	for _, o := range overrides {
		uris = append(uris, "yaml:"+o)
	}
	col, err := otelcol.NewCollector(otelcol.CollectorSettings{
		BuildInfo: component.NewDefaultBuildInfo(),
		Factories: func() (otelcol.Factories, error) { return factories(t), nil },
		ConfigProviderSettings: otelcol.ConfigProviderSettings{ResolverSettings: confmap.ResolverSettings{
			URIs:              uris,
			ProviderFactories: []confmap.ProviderFactory{fileprovider.NewFactory(), yamlprovider.NewFactory()},
		}},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- col.Run(ctx) }()
	defer func() {
		cancel()
		require.NoError(t, <-done)
	}()
	require.Eventually(t, func() bool { return col.GetState() == otelcol.StateRunning }, 20*time.Second, 10*time.Millisecond)

	// batch 1 goes hot and pushes spend over the tiny budget
	postLogs(t, httpEP, bigLogs("noisy", 1000))
	require.Eventually(t, func() bool { return lokiBatches.Load() > 0 }, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return state(t, apiEP, "noisy").Tier == 2 }, 10*time.Second, 10*time.Millisecond, "hard cap divert")
	hotBefore := state(t, apiEP, "noisy").SpendHot
	assert.Positive(t, hotBefore)

	// batch 2 is diverted to the cold pipeline
	postLogs(t, httpEP, bigLogs("noisy", 1000))
	require.Eventually(t, func() bool {
		b, err := os.ReadFile(coldFile)
		return err == nil && strings.Count(string(b), `"otelcol.budget.divert"`) > 0 && strings.Count(string(b), "xxxx") >= 1000
	}, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return state(t, apiEP, "noisy").SpendCold > 0 }, 10*time.Second, 10*time.Millisecond)
	row := state(t, apiEP, "noisy")
	assert.InDelta(t, hotBefore, row.SpendHot, 1e-9, "diverted data is not accounted at hot prices")
	assert.InDelta(t, row.SpendHot/20, row.SpendCold, row.SpendHot/200, "same volume at 0.02 instead of 0.40 per GB")
	assert.Equal(t, int64(1), lokiBatches.Load(), "nothing else reached the hot exporter")
}
