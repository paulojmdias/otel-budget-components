package examples

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

// sink counts exported log records by service and severity.
type sink struct {
	mu     sync.Mutex
	counts map[string]int
}

func (s *sink) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	req := plogotlp.NewExportRequest()
	if req.UnmarshalProto(body) == nil {
		s.mu.Lock()
		rls := req.Logs().ResourceLogs()
		for i := range rls.Len() {
			svc, _ := rls.At(i).Resource().Attributes().Get("service.name")
			sls := rls.At(i).ScopeLogs()
			for j := range sls.Len() {
				lrs := sls.At(j).LogRecords()
				for k := range lrs.Len() {
					s.counts[svc.Str()+"/"+lrs.At(k).SeverityNumber().String()]++
				}
			}
		}
		s.mu.Unlock()
	}
	w.WriteHeader(http.StatusOK)
}

func (s *sink) get(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[key]
}

func (s *sink) snapshot() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.counts))
	maps.Copy(out, s.counts)
	return out
}

func logsBatch(service string, debug, errors int) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", service)
	lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
	for i := range debug + errors {
		lr := lrs.AppendEmpty()
		lr.SetSeverityNumber(plog.SeverityNumberDebug)
		if i >= debug {
			lr.SetSeverityNumber(plog.SeverityNumberError)
		}
		lr.Body().SetStr(fmt.Sprintf("record %d %s", i, strings.Repeat("z", 200)))
	}
	return ld
}

// End to end with the real extension and processor: a key burning far above
// its budget escalates and loses its DEBUG logs, its ERROR logs still pass,
// and an exempt key passes everything.
func TestPipelineEnforcement(t *testing.T) {
	s := &sink{counts: map[string]int{}}
	srv := httptest.NewServer(s)
	defer srv.Close()

	httpEP := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	reader, telEP := telemetryReader(t)
	uris := []string{"file:01-minimal.yaml"}
	for _, o := range []string{
		fmt.Sprintf("receivers::otlp::protocols::grpc::endpoint: 127.0.0.1:%d", freePort(t)),
		"receivers::otlp::protocols::http::endpoint: " + httpEP,
		"exporters::otlp_http::endpoint: " + srv.URL,
		"exporters::otlp_http::compression: none",
		"exporters::otlp_http::sending_queue::batch::flush_timeout: 10ms",
		"service::pipelines::logs::exporters: [otlp_http]",
		"service::pipelines::traces::exporters: [debug]",
		"extensions::budget::period: 10m",
		"extensions::budget::windows: {short: 5s, long: 15s}",
		"extensions::budget::decision_interval: 100ms",
		"extensions::budget::hard_cap: off",
		"extensions::budget::prices::hot::logs_gb: 1000",
		"extensions::budget::budgets: [{name: default, match: default, budget: 0.01}]",
		"extensions::budget::exempt: [vip]",
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
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- col.Run(ctx) }()
	defer func() {
		cancel()
		require.NoError(t, <-done)
	}()
	require.Eventually(t, func() bool { return col.GetState() == otelcol.StateRunning }, 20*time.Second, 10*time.Millisecond)

	// keep sending until the noisy key reaches tier 1 (after the warm up)
	require.Eventually(t, func() bool {
		postLogs(t, httpEP, logsBatch("noisy", 50, 0))
		return state(t, telEP, "noisy").Tier >= 1
	}, 30*time.Second, 100*time.Millisecond)

	debugBefore := s.get("noisy/Debug")
	errorBefore := s.get("noisy/Error")
	vipBefore := s.get("vip/Debug")
	ld := logsBatch("noisy", 100, 10)
	logsBatch("vip", 100, 0).ResourceLogs().MoveAndAppendTo(ld.ResourceLogs())
	postLogs(t, httpEP, ld)

	require.Eventually(t, func() bool {
		return s.get("noisy/Error") == errorBefore+10 && s.get("vip/Debug") == vipBefore+100
	}, 10*time.Second, 10*time.Millisecond, "sink counts: %v", s.snapshot())
	assert.Equal(t, debugBefore, s.get("noisy/Debug"), "DEBUG of the escalated key is dropped")
	assert.Equal(t, 0, state(t, telEP, "vip").Tier, "exempt key is never enforced")
}
