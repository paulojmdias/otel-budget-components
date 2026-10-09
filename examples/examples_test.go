package examples

import (
	"path/filepath"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/redisstorageextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/k8sattributesprocessor"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/debugexporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/otelcol/otelcoltest"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/memorylimiterprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension"
	"github.com/paulojmdias/otel-budget-processor/processor/budgetprocessor"
)

// factories mirrors the OCB manifest in cmd/otelcol-budget.
func factories(t *testing.T) otelcol.Factories {
	f, err := otelcoltest.NopFactories()
	require.NoError(t, err)
	f.Telemetry = otelconftelemetry.NewFactory()
	f.Receivers, err = otelcol.MakeFactoryMap[receiver.Factory](otlpreceiver.NewFactory())
	require.NoError(t, err)
	f.Processors, err = otelcol.MakeFactoryMap[processor.Factory](
		memorylimiterprocessor.NewFactory(), k8sattributesprocessor.NewFactory(), budgetprocessor.NewFactory(),
	)
	require.NoError(t, err)
	f.Exporters, err = otelcol.MakeFactoryMap[exporter.Factory](
		debugexporter.NewFactory(), otlphttpexporter.NewFactory(), fileexporter.NewFactory(),
	)
	require.NoError(t, err)
	f.Extensions, err = otelcol.MakeFactoryMap[extension.Factory](
		budgetextension.NewFactory(), filestorage.NewFactory(), redisstorageextension.NewFactory(),
	)
	require.NoError(t, err)
	f.Connectors, err = otelcol.MakeFactoryMap[connector.Factory](routingconnector.NewFactory())
	require.NoError(t, err)
	return f
}

func TestExamplesValidate(t *testing.T) {
	t.Setenv("POD_NAME", "otelcol-gw-0")
	files, err := filepath.Glob("*.yaml")
	require.NoError(t, err)
	require.Len(t, files, 8)
	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			cfg, err := otelcoltest.LoadConfigAndValidate(file, factories(t))
			require.NoError(t, err)
			require.NotNil(t, cfg)
		})
	}
}
